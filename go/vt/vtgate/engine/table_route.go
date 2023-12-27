package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*TableRoute)(nil)

type TableRoute struct {
	// TableName specifies the tables to send the query to.
	TableNames []string

	ShardRouteParam *RoutingParameters

	TableRouteParam *TableRoutingParameters

	// Query specifies the query to be executed.
	Query sqlparser.Statement

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

	OrderBy []OrderByParams

	// Route does not take inputs
	noInputs

	// Route does not need transaction handling
	noTxNeeded

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int
}

func (tableRoute *TableRoute) RouteType() string {
	return tableRoute.TableRouteParam.TableOpcode.String()
}

func (tableRoute *TableRoute) GetKeyspaceName() string {
	return tableRoute.ShardRouteParam.Keyspace.Name
}

func (tableRoute *TableRoute) GetTableName() string {
	sort.Strings(tableRoute.TableNames)
	var tableNames []string
	var previousTbl string
	for _, name := range tableRoute.TableNames {
		if name != previousTbl {
			tableNames = append(tableNames, name)
			previousTbl = name
		}
	}
	return strings.Join(tableNames, ", ")
}

func (tableRoute *TableRoute) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if tableRoute.TableRouteParam == nil {
		return nil, vterrors.VT13001(fmt.Sprintf("No table Route available : %s", tableRoute.GetTableName()))
	}

	var rs *srvtopo.ResolvedShard

	// Use an existing shard session
	sss := vcursor.Session().ShardSession()
	for _, ss := range sss {
		if ss.Target.Keyspace == tableRoute.ShardRouteParam.Keyspace.Name {
			rs = ss
			break
		}
	}

	// If not find, then pick any shard.
	if rs == nil {
		rss, _, err := vcursor.ResolveDestinations(ctx, tableRoute.ShardRouteParam.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
		if err != nil {
			return nil, err
		}
		if len(rss) != 1 {
			// This code is unreachable. It's just a sanity check.
			return nil, fmt.Errorf("no shards for keyspace: %s", tableRoute.ShardRouteParam.Keyspace.Name)
		}
		rs = rss[0]
	}

	qr, err := execShard(ctx, tableRoute, vcursor, tableRoute.FieldQuery, bindVars, rs, false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}

	var nameMap = make(map[string]string)
	for _, logicTableConfig := range tableRoute.TableRouteParam.LogicTable {
		actualTable := vindexes.GetFirstActualTable(logicTableConfig)
		nameMap[actualTable] = logicTableConfig.LogicTableName
	}
	for _, field := range qr.Fields {
		logicTableName := nameMap[field.Table]
		if logicTableName != "" {
			field.Table = logicTableName
		}
	}

	return qr.Truncate(tableRoute.TruncateColumnCount), nil
}

func (tableRoute *TableRoute) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, tableRoute.QueryTimeout)
	defer cancelFunc()
	qr, err := tableRoute.executeInternal(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return qr.Truncate(tableRoute.TruncateColumnCount), nil
}

func (tableRoute *TableRoute) executeInternal(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
) (*sqltypes.Result, error) {

	// 0.计算分片
	rss, bvs, err := tableRoute.ShardRouteParam.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	// No route.
	if len(rss) == 0 {
		if wantfields {
			return tableRoute.GetFields(ctx, vcursor, bindVars)
		}
		return &sqltypes.Result{}, nil
	}

	//1. 计算分表
	actualTableMap, err := tableRoute.TableRouteParam.findTableRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	//排序分表
	SortTableList(actualTableMap)

	return tableRoute.executeShards(ctx, vcursor, rss, bvs, actualTableMap)
}

func SortTableList(tables map[string][]vindexes.ActualTable) {
	for _, ActualTableList := range tables {
		sort.Slice(ActualTableList, func(i, j int) bool {
			return ActualTableList[i].Index < ActualTableList[j].Index
		})
	}
}

func (tableRoute *TableRoute) executeShards(
	ctx context.Context,
	vcursor VCursor,
	rss []*srvtopo.ResolvedShard,
	bvs []map[string]*querypb.BindVariable,
	actualTableNameMap map[string][]vindexes.ActualTable,
) (*sqltypes.Result, error) {
	querieses := make([][]*querypb.BoundQuery, len(rss))
	for j := range rss {
		// 2.SQL改写 改写表名（逻辑表->实际表）这里取的是获取分表的actualTableNameMap
		boundQueries, err := tableRoute.TableRouteParam.getTableQueries(tableRoute.Query, bvs[j], actualTableNameMap)
		if err != nil {
			return nil, err
		}
		querieses[j] = boundQueries
	}
	result, errs := vcursor.ExecuteBatchMultiShard(ctx, tableRoute, rss, querieses, false /* rollbackOnError */, false /* canAutocommit */)

	if errs != nil {
		errs = filterOutNilErrors(errs)
		return nil, vterrors.Aggregate(errs)
	}

	var nameMap = make(map[string]string)
	for logicTableName, actualTables := range actualTableNameMap {
		for _, actualTable := range actualTables {
			nameMap[actualTable.ActualTableName] = logicTableName
		}
	}

	for _, field := range result.Fields {
		logicTableName := nameMap[field.Table]
		if logicTableName != "" {
			field.Table = logicTableName
		}
	}

	if len(tableRoute.OrderBy) != 0 {
		result, errSort := tableRoute.sort(result)
		if errSort != nil {
			return result, errSort
		}
	}
	return result.Truncate(tableRoute.TruncateColumnCount), nil
}

func (tableRoute *TableRoute) sort(in *sqltypes.Result) (*sqltypes.Result, error) {
	var err error
	// Since Result is immutable, we make a copy.
	// The copy can be shallow because we won't be changing
	// the contents of any row.
	out := in.ShallowCopy()

	compares := extractSlices(tableRoute.OrderBy)

	sort.Slice(out.Rows, func(i, j int) bool {
		var cmp int
		if err != nil {
			return true
		}
		// If there are any errors below, the function sets
		// the external err and returns true. Once err is set,
		// all subsequent calls return true. This will make
		// Slice think that all elements are in the correct
		// order and return more quickly.
		for _, c := range compares {
			cmp, err = c.compare(out.Rows[i], out.Rows[j])
			if err != nil {
				return true
			}
			if cmp == 0 {
				continue
			}
			return cmp < 0
		}
		return true
	})

	return out.Truncate(tableRoute.TruncateColumnCount), err
}

func (tableRoute *TableRoute) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// SetTruncateColumnCount sets the truncate column count.
func (tableRoute *TableRoute) SetTruncateColumnCount(count int) {
	tableRoute.TruncateColumnCount = count
}

func (tableRoute *TableRoute) description() PrimitiveDescription {
	other := map[string]any{
		"Query":      sqlparser.String(tableRoute.Query),
		"Table":      tableRoute.GetTableName(),
		"FieldQuery": tableRoute.FieldQuery,
	}
	if tableRoute.ShardRouteParam.Vindex != nil {
		other["Vindex"] = tableRoute.ShardRouteParam.Vindex.String()
	}
	if tableRoute.ShardRouteParam.Values != nil {
		formattedValues := make([]string, 0, len(tableRoute.ShardRouteParam.Values))
		for _, value := range tableRoute.ShardRouteParam.Values {
			formattedValues = append(formattedValues, evalengine.FormatExpr(value))
		}
		other["Values"] = formattedValues
	}
	if tableRoute.TableRouteParam.TableValues != nil {
		formattedValues := make([]string, 0, len(tableRoute.TableRouteParam.TableValues))
		for _, value := range tableRoute.TableRouteParam.TableValues {
			formattedValues = append(formattedValues, evalengine.FormatExpr(value))
		}
		other["TableValues"] = formattedValues
	}

	if len(tableRoute.ShardRouteParam.SysTableTableSchema) != 0 {
		sysTabSchema := "["
		for idx, tableSchema := range tableRoute.ShardRouteParam.SysTableTableSchema {
			if idx != 0 {
				sysTabSchema += ", "
			}
			sysTabSchema += evalengine.FormatExpr(tableSchema)
		}
		sysTabSchema += "]"
		other["SysTableTableSchema"] = sysTabSchema
	}
	if len(tableRoute.ShardRouteParam.SysTableTableName) != 0 {
		var sysTableName []string
		for k, v := range tableRoute.ShardRouteParam.SysTableTableName {
			sysTableName = append(sysTableName, k+":"+evalengine.FormatExpr(v))
		}
		sort.Strings(sysTableName)
		other["SysTableTableName"] = "[" + strings.Join(sysTableName, ", ") + "]"
	}
	orderBy := GenericJoin(tableRoute.OrderBy, orderByToString)
	if orderBy != "" {
		other["OrderBy"] = orderBy
	}
	if tableRoute.TruncateColumnCount > 0 {
		other["ResultColumns"] = tableRoute.TruncateColumnCount
	}
	/*	if tableRoute.ScatterErrorsAsWarnings {
			other["ScatterErrorsAsWarnings"] = true
		}
		if tableRoute.QueryTimeout > 0 {
			other["QueryTimeout"] = tableRoute.QueryTimeout
		}*/
	return PrimitiveDescription{
		OperatorType:      "TableRoute",
		Variant:           tableRoute.ShardRouteParam.Opcode.String() + "-" + tableRoute.TableRouteParam.TableOpcode.String(),
		Keyspace:          tableRoute.ShardRouteParam.Keyspace,
		TargetDestination: tableRoute.ShardRouteParam.TargetDestination,
		Other:             other,
	}
}
