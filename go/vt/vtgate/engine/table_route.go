package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*TableRoute)(nil)

type TableRoute struct {
	// TableName specifies the tables to send the query to.
	TableName string

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
}

func (tableRoute *TableRoute) RouteType() string {
	return tableRoute.TableRouteParam.Opcode.String()
}

func (tableRoute *TableRoute) GetKeyspaceName() string {
	return tableRoute.ShardRouteParam.Keyspace.Name
}

func (tableRoute *TableRoute) GetTableName() string {
	return tableRoute.TableName
}

func (tableRoute *TableRoute) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if tableRoute.TableRouteParam == nil {
		return nil, vterrors.VT15001(fmt.Sprintf("No table Route available : %s", tableRoute.TableName))
	}

	resolvedShards, mapBindVariables, errFindRout := tableRoute.ShardRouteParam.findRoute(ctx, vcursor, bindVars)
	if errFindRout != nil {
		return nil, errFindRout
	}

	qr, err := execShard(ctx, tableRoute, vcursor, tableRoute.FieldQuery, mapBindVariables[0], resolvedShards[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (tableRoute *TableRoute) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	splitTableConfig, found := tableRoute.TableRouteParam.LogicTable[tableRoute.TableName]
	if !found {
		return nil, vterrors.VT13001("not found %s splitTableConfig", tableRoute.TableName)
	}

	// 0.计算分片，先写Scatter场景，不用计算路由发到所有分片所有表
	rss, _, err := vcursor.ResolveDestinations(ctx, tableRoute.ShardRouteParam.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}

	// 1.SQL改写 改写表名（逻辑表->实际表）
	queries, err := getTableQueries(tableRoute.Query, splitTableConfig, bindVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	for _, query := range queries {
		rssqueries := make([]*querypb.BoundQuery, 0, len(rss))
		for range rss {
			rssqueries = append(rssqueries, query)
		}

		// 2.执行SQL
		innerResult, errs := vcursor.ExecuteMultiShard(ctx, tableRoute, rss, rssqueries, false /* rollbackOnError */, false /* canAutocommit */)
		if errs != nil {
			return nil, errs[0]
		}
		result.AppendResult(innerResult)
	}

	for _, field := range result.Fields {
		field.Table = tableRoute.TableRouteParam.LogicTable[tableRoute.TableName].LogicTableName
	}

	log.Info(result)

	// 4.可能要处理Order by排序
	if len(tableRoute.OrderBy) == 0 {
		return result, nil
	}
	return tableRoute.sort(result)
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

	return out, err
}

func getTableQueries(stmt sqlparser.Statement, logicTb tableindexes.LogicTableConfig, bvs map[string]*querypb.BindVariable) ([]*querypb.BoundQuery, error) {
	var queries []*querypb.BoundQuery
	for _, act := range logicTb.ActualTableList {
		sql, err := rewriteQuery(stmt, act, logicTb.LogicTableName)
		if err != nil {
			return nil, err
		}
		queries = append(queries, &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bvs,
		})
	}
	return queries, nil
}

func rewriteQuery(stmt sqlparser.Statement, act tableindexes.ActualTable, logicTbName string) (string, error) {
	cloneStmt := sqlparser.DeepCloneStatement(stmt)
	sqlparser.SafeRewrite(cloneStmt, nil, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.TableName:
			if strings.EqualFold(node.Name.String(), logicTbName) {
				cursor.Replace(sqlparser.TableName{
					Name: sqlparser.NewIdentifierCS(act.ActualTableName),
				})
			}
		}
		return true
	})
	return sqlparser.String(cloneStmt), nil
}

func (tableRoute *TableRoute) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func resultMerge(logicTableName string, innerResult []sqltypes.Result) (result *sqltypes.Result, err error) {
	result = &sqltypes.Result{}
	for _, innner := range innerResult {
		result.AppendResult(&innner)
	}

	//field tableName处理，从分表名修改为逻辑表名
	for _, field := range result.Fields {
		field.Table = logicTableName
	}

	return result, nil
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
	if tableRoute.TableRouteParam.Values != nil {
		formattedValues := make([]string, 0, len(tableRoute.TableRouteParam.Values))
		for _, value := range tableRoute.TableRouteParam.Values {
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
		Variant:           tableRoute.ShardRouteParam.Opcode.String() + "-" + tableRoute.TableRouteParam.Opcode.String(),
		Keyspace:          tableRoute.ShardRouteParam.Keyspace,
		TargetDestination: tableRoute.ShardRouteParam.TargetDestination,
		Other:             other,
	}
}
