package engine

import (
	"context"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
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

func (tableRoute TableRoute) RouteType() string {
	panic("implement me")
}

func (tableRoute *TableRoute) GetKeyspaceName() string {
	return tableRoute.ShardRouteParam.Keyspace.Name
}

func (tableRoute *TableRoute) GetTableName() string {
	return tableRoute.TableName
}

func (tableRoute *TableRoute) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (tableRoute *TableRoute) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// 0.计算分片，先写Scatter场景，不用计算路由发到所有分片所有表
	rss, _, err := vcursor.ResolveDestinations(ctx, tableRoute.ShardRouteParam.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}

	// 1.SQL改写 改写表名（逻辑表->实际表）

	// 2.执行SQL
	result, errs := vcursor.ExecuteMultiShard(ctx, tableRoute, rss, nil, false /* rollbackOnError */, false /* canAutocommit */)
	if errs != nil {
		return nil, errs[0]
	}
	print(result)

	// 3.结果聚合，主要是多张分表的结果聚合，可能要处理field中table name不同的场景
	var innerQrList = []sqltypes.Result{}

	// 3.结果聚合，主要是多张分表的结果聚合，可能要处理field中table name不同的场景
	resultFinal, error := resultAggr(tableRoute.TableRouteParam.LogicTable.LogicTableName, innerQrList)
	if (error) != nil {
		return nil, errs[0]
	}
	print(resultFinal)

	// 4.可能要处理Order by排序

	panic("implement me")
}

func (tableRoute *TableRoute) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// 分表结果聚合
func resultAggr(logicTableName string, innerResult []sqltypes.Result) (result *sqltypes.Result, err error) {

	result = &sqltypes.Result{}
	//结果聚合
	for _, innner := range innerResult {
		result.AppendResult(&innner)
	}

	//field tableName处理
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
