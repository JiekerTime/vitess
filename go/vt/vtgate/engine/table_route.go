package engine

import (
	"context"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/tableindexes"

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

func (tableRoute *TableRoute) RouteType() string {
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
	queries, err := getTableQueries(tableRoute.Query, tableRoute.TableRouteParam.LogicTable, bindVars)
	if err != nil {
		return nil, err
	}
	// 2.执行SQL
	result, errs := vcursor.ExecuteMultiShard(ctx, tableRoute, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	if errs != nil {
		return nil, errs[0]
	}
	print(result)

	// 3.结果聚合，主要是多张分表的结果聚合，可能要处理field中table name不同的场景

	// 4.可能要处理Order by排序

	panic("implement me")
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
	cloneStmt := sqlparser.CloneStatement(stmt)
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
