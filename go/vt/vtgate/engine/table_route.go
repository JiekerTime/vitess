package engine

import (
	"context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*TableRoute)(nil)

type TableRoute struct {
	// TableName specifies the tables to send the query to.
	TableName string

	shardRouteParam *RoutingParameters

	tableRouteParam *TableRoutingParameters

	// Query specifies the query to be executed.
	Query sqlparser.Statement

	// FieldQuery specifies the query to be executed for a GetFieldInfo request.
	FieldQuery string

	orderBy []OrderByParams

	// Route does not take inputs
	noInputs

	// Route does not need transaction handling
	noTxNeeded
}

func (tableRoute TableRoute) RouteType() string {
	panic("implement me")
}

func (tableRoute TableRoute) GetKeyspaceName() string {
	return tableRoute.shardRouteParam.Keyspace.Name
}

func (tableRoute TableRoute) GetTableName() string {
	return tableRoute.TableName
}

func (tableRoute TableRoute) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (tableRoute TableRoute) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// 0.计算分片，先写Scatter场景，不用计算路由发到所有分片所有表
	rss, _, err := vcursor.ResolveDestinations(ctx, tableRoute.shardRouteParam.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
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

	// 4.可能要处理Order by排序

	panic("implement me")
}

func (tableRoute TableRoute) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (tableRoute TableRoute) description() PrimitiveDescription {
	panic("implement me")
}
