package engine

import (
	"context"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

var _ Primitive = (*TableRoute)(nil)

type TableRoute struct {
	routeOpCode TableOpCode

	tableIndex tableindexes.TableIndex

	logicTables []tableindexes.LogicTable

	executeEngine Primitive

	orderBy []OrderByParams

	values []evalengine.Expr
}

func (t TableRoute) RouteType() string {
	panic("implement me")
}

func (t TableRoute) GetKeyspaceName() string {
	panic("implement me")
}

func (t TableRoute) GetTableName() string {
	panic("implement me")
}

func (t TableRoute) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableRoute) NeedsTransaction() bool {
	panic("implement me")
}

func (t TableRoute) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableRoute) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t TableRoute) Inputs() []Primitive {
	panic("implement me")
}

func (t TableRoute) description() PrimitiveDescription {
	panic("implement me")
}
