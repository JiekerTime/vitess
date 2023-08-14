package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*TableDML)(nil)

type TableDML struct {

}

func (t TableDML) RouteType() string {
	panic("implement me")
}

func (t TableDML) GetKeyspaceName() string {
	panic("implement me")
}

func (t TableDML) GetTableName() string {
	panic("implement me")
}

func (t TableDML) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableDML) NeedsTransaction() bool {
	panic("implement me")
}

func (t TableDML) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableDML) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t TableDML) Inputs() []Primitive {
	panic("implement me")
}

func (t TableDML) description() PrimitiveDescription {
	panic("implement me")
}

