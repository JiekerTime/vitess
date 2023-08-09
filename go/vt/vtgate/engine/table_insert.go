package engine

import (
	"context"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*TableInsert)(nil)

type TableInsert struct {

}

func (t TableInsert) RouteType() string {
	panic("implement me")
}

func (t TableInsert) GetKeyspaceName() string {
	panic("implement me")
}

func (t TableInsert) GetTableName() string {
	panic("implement me")
}

func (t TableInsert) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableInsert) NeedsTransaction() bool {
	panic("implement me")
}

func (t TableInsert) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (t TableInsert) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t TableInsert) Inputs() []Primitive {
	panic("implement me")
}

func (t TableInsert) description() PrimitiveDescription {
	panic("implement me")
}


