package engine

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*TableUpdate)(nil)

// TableUpdate represents the instructions to perform an update for split table.
type TableUpdate struct {
	*TableDML

	// Update does not take inputs
	noInputs
}

// TryExecute performs a non-streaming exec.
func (upd *TableUpdate) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	rss, _, err := upd.ShardRouteParam.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	actualTableMap, err := upd.TableRouteParam.findTableRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	SortTableList(actualTableMap)

	switch upd.ShardRouteParam.Opcode {
	case EqualUnique, IN, Scatter, MultiEqual:
		return upd.execMultiDestination(ctx, upd, vcursor, bindVars, rss, nil, actualTableMap)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", upd.ShardRouteParam.Opcode)
	}
}

func (upd *TableUpdate) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := upd.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (upd *TableUpdate) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", upd.AST)
}

func (upd *TableUpdate) description() PrimitiveDescription {
	other := map[string]any{
		"Queries":              sqlparser.String(upd.AST),
		"Table":                upd.GetTableName(),
		"MultiShardAutocommit": upd.MultiShardAutocommit,
		"QueryTimeout":         upd.QueryTimeout,
	}

	if len(upd.ShardRouteParam.Values) > 0 {
		s := make([]string, 0, len(upd.ShardRouteParam.Values))
		for _, value := range upd.ShardRouteParam.Values {
			s = append(s, evalengine.FormatExpr(value))
		}
		other["Values"] = s
	}
	if len(upd.TableRouteParam.TableValues) > 0 {
		s := make([]string, 0, len(upd.TableRouteParam.TableValues))
		for _, value := range upd.TableRouteParam.TableValues {
			s = append(s, evalengine.FormatExpr(value))
		}
		other["TableValues"] = s
	}

	return PrimitiveDescription{
		OperatorType: "TableUpdate",
		Keyspace:     upd.ShardRouteParam.Keyspace,
		Variant:      upd.ShardRouteParam.Opcode.String() + "-" + upd.TableRouteParam.TableOpcode.String(),
		Other:        other,
	}
}
