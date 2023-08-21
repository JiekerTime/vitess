package engine

import (
	"context"
	"encoding/json"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

type TableOpCode int

const (
	TableEqualUnique = TableOpCode(iota)

	TableIn

	TableScatter
)

var tableOpName = map[TableOpCode]string{
	TableEqualUnique: "TableEqualUnique",
	TableIn:          "TableIn",
	TableScatter:     "TableScatter",
}

// MarshalJSON serializes the Opcode as a JSON string.
// It's used for testing and diagnostics.
func (code TableOpCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(tableOpName[code])
}

// String returns a string presentation of this opcode
func (code TableOpCode) String() string {
	return tableOpName[code]
}

type TableRoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode     TableOpCode
	Vindex     vindexes.Vindex
	LogicTable tableindexes.LogicTableConfig

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}

func (rp *TableRoutingParameters) findTableRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]string, error) {
	switch rp.Opcode {
	case TableScatter:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	case TableEqualUnique:
		switch rp.Vindex.(type) {
		case vindexes.TableMultiColumn:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		default:
			return rp.equal(ctx, vcursor, bindVars)
		}
	case TableIn:
		switch rp.Vindex.(type) {
		case vindexes.TableMultiColumn:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		}
	default:
		// Unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	}
}

func (rp *TableRoutingParameters) equal(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]string, error) {

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, err
	}
	rss, err := rp.resolveTables(ctx, vcursor, rp.Vindex.(vindexes.TableSingleColumn), rp.LogicTable.LogicTableName, []sqltypes.Value{value.Value()})
	if err != nil {
		return nil, err
	}
	return rss, nil

}
func (rp *TableRoutingParameters) resolveTables(ctx context.Context, vcursor VCursor, vindex vindexes.TableSingleColumn, logicTable string, vindexKeys []sqltypes.Value) ([]string, error) {
	// Convert vindexKeys to []*querypb.Value
	ids := make([]*querypb.Value, len(vindexKeys))
	for i, vik := range vindexKeys {
		ids[i] = sqltypes.ValueToProto(vik)
	}

	// Map using the Vindex
	destinations, err := vindex.Map(ctx, vcursor, vindexKeys)
	if err != nil {
		return nil, err

	}

	// And use the Resolver to map to ResolvedShards.
	return rp.tableTransform(ctx, destinations)
}

// tableTransformation Logical Table to Physical Tables
func (rp *TableRoutingParameters) tableTransform(ctx context.Context, destinations []key.TableDestination) (tables []string, err error) {

	for _, destination := range destinations {
		if err = destination.Resolve(&rp.LogicTable, func(table uint64) error {
			tables = append(tables, rp.LogicTable.ActualTableList[table].ActualTableName)
			return nil
		}); err != nil {
			return tables, err
		}
	}
	return tables, nil
}
