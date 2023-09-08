package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type TableRoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode     Opcode
	LogicTable tableindexes.SplitTableMap
	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}
type LogicTableName string
type ActualTableName []string

func (rp *TableRoutingParameters) findTableRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (map[string]ActualTableName, error) {

	logicTableMap := make(map[string]ActualTableName)
	var err error
	for logicTable := range rp.LogicTable {
		switch rp.Opcode {
		case Scatter:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		case EqualUnique:
			logicTableMap[logicTable], err = rp.equal(ctx, vcursor, bindVars)
			if err != nil {
				return nil, err
			}
		case IN:

			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		default:
			// Unreachable.
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		}
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)

}

func (rp *TableRoutingParameters) equal(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (ActualTableName, error) {

	//env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	//value, err := env.Evaluate(rp.Values[0])
	//if err != nil {
	//	return nil, err
	//}
	//rss, err := rp.resolveTables(ctx, vcursor, rp.Vindex.(vindexes.TableSingleColumn), rp.LogicTable.LogicTableName, []sqltypes.Value{value.Value()})
	//if err != nil {
	//	return nil, err
	//}
	return nil, nil

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

	//for _, destination := range destinations {
	//	if err = destination.Resolve(&rp.LogicTable, func(table uint64) error {
	//		tables = append(tables, rp.LogicTable.ActualTableList[table].ActualTableName)
	//		return nil
	//	}); err != nil {
	//		return tables, err
	//	}
	//}
	return tables, nil
}
