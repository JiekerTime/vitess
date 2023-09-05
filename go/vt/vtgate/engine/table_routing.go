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
	Opcode Opcode

	LogicTable tableindexes.SplitTableMap

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr

	Vindex vindexes.Vindex
}

type LogicTableName string

type ActualTableName []string

func (rp *TableRoutingParameters) findRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (logicTableMap map[string]ActualTableName, err error) {

	logicTableMap = make(map[string]ActualTableName)

	for logicTable := range rp.LogicTable {
		switch rp.Opcode {
		case None:
			return nil, nil
		case DBA:
			//return rp.systemQuery(ctx, vcursor, bindVars)
		case Unsharded, Next:
		//	return rp.unsharded(ctx, vcursor, bindVars)
		case Reference:
		//	return rp.anyShard(ctx, vcursor, bindVars)
		case Scatter:
			logicTableMap[logicTable], err = rp.byDestination(logicTable)
			if err != nil {
				return nil, err
			}
		//	return
		case ByDestination:
		//	return rp.byDestination(ctx, vcursor, bindVars, rp.TargetDestination)
		case EqualUnique:
			logicTableMap[logicTable], err = rp.equal(ctx, vcursor, bindVars, logicTable)
			if err != nil {
				return nil, err
			}
		case Equal, SubShard:
			//switch rp.Vindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.equalMultiCol(ctx, vcursor, bindVars)
			//default:
			//	return rp.equal(ctx, vcursor, bindVars)
			//}
		case IN:
			//switch rp.Vindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.inMultiCol(ctx, vcursor, bindVars)
			//default:
			//	return rp.in(ctx, vcursor, bindVars)
			//}
		case MultiEqual:
			//switch rp.Vindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.multiEqualMultiCol(ctx, vcursor, bindVars)
			//default:
			//	return rp.multiEqual(ctx, vcursor, bindVars)
			//}
		default:
			// Unreachable.
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
		}

	}
	return logicTableMap, nil

}

func (rp *TableRoutingParameters) equal(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableName string) (ActualTableName, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, err
	}
	actualTableName, err := rp.resolveTables(ctx, vcursor, rp.Vindex.(vindexes.TableSingleColumn), tableName, []sqltypes.Value{value.Value()})
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
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
	return rp.tableTransform(ctx, destinations, logicTable)
}

func (rp *TableRoutingParameters) tableTransform(ctx context.Context, destinations []key.TableDestination, logicTable string) (tables []string, err error) {
	var logicTableConfig = rp.LogicTable[logicTable]
	for _, destination := range destinations {
		if err = destination.Resolve(&logicTableConfig, func(table uint64) error {
			//tables = append(tables, rp.LogicTable[table].ActualTableName)
			return nil
		}); err != nil {
			return tables, err
		}
	}
	return tables, nil
}

func (rp *TableRoutingParameters) byDestination(tableName string) (ActualTableName, error) {

	actualTableNameSlice := make(ActualTableName, 0)

	for _, actualTableName := range rp.LogicTable[tableName].ActualTableList {
		actualTableNameSlice = append(actualTableNameSlice, actualTableName.ActualTableName)
	}

	return actualTableNameSlice, nil
}
