package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type TableRoutingParameters struct {
	// TableOpcode is the execution opcode.
	TableOpcode Opcode

	// TableValues specifies the vindex values to use for routing.
	TableValues []evalengine.Expr

	LogicTable vindexes.SplitTableMap

	TableVindex vindexes.Vindex
}

type LogicTableName string

func (rp *TableRoutingParameters) findTableRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (logicTableMap map[string][]vindexes.ActualTable, err error) {
	logicTableMap = make(map[string][]vindexes.ActualTable)

	for logicTable := range rp.LogicTable {
		switch rp.TableOpcode {
		case None:
			return nil, nil
		case DBA:
			//return rp.systemQuery(ctx, vcursor, bindVars)
		case Unsharded, Next:
		//	return rp.unsharded(ctx, vcursor, bindVars)
		//case Reference:
		//	logicTableMap[logicTable], err = rp.anyTable(ctx, vcursor, logicTable, key.DestinationAnyTable{})
		//	if err != nil {
		//		return nil, err
		//	}
		case Scatter:
			logicTableMap[logicTable], err = rp.byDestination(ctx, vcursor, logicTable, vindexes.DestinationAllTables{})
			if err != nil {
				return nil, err
			}
		case ByDestination:
			//logicTableMap[logicTable], err = rp.byDestination(ctx, vcursor, logicTable, key.DestinationAllTables{})
			//if err != nil {
			//	return nil, err
			//}
		case Equal, EqualUnique, SubShard:
			switch rp.TableVindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.equalMultiCol(ctx, vcursor, bindVars)
			default:
				logicTableMap[logicTable], err = rp.equal(ctx, vcursor, bindVars, logicTable)
				if err != nil {
					return nil, err
				}
			}
		case IN:
			switch rp.TableVindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.inMultiCol(ctx, vcursor, bindVars)
			default:
				logicTableMap[logicTable], err = rp.in(ctx, vcursor, bindVars, logicTable)
				if err != nil {
					return nil, err
				}
			}
		case MultiEqual:
			switch rp.TableVindex.(type) {
			//case vindexes.MultiColumn:
			//	return rp.multiEqualMultiCol(ctx, vcursor, bindVars)
			default:
				logicTableMap[logicTable], err = rp.multiEqual(ctx, vcursor, bindVars, logicTable)
				if err != nil {
					return nil, err
				}
			}
		default:
			// Unreachable.
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.TableOpcode)
		}
	}
	return logicTableMap, nil

}

func (rp *TableRoutingParameters) equal(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}
	actualTableName, err := rp.resolveTables(ctx, vcursor, rp.TableVindex.(vindexes.TableSingleColumn), tableName, []sqltypes.Value{value.Value(vcursor.ConnCollation())})
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
}

func (rp *TableRoutingParameters) multiEqual(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}
	actualTableName, err := rp.resolveTables(ctx, vcursor, rp.TableVindex.(vindexes.TableSingleColumn), tableName, value.TupleValues())
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
}

func (rp *TableRoutingParameters) anyTable(ctx context.Context, vcursor VCursor, logicTable string, destination vindexes.DestinationAnyTable) (tables []vindexes.ActualTable, err error) {

	var logicTableConfig = rp.LogicTable[logicTable]

	if err = destination.Resolve(logicTableConfig, func(actualTableIndex int) error {
		tables = append(tables, rp.LogicTable[logicTable].ActualTableList[actualTableIndex])
		return nil
	}); err != nil {
		return tables, err
	}

	return tables, nil
}

func (rp *TableRoutingParameters) in(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}

	actualTableName, err := rp.resolveTables(ctx, vcursor, rp.TableVindex.(vindexes.TableSingleColumn), tableName, value.TupleValues())
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
}

func (rp *TableRoutingParameters) resolveTables(ctx context.Context, vcursor VCursor, vindex vindexes.TableSingleColumn, logicTable string, vindexKeys []sqltypes.Value) ([]vindexes.ActualTable, error) {
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

func (rp *TableRoutingParameters) tableTransform(ctx context.Context, destinations []vindexes.TableDestination, logicTable string) (tables []vindexes.ActualTable, err error) {
	var logicTableConfig = rp.LogicTable[logicTable]
	for _, destination := range destinations {
		if err = destination.Resolve(logicTableConfig, func(actualTableIndex int) error {
			tables = append(tables, rp.LogicTable[logicTable].ActualTableList[actualTableIndex])
			return nil
		}); err != nil {
			return tables, err
		}
	}
	return tables, nil
}

func (rp *TableRoutingParameters) byDestination(ctx context.Context, vcursor VCursor, logicTable string, destination vindexes.TableDestination) (tables []vindexes.ActualTable, err error) {
	var logicTableConfig = rp.LogicTable[logicTable]

	if err = destination.Resolve(logicTableConfig, func(actualTableIndex int) error {
		tables = append(tables, rp.LogicTable[logicTable].ActualTableList[actualTableIndex])
		return nil
	}); err != nil {
		return tables, err
	}

	return tables, nil
}

func (rp *TableRoutingParameters) IsSingleTable() bool {
	switch rp.TableOpcode {
	case EqualUnique:
		return true
	}
	return false
}
