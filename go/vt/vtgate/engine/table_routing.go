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
}

func (rp *TableRoutingParameters) findTableRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (logicTableMap map[string][]vindexes.ActualTable, err error) {
	logicTableMap = make(map[string][]vindexes.ActualTable)

	for logicTableName, logicTable := range rp.LogicTable {
		switch rp.TableOpcode {
		case None:
			return nil, nil
		case Scatter:
			logicTableMap[logicTableName], err = rp.byDestination(ctx, vcursor, logicTableName, vindexes.DestinationAllTables{})
		case Equal, EqualUnique, SubShard:
			logicTableMap[logicTableName], err = rp.equal(ctx, vcursor, logicTable.TableVindex, bindVars, logicTableName)
		case IN:
			logicTableMap[logicTableName], err = rp.in(ctx, vcursor, logicTable.TableVindex, bindVars, logicTableName)
		case MultiEqual:
			logicTableMap[logicTableName], err = rp.multiEqual(ctx, vcursor, logicTable.TableVindex, bindVars, logicTableName)
		default:
			// Unreachable.
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.TableOpcode)
		}
	}
	if err != nil {
		return nil, err
	}
	return logicTableMap, nil
}

func (rp *TableRoutingParameters) equal(ctx context.Context, vcursor VCursor, vindex vindexes.Vindex, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}
	actualTableName, err := rp.resolveTables(ctx, vcursor, vindex, tableName, []sqltypes.Value{value.Value(vcursor.ConnCollation())})
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
}

func (rp *TableRoutingParameters) multiEqual(ctx context.Context, vcursor VCursor, vindex vindexes.Vindex, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}
	actualTableName, err := rp.resolveTables(ctx, vcursor, vindex, tableName, value.TupleValues())
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

func (rp *TableRoutingParameters) in(ctx context.Context, vcursor VCursor, vindex vindexes.Vindex, bindVars map[string]*querypb.BindVariable, tableName string) ([]vindexes.ActualTable, error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	value, err := env.Evaluate(rp.TableValues[0])
	if err != nil {
		return nil, err
	}

	actualTableName, err := rp.resolveTables(ctx, vcursor, vindex, tableName, value.TupleValues())
	if err != nil {
		return nil, err
	}
	return actualTableName, nil
}

func (rp *TableRoutingParameters) resolveTables(ctx context.Context, vcursor VCursor, vindex vindexes.Vindex, logicTable string, vindexKeys []sqltypes.Value) ([]vindexes.ActualTable, error) {
	// Convert vindexKeys to []*querypb.Value
	ids := make([]*querypb.Value, len(vindexKeys))
	for i, vik := range vindexKeys {
		ids[i] = sqltypes.ValueToProto(vik)
	}
	var destinations []vindexes.TableDestination
	var err error
	switch tableVindex := vindex.(type) {
	case vindexes.TableSingleColumn:
		// Map using the Vindex
		destinations, err = vindex.(vindexes.TableSingleColumn).Map(ctx, vcursor, vindexKeys)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported tableVindex: %v", tableVindex)
	}
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
