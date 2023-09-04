package engine

import (
	"context"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

type TableRoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode Opcode

	LogicTable tableindexes.SplitTableMap

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}

type LogicTableName string

type ActualTableName []string

func (rp *TableRoutingParameters) findRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, routingParameters RoutingParameters, tableName string) error {
	switch rp.Opcode {
	case None:
		return nil
	case DBA:
		//return rp.systemQuery(ctx, vcursor, bindVars)
	case Unsharded, Next:
	//	return rp.unsharded(ctx, vcursor, bindVars)
	case Reference:
	//	return rp.anyShard(ctx, vcursor, bindVars)
	case Scatter:
		return rp.byDestination(ctx, vcursor, bindVars, key.DestinationAllShards{}, routingParameters, tableName)
	case ByDestination:
	//	return rp.byDestination(ctx, vcursor, bindVars, rp.TargetDestination)
	case Equal, EqualUnique, SubShard:
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
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	}

	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
}

func (rp *TableRoutingParameters) byDestination(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, destination key.Destination, routingParameters RoutingParameters, tableName string) error {

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)

	logicTableConfig, err := vcursor.FindSplitTable(tableName)
	if err != nil {
		return err
	}
	logicTableMap[tableName] = *logicTableConfig

	return nil
}
