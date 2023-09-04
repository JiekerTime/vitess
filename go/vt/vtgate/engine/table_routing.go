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

func (rp *TableRoutingParameters) findRoute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, routingParameters RoutingParameters) (map[string]ActualTableName, error) {
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
		return rp.byDestination(ctx, vcursor, bindVars, key.DestinationAllShards{}, routingParameters)
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
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
}

func (rp *TableRoutingParameters) byDestination(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, destination key.Destination, routingParameters RoutingParameters) (map[string]ActualTableName, error) {
	//rss, _, err := vcursor.ResolveDestinations(ctx, routingParameters.Keyspace.Name, nil, []key.Destination{destination})
	//if err != nil {
	//	return nil, nil, err
	//}
	//multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	//for i := range multiBindVars {
	//	multiBindVars[i] = bindVars
	//}
	//return rss, multiBindVars, err
	return nil, nil
}
