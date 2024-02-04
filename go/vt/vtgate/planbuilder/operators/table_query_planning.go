package operators

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// runPhasesForSplitTable is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func runPhasesForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (op ops.Operator, err error) {
	op = root
	for _, phase := range getPhases(ctx) {
		ctx.CurrentPhase = int(phase)
		if rewrite.DebugOperatorTree {
			fmt.Printf("PHASE: %s\n", phase.String())
		}

		op, err = phase.actForSplitTable(ctx, op)
		if err != nil {
			return nil, err
		}

		op, err = runRewritersForSplitTable(ctx, op)
		if err != nil {
			return nil, err
		}

		op, err = compact(ctx, op)
		if err != nil {
			return nil, err
		}
	}

	return addGroupByOnRHSOfJoin(op)
}

func runRewritersForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case *Horizon:
			return pushOrExpandHorizonForSplitTable(ctx, in)
		case *Join:
			return optimizeJoinForSplitTable(ctx, in)
		case *Projection:
			return tryPushingDownProjectionForSplitTable(ctx, in)
		case *Limit:
			return tryPushingDownLimitForSplitTable(ctx, in)
		case *Ordering:
			return tryPushingDownOrderingForSplitTable(ctx, in)
		case *Aggregator:
			return tryPushAggregatorForSplitTable(ctx, in)
		case *Filter:
			return tryPushFilterForSplitTable(ctx, in)
		case *Distinct:
			return tryPushDistinctForSplitTable(ctx, in)
		case *QueryGraph:
			return optimizeQueryGraphForSplitTable(ctx, in)
		case *LockAndComment:
			return pushLockAndComment(in)
		case *Union:
			return tryPushUnionForSplitTable(ctx, in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	return rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
}
