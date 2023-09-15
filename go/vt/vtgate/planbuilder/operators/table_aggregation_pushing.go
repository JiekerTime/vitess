package operators

import (
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryPushingDownAggregatorForSplitTable(ctx *plancontext.PlanningContext, aggregator *Aggregator,
) (output ops.Operator, applyResult *rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	aggregator.Pushed = true
	switch src := aggregator.Source.(type) {
	case *TableRoute:
		output, applyResult, err = pushDownAggregationThroughRouteForSplitTable(ctx, aggregator, src)
	default:
		return aggregator, rewrite.SameTree, nil
	}

	if applyResult != rewrite.SameTree && aggregator.Original {
		aggregator.aggregateTheAggregates()
	}

	return
}

func pushDownAggregationThroughRouteForSplitTable(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	route *TableRoute,
) (ops.Operator, *rewrite.ApplyResult, error) {
	// If the route is single-splitTable, or we are grouping by sharding keys, we can just push down the aggregation
	// or logicPlan of shardKeyspace is multiShard.
	if route.IsSingleSplitTable() || isMultiShard(ctx.KsERoute) {
		return rewrite.Swap(aggregator, route, "push down aggregation under tableRoute - remove original")
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.Clone([]ops.Operator{route.Source}).(*Aggregator)
	aggrBelowRoute.Pushed = false
	aggrBelowRoute.Original = false

	// Set the source of the route to the new aggregator placed below the route.
	route.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree("push aggregation under tableRoute - remove original", aggregator), nil
	}

	return aggregator, rewrite.NewTree("push aggregation under tableRoute - keep original", aggregator), nil
}
