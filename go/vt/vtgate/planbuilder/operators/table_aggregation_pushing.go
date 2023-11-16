package operators

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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
	if err := checkIfHasDistinct(aggregator); err != nil {
		return nil, nil, err
	}

	// If the route is single-splitTable, or we are grouping by table index keys, we can just push down the aggregation
	if route.IsSingleSplitTable() || overlappingUniqueTableIndex(ctx, aggregator.Grouping) {
		return rewrite.Swap(aggregator, route, "push down aggregation under tableRoute - remove original")
	}
	// If the logicPlan of shardKeyspace has Aggregation, then the split table plan does not need to generate it again.
	// such as Cross-shard aggregation functions, Cross-shard group by (not grouping by sharding keys)
	if aggregator.Grouping == nil && isCrossShard(ctx.GetRoute()) {
		return rewrite.Swap(aggregator, route, "push down aggregation under tableRoute, Cross-shard aggregation functions - remove original")
	}
	if aggregator.Grouping != nil && isCrossShard(ctx.GetRoute()) && !overlappingUniqueVindex(ctx, aggregator.Grouping) {
		return rewrite.Swap(aggregator, route, "push down aggregation under tableRoute, Cross-shard group by - remove original")
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

func overlappingUniqueTableIndex(ctx *plancontext.PlanningContext, groupByExprs []GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueTableIndex(ctx, groupByExpr.SimplifiedExpr) {
			return true
		}
	}
	return false
}

func exprHasUniqueTableIndex(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := ctx.SemTable.RecursiveDeps(expr)
	tableInfo, err := ctx.SemTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	logicTableConfig := ctx.SplitTableConfig[tableInfo.GetVindexTable().Name.String()]
	if len(logicTableConfig.TableIndexColumn) > 1 {
		return false
	}
	column := logicTableConfig.TableIndexColumn[0].Column
	return col.Name.Equal(column)
}

func checkIfHasDistinct(aggregator *Aggregator) error {
	for _, aggr := range aggregator.Aggregations {
		if aggr.Distinct {
			return vterrors.VT12001(fmt.Sprintf("statement(%s) in split table", sqlparser.String(aggr.Original)))
		}
	}
	return nil
}
