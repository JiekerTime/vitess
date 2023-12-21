package operators

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryPushAggregatorForSplitTable(ctx *plancontext.PlanningContext, aggregator *Aggregator,
) (output ops.Operator, applyResult *rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	switch src := aggregator.Source.(type) {
	case *TableRoute:
		output, applyResult, err = pushAggregationThroughTableRoute(ctx, aggregator, src)
	default:
		return aggregator, rewrite.SameTree, nil
	}

	if err != nil {
		return nil, nil, err
	}

	if output == nil {
		return aggregator, rewrite.SameTree, nil
	}

	aggregator.Pushed = true
	return
}

func pushAggregationThroughTableRoute(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	route *TableRoute,
) (ops.Operator, *rewrite.ApplyResult, error) {
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

	if !reachedPhase(ctx, delegateAggregation) {
		return nil, nil, nil
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.SplitAggregatorBelowRoute(route.Inputs())
	aggrBelowRoute.Aggregations = nil

	err := pushAggregationsForSplitTable(ctx, aggregator, aggrBelowRoute)
	if err != nil {
		return nil, nil, err
	}

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

// pushAggregations splits aggregations between the original aggregator and the one we are pushing down
func pushAggregationsForSplitTable(ctx *plancontext.PlanningContext, aggregator *Aggregator, aggrBelowRoute *Aggregator) error {
	canPushDistinctAggr, distinctExpr, err := checkIfWeCanPushForSplitTable(ctx, aggregator)
	if err != nil {
		return err
	}

	distinctAggrGroupByAdded := false

	for i, aggr := range aggregator.Aggregations {
		if !aggr.Distinct || canPushDistinctAggr {
			aggrBelowRoute.Aggregations = append(aggrBelowRoute.Aggregations, aggr)
			aggregateTheAggregate(aggregator, i)
			continue
		}

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		aeDistinctExpr := aeWrap(distinctExpr)
		aggrBelowRoute.Columns[aggr.ColOffset] = aeDistinctExpr

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		// Adding to group by can be done only once even though there are multiple distinct aggregation with same expression.
		if !distinctAggrGroupByAdded {
			groupBy := NewGroupBy(distinctExpr, distinctExpr, aeDistinctExpr)
			groupBy.ColOffset = aggr.ColOffset
			aggrBelowRoute.Grouping = append(aggrBelowRoute.Grouping, groupBy)
			distinctAggrGroupByAdded = true
		}
	}

	if !canPushDistinctAggr {
		aggregator.DistinctExpr = distinctExpr
	}

	return nil
}

func checkIfWeCanPushForSplitTable(ctx *plancontext.PlanningContext, aggregator *Aggregator) (bool, sqlparser.Expr, error) {
	canPush := true
	var distinctExpr sqlparser.Expr
	var differentExpr *sqlparser.AliasedExpr

	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}

		innerExpr := aggr.Func.GetArg()
		if !exprHasUniqueTableIndex(ctx, innerExpr) {
			canPush = false
		}
		if distinctExpr == nil {
			distinctExpr = innerExpr
		}
		if !ctx.SemTable.EqualsExpr(distinctExpr, innerExpr) {
			differentExpr = aggr.Original
		}
	}

	if !canPush && differentExpr != nil {
		return false, nil, vterrors.VT12001(fmt.Sprintf("only one DISTINCT aggregation is allowed in a SELECT: %s", sqlparser.String(differentExpr)))
	}

	return canPush, distinctExpr, nil
}
