package operators

import (
	"fmt"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func tryHorizonPlanningForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
	if _, ok := root.(*Horizon); !ok {
		return root, nil
	}

	output, err = planHorizonsForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}
	output, err = planOffsetsForSplitTable(ctx, output)
	if err != nil {
		return nil, err
	}

	return addTruncationOrProjectionToReturnOutputForSplitTable(ctx, root, output)
}

// planHorizonsForSplitTable is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func planHorizonsForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	root, err := optimizeHorizonPlanningForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}

	// Adding Ordering Op - This is needed if there is no explicit ordering and aggregation is performed on top of route.
	// Adding Group by - This is needed if the grouping is performed on a join with a join condition then
	//                   aggregation happening at route needs a group by to ensure only matching rows returns
	//                   the aggregations otherwise returns no result.
	root, err = addOrderBysAndGroupBysForAggregationsForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}

	root, err = optimizeHorizonPlanningForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func addOrderBysAndGroupBysForAggregationsForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case *Aggregator:
			if in.Pushed {
				// first we update the incoming columns, so we know about any new columns that have been added
				columns, err := in.Source.GetColumns()
				if err != nil {
					return nil, nil, err
				}
				in.Columns = columns
			}

			requireOrdering, err := needsOrdering(in, ctx)
			if err != nil {
				return nil, nil, err
			}
			if !requireOrdering {
				return in, rewrite.SameTree, nil
			}
			in.Source = &Ordering{
				Source: in.Source,
				Order: slices2.Map(in.Grouping, func(from GroupBy) ops.OrderBy {
					return from.AsOrderBy()
				}),
			}
			return in, rewrite.NewTree("added ordering before aggregation", in), nil
		case *ApplyJoin:
			_ = rewrite.Visit(in.RHS, func(op ops.Operator) error {
				aggr, isAggr := op.(*Aggregator)
				if !isAggr {
					return nil
				}
				if len(aggr.Grouping) == 0 {
					gb := sqlparser.NewIntLiteral(".0")
					aggr.Grouping = append(aggr.Grouping, NewGroupBy(gb, gb, aeWrap(gb)))
				}
				return nil
			})
		}
		return in, rewrite.SameTree, nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtTableRoute)
}

func optimizeHorizonPlanningForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case horizonLike:
			return pushOrExpandHorizonForSplitTable(ctx, in)
		case *Ordering:
			return tryPushingDownOrderingForSplitTable(ctx, in)
		case *Projection:
			return tryPushingDownProjectionForSplitTable(ctx, in)
		case *Limit:
			return tryPushingDownLimitForSplitTable(ctx, in)
		case *Aggregator:
			return tryPushingDownAggregatorForSplitTable(ctx, in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtTableRoute)
	if err != nil {
		return nil, err
	}

	return newOp, nil
}

func tryPushingDownLimitForSplitTable(ctx *plancontext.PlanningContext, in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return tryPushingDownLimitInRouteForSplitTable(ctx, in, src)
	case *Projection:
		return rewrite.Swap(in, src, "push limit under projection")
	case *Aggregator:
		if isCrossShard(ctx.GetRoute()) {
			return rewrite.Swap(in, src, "limit pushed into aggregator")
		}
		return in, rewrite.SameTree, nil
	default:
		return setUpperLimitForSplitTable(in)
	}
}

func tryPushingDownLimitInRouteForSplitTable(ctx *plancontext.PlanningContext, in *Limit, src *TableRoute) (ops.Operator, *rewrite.ApplyResult, error) {
	if src.IsSingleSplitTable() || isCrossShard(ctx.GetRoute()) {
		return rewrite.Swap(in, src, "limit pushed into tableRoute")
	}
	return setUpperLimitForSplitTable(in)
}

func isCrossShard(route engine.Route) bool {
	switch route.Opcode {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference:
		return false
	}
	return true
}

func setUpperLimitForSplitTable(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Pushed {
		return in, rewrite.SameTree, nil
	}
	in.Pushed = true
	visitor := func(op ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		return op, rewrite.SameTree, nil
	}
	shouldVisit := func(op ops.Operator) rewrite.VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin:
			// we can't push limits down on either side
			return rewrite.SkipChildren
		case *TableRoute:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			return rewrite.SkipChildren
		default:
			return rewrite.VisitChildren
		}
	}

	_, err := rewrite.TopDown(in.Source, TableID, visitor, shouldVisit)
	if err != nil {
		return nil, nil, err
	}
	return in, rewrite.SameTree, nil
}

func stopAtTableRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*TableRoute)
	return rewrite.VisitRule(!isRoute)
}

func tryPushingDownOrderingForSplitTable(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(in, src, "push ordering under tableRoute")
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, rewrite.NewTree("remove double ordering", src), nil
	case *Projection:
		// we can move ordering under a projection if it's not introducing a column we're sorting by
		for _, by := range in.Order {
			if !fetchByOffset(by.SimplifiedExpr) {
				return in, rewrite.SameTree, nil
			}
		}
		return rewrite.Swap(in, src, "push ordering under projection")
	case *Aggregator:
		if !(src.QP.AlignGroupByAndOrderBy(ctx) || overlaps(ctx, in.Order, src.Grouping)) {
			return in, rewrite.SameTree, nil
		}
		return pushOrderingUnderAggr(ctx, in, src)
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownProjectionForSplitTable(_ *plancontext.PlanningContext, p *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(p, src, "pushed projection under tableRoute")
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushOrExpandHorizonForSplitTable(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	rb, isTableRoute := in.src().(*TableRoute)
	if isTableRoute && rb.IsSingleSplitTable() && !isCrossShard(ctx.GetRoute()) {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, nil, errHorizonNotPlanned()
	}

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isTableRoute && sel.Having == nil && !needsOrdering && !qp.NeedsAggregation() && !sel.Distinct && sel.Limit == nil

	if canPushDown {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	return expandHorizonForSplitTable(ctx, in)
}

func expandHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	sel, _ := horizon.selectStatement().(*sqlparser.Select)

	op, err := createProjectionFromSelectForSplitTable(ctx, horizon)

	if err != nil {
		return nil, nil, err
	}

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Source: op,
			QP:     qp,
		}
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
	}

	return op, rewrite.NewTree("expand horizon into smaller components", op), nil
}

func addTruncationOrProjectionToReturnOutputForSplitTable(ctx *plancontext.PlanningContext, oldHorizon ops.Operator, output ops.Operator) (ops.Operator, error) {
	cols, err := output.GetSelectExprs(ctx)
	if err != nil {
		return nil, err
	}

	horizon := oldHorizon.(*Horizon)

	sel := sqlparser.GetFirstSelect(horizon.Select)

	if len(sel.SelectExprs) == len(cols) {
		return output, nil
	}

	if tryTruncateColumnsAt(output, len(sel.SelectExprs)) {
		return output, nil
	}

	return nil, vterrors.VT13001("split table not implement yet")
}

// createProjectionFromSelectForSplitTable is simplified to createProjectionFromSelect.
func createProjectionFromSelectForSplitTable(ctx *plancontext.PlanningContext, horizon horizonLike) (out ops.Operator, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(qp, horizon.src())
		if err != nil {
			return nil, err
		}
		if _, isDerived := horizon.(*Derived); isDerived {
			return nil, vterrors.VT13001("todo: Derived")
		}
		out = projX

		return out, nil
	}

	aggregations, err := qp.AggregationExpressions(ctx)
	if err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
	}

	if _, isDerived := horizon.(*Derived); isDerived {
		return nil, vterrors.VT13001("todo: Derived")
	}

outer:
	for colIdx, expr := range qp.SelectExprs {
		ae, err := expr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		addedToCol := false
		for idx, groupBy := range a.Grouping {
			if ctx.SemTable.EqualsExprWithDeps(groupBy.SimplifiedExpr, ae.Expr) {
				if !addedToCol {
					a.Columns = append(a.Columns, ae)
					addedToCol = true
				}
				if groupBy.ColOffset < 0 {
					a.Grouping[idx].ColOffset = colIdx
				}
			}
		}
		if addedToCol {
			continue
		}
		for idx, aggr := range a.Aggregations {
			if ctx.SemTable.EqualsExprWithDeps(aggr.Original.Expr, ae.Expr) && aggr.ColOffset < 0 {
				a.Columns = append(a.Columns, ae)
				a.Aggregations[idx].ColOffset = colIdx
				continue outer
			}
		}
		return nil, vterrors.VT13001(fmt.Sprintf("Could not find the %s in aggregation in the original query", sqlparser.String(ae)))
	}

	return a, nil
}
