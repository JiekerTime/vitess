package operators

import (
	"fmt"

	"vitess.io/vitess/go/slice"

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
				columns, err := in.Source.GetColumns(ctx)
				if err != nil {
					return nil, nil, err
				}
				in.Columns = columns
			}

			requireOrdering, err := needsOrdering(ctx, in)
			if err != nil {
				return nil, nil, err
			}
			if !requireOrdering {
				return in, rewrite.SameTree, nil
			}
			in.Source = &Ordering{
				Source: in.Source,
				Order: slice.Map(in.Grouping, func(from GroupBy) ops.OrderBy {
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
		case *Horizon:
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

func pushOrExpandHorizonForSplitTable(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	rb, isTableRoute := in.src().(*TableRoute)
	if isTableRoute && rb.IsSingleSplitTable() {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	hasHaving := isSel && sel.Having != nil

	canPush := isTableRoute &&
		!hasHaving &&
		!needsOrdering &&
		!qp.NeedsAggregation() &&
		!in.selectStatement().IsDistinct() &&
		in.selectStatement().GetLimit() == nil

	if canPush {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	return expandHorizonForSplitTable(ctx, in)
}

func checkForSupportSql(ctx *plancontext.PlanningContext, sel *sqlparser.Select) error {
	if sel.Distinct {
		return vterrors.VT12001("distinct in split table")
	} else if sel.Having != nil {
		return vterrors.VT12001(fmt.Sprintf("statement(%s) in split table", sqlparser.String(sel.Having)))
	} else if hasSubqueryInExprsAndWhere(sel) {
		return vterrors.VT12001("subquery in split table")
	}
	return nil
}

func expandHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
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
		return nil, nil, vterrors.VT12001("distinct in split table")
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

	sel := sqlparser.GetFirstSelect(horizon.Query)

	if len(sel.SelectExprs) == len(cols) {
		return output, nil
	}

	if tryTruncateColumnsAt(output, len(sel.SelectExprs)) {
		return output, nil
	}

	return nil, vterrors.VT13001("split table not implement yet")
}

// createProjectionFromSelectForSplitTable is simplified to createProjectionFromSelect.
func createProjectionFromSelectForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon) (out ops.Operator, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	var dt *DerivedTable
	if horizon.TableId != nil {
		dt = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(ctx, qp, horizon.src())
		if err != nil {
			return nil, err
		}
		projX.DT = dt
		out = projX

		return out, nil
	}

	aggregations, complexAggr, err := qp.AggregationExpressions(ctx, true)
	if err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
		DT:           dt,
	}

	if complexAggr {
		return createProjectionForComplexAggregation(a, qp)
	}
	return createProjectionForSimpleAggregation(ctx, a, qp)
}
