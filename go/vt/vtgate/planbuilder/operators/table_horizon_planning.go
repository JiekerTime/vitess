package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func tryHorizonPlanningForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
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

	// todo(jinyue): 处理聚合操作
	//root, err = addOrderBysAndGroupBysForAggregations(ctx, root)
	//if err != nil {
	//	return nil, err
	//}

	root, err = optimizeHorizonPlanningForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}
	return root, nil
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
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtTableRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return newOp, nil
}

func stopAtTableRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*TableRoute)
	return rewrite.VisitRule(!isRoute)
}

func tryPushingDownOrderingForSplitTable(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(in, src, "push ordering under TableRoute")
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
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownProjectionForSplitTable(ctx *plancontext.PlanningContext, p *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(p, src, "pushed projection under TableRoute")
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushOrExpandHorizonForSplitTable(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	rb, isRoute := in.src().(*TableRoute)
	if isRoute && rb.IsSingleSplitTable() {
		return rewrite.Swap(in, rb, "push horizon into route")
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
	canPushDown := isRoute && sel.Having == nil && !needsOrdering && !qp.NeedsAggregation() && !sel.Distinct && sel.Limit == nil

	if canPushDown {
		return rewrite.Swap(in, rb, "push horizon into route")
	}

	return expandHorizonForSplitTable(ctx, in)
}

func expandHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	sel, _ := horizon.selectStatement().(*sqlparser.Select)

	op, err := createProjectionFromSelect(ctx, horizon)
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
