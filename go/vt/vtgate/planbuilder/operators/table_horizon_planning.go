package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func stopAtTableRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*TableRoute)
	return rewrite.VisitRule(!isRoute)
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
