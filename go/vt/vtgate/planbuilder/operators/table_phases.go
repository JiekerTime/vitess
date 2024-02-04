package operators

import (
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func (p Phase) actForSplitTable(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	switch p {
	case pullDistinctFromUnion:
		return pullDistinctFromUNION(ctx, op)
	case delegateAggregation:
		return enableDelegateAggregation(ctx, op)
	case addAggrOrdering:
		return addOrderBysForAggregations(ctx, op)
	case cleanOutPerfDistinct:
		return removePerformanceDistinctAboveRoute(ctx, op)
	}

	return op, nil
}
