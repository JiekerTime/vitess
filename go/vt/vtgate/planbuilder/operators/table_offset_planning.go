package operators

import (
	"fmt"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// planOffsetsForSplitTable will walk the tree top down, adding offset information to columns in the tree for use in further optimization,
func planOffsetsForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	type offsettable interface {
		planOffsets(ctx *plancontext.PlanningContext) error
	}

	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Derived, *Horizon:
			return nil, nil, vterrors.VT13001(fmt.Sprintf("should not see %T here", in))
		case offsettable:
			err = op.planOffsets(ctx)
		}
		if err != nil {
			return nil, nil, err
		}
		return in, rewrite.SameTree, nil
	}

	op, err := rewrite.TopDown(root, TableID, visitor, stopAtTableRoute)
	if err != nil {
		return nil, err
	}

	return op, nil
}
