package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// TablePlanQuery creates a query plan for a given SQL statement
func TablePlanQuery(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (ops.Operator, error) {
	op, err := createLogicalOperatorFromAST(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if err = checkValid(op); err != nil {
		return nil, err
	}

	if op, err = transformToPhysical(ctx, op); err != nil {
		return nil, err
	}

	if op, err = tryHorizonPlanning(ctx, op); err != nil {
		return nil, err
	}

	if op, err = compact(ctx, op); err != nil {
		return nil, err
	}

	_, isRoute := op.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	return op, err
}
