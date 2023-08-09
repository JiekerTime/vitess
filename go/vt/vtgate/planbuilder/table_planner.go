package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func buildSelectTablePlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan,
) (plan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	plan = ksPlan

	_, err = operators.TablePlanQuery(nil, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	return plan, nil, nil, err
}
