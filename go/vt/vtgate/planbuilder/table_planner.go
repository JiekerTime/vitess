package planbuilder

import (
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func buildTableSelectPlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan,
) (plan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	// get split table config
	config := plancontext.LogicTableConfig{
		LogicTable:         "t_user",
		ShardingColumnName: "col",
	}
	name := ksPlan.Primitive().GetTableName()
	if name != config.LogicTable {
		return ksPlan, ctx.SemTable, nil, err
	}

	// getRoutePlan
	route, err := getRoutePlan(ksPlan)
	if err != nil {
		return ksPlan, nil, nil, err
	}

	// generate TablePlan
	tablePlan, err := doBuildTableSelectPlan(config, ctx, route.Select, ksPlan)
	if err != nil {
		return ksPlan, nil, nil, err
	}

	// merge plan
	plan = mergePlan(plan, tablePlan)

	return plan, nil, nil, nil
}

func getRoutePlan(plan logicalPlan) (route *routeGen4, err error) {
	if plan.Inputs() == nil || len(plan.Inputs()) == 0 {
		rb, isRoute := plan.(*routeGen4)
		if isRoute {
			return rb, nil
		}
	}
	if len(plan.Inputs()) > 1 {
		return nil, vterrors.VT12001(fmt.Sprintf("unsupported multi Engine"))
	}
	return getRoutePlan(plan.Inputs()[0])
}

func mergePlan(plan logicalPlan, tablePlan logicalPlan) logicalPlan {
	return plan
}

func doBuildTableSelectPlan(config plancontext.LogicTableConfig, ctx *plancontext.PlanningContext, Select sqlparser.SelectStatement, ksPlan logicalPlan) (tablePlan logicalPlan, err error) {
	tableOperator, err := operators.TablePlanQuery(ctx, Select)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToLogicalPlan(ctx, tableOperator, true)
	if err = tablePlan.WireupGen4(ctx); err != nil {
		return tablePlan, nil
	}
	return tablePlan, nil
}
