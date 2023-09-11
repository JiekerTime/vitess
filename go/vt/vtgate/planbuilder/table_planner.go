package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func buildTableSelectPlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan,
) (ksAndTablePlan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	// get split table metadata
	found := findLogicTableConfig(ctx, ksPlan.Primitive().GetTableName())
	if !found {
		return ksPlan, ctx.SemTable, nil, nil
	}

	// The routePlan is used as input to generate the tablePlan
	// Replace routePlan with tablePlan
	ksAndTablePlan, err = visit(ksPlan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
		switch node := logicalPlan.(type) {
		case *routeGen4:
			tablePlan, err := doBuildTableSelectPlan(ctx, node.Select, node.eroute)
			if err != nil {
				return false, nil, err
			}
			return true, tablePlan, nil
		}

		return true, logicalPlan, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return ksAndTablePlan, semTable, nil, nil
}

func doBuildTableSelectPlan(ctx *plancontext.PlanningContext, Select sqlparser.SelectStatement, ksERoute *engine.Route,
) (tablePlan logicalPlan, err error) {
	tableOperator, err := operators.TablePlanQuery(ctx, Select)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToTableLogicalPlan(ctx, tableOperator, true, ksERoute)
	if err != nil {
		return nil, err
	}

	err = tablePlan.WireupGen4(ctx)
	if err != nil {
		return tablePlan, err
	}
	return tablePlan, nil
}

func findLogicTableConfig(ctx *plancontext.PlanningContext, tableName string) (found bool) {
	ksName := ""
	if ks, _ := ctx.VSchema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	splitTable, err := ctx.VSchema.FindSplitTable(ksName, tableName)
	if err != nil {
		return false
	}

	ctx.SplitTableConfig[splitTable.LogicTableName] = splitTable
	return true
}
