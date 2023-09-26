package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func buildTablePlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan,
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
			ctx.KsPrimitive = node.eroute
			tablePlan, err := doBuildTablePlan(ctx, node.Select)
			if err != nil {
				return false, nil, err
			}
			return false, tablePlan, nil
		case *insert:
			ctx.KsPrimitive = node.eInsert
			tablePlan, err := doBuildTablePlan(ctx, node.eInsert.AST)
			if err != nil {
				return false, nil, err
			}
			return false, tablePlan, nil
		case *primitiveWrapper:
			switch prim := node.Primitive().(type) {
			case *engine.Delete:
				ctx.DMLEngine = *prim.DML
				deleteTablePlan, err := doBuildTablePlan(ctx, prim.AST)
				if err != nil {
					return false, nil, err
				}
				return true, deleteTablePlan, nil
			}
		}

		return true, logicalPlan, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return ksAndTablePlan, semTable, nil, nil
}

func doBuildTablePlan(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (tablePlan logicalPlan, err error) {
	tableOperator, err := operators.TablePlanQuery(ctx, stmt)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToTableLogicalPlan(ctx, tableOperator, true)
	if err != nil {
		return nil, err
	}

	if err = tablePlan.WireupGen4(ctx); err != nil {
		return nil, err
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
