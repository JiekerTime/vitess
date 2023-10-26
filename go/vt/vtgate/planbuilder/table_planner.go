package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	oprewriters "vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func buildTablePlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan, tableNames []string,
) (ksAndTablePlan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	// get split table metadata
	found := findTableSchema(ctx, tableNames)
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
				return false, deleteTablePlan, nil
			case *engine.Update:
				ctx.DMLEngine = *prim.DML
				updateTablePlan, err := doBuildTablePlan(ctx, prim.AST)
				if err != nil {
					return false, nil, err
				}
				return false, updateTablePlan, nil
			}
		}

		return true, logicalPlan, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	ksAndTablePlan, err = truncateColumns(ctx, ksAndTablePlan)
	if err != nil {
		return nil, nil, nil, err
	}

	return ksAndTablePlan, semTable, nil, nil
}

func doBuildTablePlan(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (tablePlan logicalPlan, err error) {
	if oprewriters.DebugOperatorTree {
		fmt.Println(sqlparser.String(stmt))
	}
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

func findTableSchema(ctx *plancontext.PlanningContext, tableNames []string) (found bool) {
	ksName := ""
	if ks, _ := ctx.VSchema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	found = false
	for _, tableName := range tableNames {
		splitTable, err := ctx.VSchema.FindSplitTable(ksName, tableName)
		if err != nil {
			continue
		}
		ctx.SplitTableConfig[splitTable.LogicTableName] = splitTable
		found = true
	}
	return found
}

func truncateColumns(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	if ctx.OriginSelStmt == nil {
		return plan, nil
	}
	sel := sqlparser.GetFirstSelect(ctx.OriginSelStmt)
	if len(plan.OutputColumns()) == len(sel.SelectExprs) {
		return plan, nil
	}
	switch p := plan.(type) {
	case *tableRoute:
		p.eroute.SetTruncateColumnCount(len(sel.SelectExprs))
	case *orderedAggregate:
		p.truncateColumnCount = len(sel.SelectExprs)
	case *memorySort:
		p.eMemorySort.SetTruncateColumnCount(len(sel.SelectExprs))
	case *limit:
		for _, p := range plan.Inputs() {
			_, err := truncateColumns(ctx, p)
			if err != nil {
				return nil, err
			}
		}
	}
	return plan, nil
}
