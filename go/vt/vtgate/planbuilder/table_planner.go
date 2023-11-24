package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	oprewriters "vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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
		case *route:
			ctx.KsPrimitive = node.eroute

			if len(node.eroute.TableNameSlice) == 1 && getSplitTableConfig(ctx, node.eroute.TableNameSlice[0]) == nil {
				return false, logicalPlan, nil
			}

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

func setTableMiscFunc(in logicalPlan, sel *sqlparser.Select) error {
	_, err := visit(in, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := plan.(type) {
		case *tableRoute:
			err := copyTableCommentsAndLocks(node.Select, sel, node.eroute.TableRouteParam.TableOpcode)
			if err != nil {
				return false, nil, err
			}
			return true, node, nil
		}
		return true, plan, nil
	})

	if err != nil {
		return err
	}
	return nil
}

func copyTableCommentsAndLocks(statement sqlparser.SelectStatement, sel *sqlparser.Select, opcode engine.Opcode) error {
	query := sqlparser.GetFirstSelect(statement)
	query.Comments = sel.Comments
	query.Lock = sel.Lock
	if sel.Into != nil {
		if opcode != engine.Unsharded {
			return vterrors.VT12001("INTO on sharded keyspace")
		}
		query.Into = sel.Into
	}
	return nil
}

func doBuildTablePlan(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (tablePlan logicalPlan, err error) {
	if oprewriters.DebugOperatorTree {
		fmt.Println(sqlparser.String(stmt))
	}

	ksName := ""
	if ks, _ := ctx.VSchema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.TableAnalyze(stmt, ksName, ctx.VSchema)
	if err != nil {
		return nil, err
	}
	ctx.SemTable = semTable

	tableOperator, err := operators.TablePlanQuery(ctx, stmt)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToTableLogicalPlan(ctx, tableOperator, true)
	if err != nil {
		return nil, err
	}

	sel, isSel := stmt.(*sqlparser.Select)
	if isSel {
		if err = setTableMiscFunc(tablePlan, sel); err != nil {
			return nil, err
		}
	}

	if err = tablePlan.Wireup(ctx); err != nil {
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

func getSplitTableConfig(ctx *plancontext.PlanningContext, tName string) *vindexes.LogicTableConfig {
	return ctx.SplitTableConfig[tName]
}

func truncateColumns(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	if ctx.OriginSelStmt == nil {
		return plan, nil
	}
	sel := sqlparser.GetFirstSelect(ctx.OriginSelStmt)

	switch p := plan.(type) {
	case *tableRoute:
		if len(plan.OutputColumns()) == len(sel.SelectExprs) {
			return plan, nil
		}
		p.eroute.SetTruncateColumnCount(len(sel.SelectExprs))
		return plan, nil
	case *orderedAggregate:
		if len(plan.OutputColumns()) == len(sel.SelectExprs) {
			return plan, nil
		}
		p.truncateColumnCount = len(sel.SelectExprs)
		return plan, nil
	case *memorySort:
		if len(plan.OutputColumns()) == len(sel.SelectExprs) {
			return plan, nil
		}
		p.eMemorySort.SetTruncateColumnCount(len(sel.SelectExprs))
		return plan, nil
	case *limit:
		for _, p := range plan.Inputs() {
			_, err := truncateColumns(ctx, p)
			if err != nil {
				return nil, err
			}
		}
	case *simpleProjection:
		_, err := truncateColumns(ctx, p.input)
		if err != nil {
			return nil, err
		}
	case *concatenate:
		originStatement := ctx.OriginSelStmt
		defer func() {
			ctx.OriginSelStmt = originStatement
		}()

		statements := sqlparser.GetAllSelects(originStatement.(*sqlparser.Union))
		logicalPlans := plan.Inputs()
		for index, logicalPlanTemp := range logicalPlans {
			ctx.OriginSelStmt = statements[index]
			_, errLeft := truncateColumns(ctx, logicalPlanTemp)
			if errLeft != nil {
				return nil, errLeft
			}
		}
	case *uncorrelatedSubquery:
		_, err := truncateColumns(ctx, p.outer)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}
