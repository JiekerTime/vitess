package operators

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// TablePlanQuery creates a query plan for a given SQL statement
func TablePlanQuery(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (ops.Operator, error) {
	op, err := createLogicalOperatorFromASTForSplitTable(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if op, err = transformToPhysicalForSplitTable(ctx, op); err != nil {
		return nil, err
	}

	if op, err = tryHorizonPlanningForSplitTable(ctx, op); err != nil {
		return nil, err
	}

	return op, err
}

// createLogicalOperatorFromASTForSplitTable creates an operator tree that represents the input SELECT or UNION query
func createLogicalOperatorFromASTForSplitTable(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op ops.Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelectForSplitTable(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDeleteForSplitTable(ctx, node)
	default:
		err = vterrors.VT12001(fmt.Sprintf("operator: %T", selStmt))
	}
	if err != nil {
		return nil, err
	}

	return op, nil
}

// createOperatorFromSelectForSplitTable creates an operator tree that represents the input SELECT query
func createOperatorFromSelectForSplitTable(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (ops.Operator, error) {
	op, err := crossJoinForSplitTable(ctx, sel.From)
	if err != nil {
		return nil, err
	}
	if sel.Where != nil {
		exprs := sqlparser.SplitAndExpression(nil, sel.Where.Expr)
		for _, expr := range exprs {
			op, err = op.AddPredicate(ctx, expr)
			if err != nil {
				return nil, err
			}
		}
	}
	return &Horizon{
		Source: op,
		Select: sel,
	}, nil
}

func createOperatorFromDeleteForSplitTable(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) (ops.Operator, error) {
	_, qt, err := createQueryTableForDML(ctx, deleteStmt.TableExprs[0], deleteStmt.Where)
	if err != nil {
		return nil, err
	}

	tableName := deleteStmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	if err != nil {
		return nil, err
	}
	vschemaTable, _, _, _, _, err := ctx.VSchema.FindTableOrVindex(tableName)
	if err != nil {
		return nil, err
	}

	logicTableConfig := ctx.SplitTableConfig[tableName.Name.String()]
	solves := ctx.SemTable.TableSetFor(qt.Alias)
	routing := newTableShardedRouting(vschemaTable, logicTableConfig, solves)

	for _, predicate := range qt.Predicates {
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && deleteStmt.Limit != nil {
		return nil, vterrors.VT12001("multi split tables DELETE with LIMIT")
	}

	tableDelete := &Delete{
		QTable: qt,
		VTable: nil,
		AST:    deleteStmt,
	}
	tableRoute := &TableRoute{
		Source:  tableDelete,
		Routing: routing,
	}

	return tableRoute, nil
}

func crossJoinForSplitTable(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) (ops.Operator, error) {
	var output ops.Operator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExprForSplitTable(ctx, tableExpr)
		if err != nil {
			return nil, err
		}
		if output == nil {
			output = op
		} else {
			return nil, fmt.Errorf("implement me")
			// output = createJoin(ctx, output, op)
		}
	}
	return output, nil
}

func getOperatorFromTableExprForSplitTable(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr) (ops.Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return getOperatorFromAliasedTableExprForSplitTable(ctx, tableExpr)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T table type", tableExpr))
	}
}

func getOperatorFromAliasedTableExprForSplitTable(ctx *plancontext.PlanningContext, tableExpr *sqlparser.AliasedTableExpr) (ops.Operator, error) {
	tableID := ctx.SemTable.TableSetFor(tableExpr)
	switch tbl := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		qg := newQueryGraph()
		qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: false}
		qg.Tables = append(qg.Tables, qt)
		return qg, nil
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T", tbl))
	}
}

// transformToPhysicalForSplitTable takes an operator tree and rewrites any parts that have not yet been planned as physical operators.
// This is where a lot of the optimisations of the query plans are done.
// Here we try to merge query parts into the same route primitives. At the end of this process,
// all the operators in the tree are guaranteed to be PhysicalOperators
func transformToPhysicalForSplitTable(ctx *plancontext.PlanningContext, in ops.Operator) (ops.Operator, error) {
	op, err := rewrite.BottomUpAll(in, TableID, func(operator ops.Operator, ts semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch op := operator.(type) {
		case *QueryGraph:
			return optimizeQueryGraphForSplitTable(ctx, op)
		default:
			return operator, rewrite.SameTree, nil
		}
	})

	if err != nil {
		return nil, err
	}

	return op, nil
}

func optimizeQueryGraphForSplitTable(ctx *plancontext.PlanningContext, op *QueryGraph) (result ops.Operator, changed *rewrite.ApplyResult, err error) {

	switch {
	case ctx.PlannerVersion == querypb.ExecuteOptions_Gen4Left2Right:
		return nil, nil, fmt.Errorf("unsuport ExecuteOptions_Gen4Left2Right")
		// result, err = leftToRightSolve(ctx, op)
	default:
		result, err = greedySolveForSplitTable(ctx, op)
	}

	changed = rewrite.NewTree("solved query graph", result)
	return
}

func greedySolveForSplitTable(ctx *plancontext.PlanningContext, qg *QueryGraph) (ops.Operator, error) {
	routeOps, err := seedOperatorListForSplitTable(ctx, qg)
	planCache := opCacheMap{}
	if err != nil {
		return nil, err
	}

	op, err := mergeRoutesForSplitTable(ctx, qg, routeOps, planCache, false)
	if err != nil {
		return nil, err
	}
	return op, nil
}

// seedOperatorListForSplitTable returns a route for each table in the qg
func seedOperatorListForSplitTable(ctx *plancontext.PlanningContext, qg *QueryGraph) ([]ops.Operator, error) {
	plans := make([]ops.Operator, len(qg.Tables))

	// we start by seeding the table with the single routes
	for i, table := range qg.Tables {
		solves := ctx.SemTable.TableSetFor(table.Alias)
		plan, err := createTableRoute(ctx, table, solves)
		if err != nil {
			return nil, err
		}
		if qg.NoDeps != nil {
			plan, err = plan.AddPredicate(ctx, qg.NoDeps)
			if err != nil {
				return nil, err
			}
		}
		plans[i] = plan
	}
	return plans, nil
}

func mergeRoutesForSplitTable(ctx *plancontext.PlanningContext, qg *QueryGraph, physicalOps []ops.Operator, planCache opCacheMap, crossJoinsOK bool) (ops.Operator, error) {
	if len(physicalOps) == 0 {
		return nil, nil
	}
	return physicalOps[0], nil
}
