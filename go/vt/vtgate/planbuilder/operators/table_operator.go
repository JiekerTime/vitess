package operators

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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

	_, isRoute := op.(*Route)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	return op, err
}

// createLogicalOperatorFromASTForSplitTable creates an operator tree that represents the input SELECT or UNION query
func createLogicalOperatorFromASTForSplitTable(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op ops.Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelectForSplitTable(ctx, node)
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
	return &Horizon{
		Source: op,
		Select: sel,
	}, nil
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
	routeOps, err := seedOperatorList(ctx, qg)
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

func mergeRoutesForSplitTable(ctx *plancontext.PlanningContext, qg *QueryGraph, physicalOps []ops.Operator, planCache opCacheMap, crossJoinsOK bool) (ops.Operator, error) {
	if len(physicalOps) == 0 {
		return nil, nil
	}
	return physicalOps[0], nil
}

func tryHorizonPlanningForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
	output, err = planHorizonsForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}
	return
}

// planHorizonsForSplitTable is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func planHorizonsForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	root, err := optimizeHorizonPlanningForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}

	// Adding Ordering Op - This is needed if there is no explicit ordering and aggregation is performed on top of route.
	// Adding Group by - This is needed if the grouping is performed on a join with a join condition then
	//                   aggregation happening at route needs a group by to ensure only matching rows returns
	//                   the aggregations otherwise returns no result.
	root, err = addOrderBysAndGroupBysForAggregations(ctx, root)
	if err != nil {
		return nil, err
	}

	root, err = optimizeHorizonPlanningForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func optimizeHorizonPlanningForSplitTable(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case horizonLike:
			return pushOrExpandHorizon(ctx, in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return newOp, nil
}
