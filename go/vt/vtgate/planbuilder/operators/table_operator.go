package operators

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
	case *sqlparser.Insert:
		op, err = createOperatorFromInsertForSplitTable(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDeleteForSplitTable(ctx, node)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdateForSplitTable(ctx, node)
	default:
		err = vterrors.VT12001(fmt.Sprintf("statement type %T in split table", selStmt))
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

	if err := checkForSupportSql(ctx, sel); err != nil {
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
		Query:  sel,
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

	logicTableConfig := ctx.SplitTableConfig[tableName.Name.String()]
	solves := ctx.SemTable.TableSetFor(qt.Alias)
	routing := newTableShardedRouting(logicTableConfig, solves)

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

func createOperatorFromUpdateForSplitTable(ctx *plancontext.PlanningContext, updateStmt *sqlparser.Update) (ops.Operator, error) {
	_, qt, err := createQueryTableForDML(ctx, updateStmt.TableExprs[0], updateStmt.Where)
	if err != nil {
		return nil, err
	}

	tableName := updateStmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	if err != nil {
		return nil, err
	}

	logicTableConfig := ctx.SplitTableConfig[tableName.Name.String()]

	for _, col := range logicTableConfig.TableIndexColumn {
		if err := checkAndErrIfTableVindexChanging(updateStmt.Exprs, col.Column); err != nil {
			return nil, err
		}
	}

	solves := ctx.SemTable.TableSetFor(qt.Alias)
	routing := newTableShardedRouting(logicTableConfig, solves)

	for _, predicate := range qt.Predicates {
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && updateStmt.Limit != nil {
		return nil, vterrors.VT12001("multi split tables UPDATE with LIMIT")
	}

	tableRoute := &TableRoute{
		Source: &Update{
			QTable: qt,
			VTable: nil,
			AST:    updateStmt,
		},
		Routing: routing,
	}

	return tableRoute, nil
}

func createOperatorFromInsertForSplitTable(ctx *plancontext.PlanningContext, ins *sqlparser.Insert) (ops.Operator, error) {

	//1、判断columns 有没有分表建没有报错，vitess分片是不报错的
	splitTableConfig := ctx.SplitTableConfig[ins.Table.Expr.(sqlparser.TableName).Name.String()]
	colTableVindex := splitTableConfig.TableIndexColumn
	for _, tableIndexColumn := range colTableVindex {
		if findColumn(ins, tableIndexColumn.Column) == -1 {
			return nil, vterrors.VT12001("INSERT without splittable column")
		}
	}
	insOp := &TableInsert{
		TableColVindexes: splitTableConfig,
	}
	route := &TableRoute{
		Source:  insOp,
		Routing: &ShardedRouting{RouteOpCode: mapToSelectOpCode(ctx.GetInsert().Opcode)},
	}
	var err error
	switch rows := ins.Rows.(type) {
	case sqlparser.Values:
		route.Source, err = insertRowsPlanForSplitTable(insOp, ins, rows)
		if err != nil {
			return nil, err
		}
	case sqlparser.SelectStatement:
		/*	route.Source, err = insertSelectPlan(ctx, insOp, ins, rows)
			if err != nil {
				return nil, err
			}*/
		return nil, vterrors.VT12001("Unsupport split table insert into select")
	}
	//2、

	return route, nil
}

func mapToSelectOpCode(code engine.InsertOpcode) engine.Opcode {
	if code == engine.InsertUnsharded {
		return engine.Unsharded
	}
	return engine.Scatter
}

func insertRowsPlanForSplitTable(insOp *TableInsert, ins *sqlparser.Insert, rows sqlparser.Values) (*TableInsert, error) {
	colTableVindexes := insOp.TableColVindexes.TableIndexColumn
	routeValues := make([][]evalengine.Expr, len(colTableVindexes))
	for colIdx, col := range colTableVindexes {
		err := checkAndErrIfTableVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col.Column)
		if err != nil {
			return nil, err
		}
		routeValues[colIdx] = make([]evalengine.Expr, len(rows))
		colNum := findColumn(ins, col.Column)
		for rowNum, row := range rows {
			innerpv, err := evalengine.Translate(row[colNum], nil)
			if err != nil {
				return nil, err
			}
			routeValues[colIdx][rowNum] = innerpv
		}
	}

	// here we are replacing the row value with the argument.
	for _, col := range colTableVindexes {
		colNum, _ := findOrAddColumn(ins, col.Column)
		for rowNum, row := range rows {
			name := engine.InsertVarName(col.Column, rowNum)
			row[colNum] = sqlparser.NewArgument(name)
		}
	}

	insOp.TableVindexValues = routeValues
	return insOp, nil
}

func checkAndErrIfTableVindexChanging(setClauses sqlparser.UpdateExprs, col sqlparser.IdentifierCI) error {
	for _, assignment := range setClauses {
		if col.Equal(assignment.Name.Name) {
			valueExpr, isValuesFuncExpr := assignment.Expr.(*sqlparser.ValuesFuncExpr)
			// update on duplicate key is changing the vindex column, not supported.
			if !isValuesFuncExpr || !valueExpr.Name.Name.Equal(assignment.Name.Name) {
				return vterrors.VT12001("DML cannot update tablevindex column")
			}
			return nil
		}
	}
	return nil
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
			return nil, vterrors.VT12001("multiple tables in split table")
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
		return nil, vterrors.VT12001(fmt.Sprintf("unable to use: %T table type in split table", tableExpr))
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
		return nil, vterrors.VT12001(fmt.Sprintf("unable to use: %T in split table", tbl))
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

	changed = rewrite.NewTree("solved query graph for split table", result)
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

func hasSubqueryInExprsAndWhere(sel *sqlparser.Select) bool {
	hasSubquery := false
	var sqlParts []sqlparser.SQLNode
	sqlParts = append(sqlParts, sel.Where, sel.SelectExprs)

	for _, sqlNode := range sqlParts {
		sqlparser.Rewrite(sqlNode, func(cursor *sqlparser.Cursor) bool {
			switch cursor.Node().(type) {
			case *sqlparser.Subquery:
				hasSubquery = true
				return false
			}
			return true
		}, func(cursor *sqlparser.Cursor) bool {
			return !hasSubquery
		})
	}

	return hasSubquery
}
