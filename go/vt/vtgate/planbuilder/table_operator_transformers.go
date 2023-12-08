package planbuilder

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func transformToTableLogicalPlan(ctx *plancontext.PlanningContext, op ops.Operator, isRoot bool) (logicalPlan, error) {
	switch op := op.(type) {
	case *operators.TableRoute:
		return transformTableRoutePlan(ctx, op)
	case *operators.Ordering:
		return transformOrderingForSplitTable(ctx, op)
	case *operators.Projection:
		return transformProjectionForSplitTable(ctx, op)
	case *operators.Limit:
		return transformLimitForSplitTable(ctx, op)
	case *operators.Aggregator:
		return transformAggregatorForSplitTable(ctx, op)
	case *operators.Filter:
		return transformFilterForSplitTable(ctx, op)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

func transformLimitForSplitTable(ctx *plancontext.PlanningContext, op *operators.Limit) (logicalPlan, error) {
	plan, err := transformToTableLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	return createLimit(plan, op.AST)
}

func transformOrderingForSplitTable(ctx *plancontext.PlanningContext, op *operators.Ordering) (logicalPlan, error) {
	plan, err := transformToTableLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	return createMemorySort(ctx, plan, op)
}

func transformProjectionForSplitTable(ctx *plancontext.PlanningContext, op *operators.Projection) (logicalPlan, error) {
	src, err := transformToTableLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	if cols := op.AllOffsets(); cols != nil {
		// if all this op is doing is passing through columns from the input, we
		// can use the faster SimpleProjection
		return useSimpleProjection(ctx, op, cols, src)
	}

	ap, err := op.GetAliasedProjections()
	if err != nil {
		return nil, err
	}

	var exprs []sqlparser.Expr
	var evalengineExprs []evalengine.Expr
	var columnNames []string
	for _, pe := range ap {
		ee, err := getEvalEngingeExpr(ctx, pe)
		if err != nil {
			return nil, err
		}
		evalengineExprs = append(evalengineExprs, ee)
		exprs = append(exprs, pe.EvalExpr)
		columnNames = append(columnNames, pe.Original.ColumnName())
	}

	primitive := &engine.Projection{
		Cols:  columnNames,
		Exprs: evalengineExprs,
	}

	return &projection{
		source:      src,
		columnNames: columnNames,
		columns:     exprs,
		primitive:   primitive,
	}, nil
}

func transformTableRoutePlan(ctx *plancontext.PlanningContext, op *operators.TableRoute) (logicalPlan, error) {
	switch src := op.Source.(type) {
	case *operators.Delete:
		return transformTableDeletePlan(ctx, op, src)
	case *operators.Update:
		return transformTableUpdatePlan(ctx, op, src)
	case *operators.TableInsert:
		return transformInsertPlanForSplitTable(ctx, op, src)
	}

	sel, _, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	selStmt, ok := sel.(sqlparser.SelectStatement)
	if !ok {
		return nil, vterrors.VT13001(fmt.Sprintf("dont know how to %T", selStmt))
	}

	if op.Lock != sqlparser.NoLock {
		selStmt.SetLock(op.Lock)
	}

	ksERoute := ctx.GetRoute()
	eroute, err := routeToEngineTableRoute(ctx, ksERoute.RoutingParameters, op)
	if err != nil {
		return nil, err
	}

	if op.Ordering != nil {
		for _, order := range op.Ordering {
			typ, collation, _ := ctx.SemTable.TypeForExpr(order.AST)
			eroute.OrderBy = append(eroute.OrderBy, engine.OrderByParams{
				Col:             order.Offset,
				WeightStringCol: order.WOffset,
				Desc:            order.Direction == sqlparser.DescOrder,
				Type:            typ,
				CollationID:     collation,
			})
		}
	} else {
		eroute.OrderBy = ksERoute.OrderBy
	}

	return &tableRoute{
		Select: selStmt,
		eroute: eroute,
	}, nil
}

func transformTableDeletePlan(ctx *plancontext.PlanningContext, op *operators.TableRoute, del *operators.Delete) (logicalPlan, error) {
	ast := del.AST
	rp := newTableRoutingParams(ctx, op.Routing.OpCode())
	if err := op.Routing.UpdateTableRoutingParams(ctx, rp); err != nil {
		return nil, err
	}

	edml := &engine.TableDML{
		AST:             ast,
		KsidVindex:      ctx.DMLEngine.KsidVindex,
		KsidLength:      ctx.DMLEngine.KsidLength,
		TableNames:      []string{del.QTable.Table.Name.String()},
		Vindexes:        ctx.DMLEngine.Vindexes,
		ShardRouteParam: ctx.DMLEngine.RoutingParameters,
		TableRouteParam: rp,
	}

	e := &engine.TableDelete{
		TableDML: edml,
	}
	return &primitiveWrapper{prim: e}, nil
}

func transformTableUpdatePlan(ctx *plancontext.PlanningContext, op *operators.TableRoute, updateOperator *operators.Update) (logicalPlan, error) {
	ast := updateOperator.AST
	rp := newTableRoutingParams(ctx, op.Routing.OpCode())
	if err := op.Routing.UpdateTableRoutingParams(ctx, rp); err != nil {
		return nil, err
	}

	edml := &engine.TableDML{
		AST:             ast,
		KsidVindex:      ctx.DMLEngine.KsidVindex,
		KsidLength:      ctx.DMLEngine.KsidLength,
		TableNames:      []string{updateOperator.QTable.Table.Name.String()},
		Vindexes:        ctx.DMLEngine.Vindexes,
		ShardRouteParam: ctx.DMLEngine.RoutingParameters,
		TableRouteParam: rp,
	}

	e := &engine.TableUpdate{
		TableDML: edml,
	}
	return &primitiveWrapper{prim: e}, nil
}

func transformAggregatorForSplitTable(ctx *plancontext.PlanningContext, op *operators.Aggregator) (logicalPlan, error) {
	plan, err := transformToTableLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}

	oa := &orderedAggregate{
		resultsBuilder: newResultsBuilder(plan, nil),
	}

	for _, aggr := range op.Aggregations {
		if aggr.OpCode == opcode.AggregateUnassigned {
			return nil, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
		}
		aggrParam := engine.NewAggregateParam(aggr.OpCode, aggr.ColOffset, aggr.Alias)
		aggrParam.Expr = aggr.Func
		aggrParam.Original = aggr.Original
		aggrParam.OrigOpcode = aggr.OriginalOpCode
		aggrParam.WCol = aggr.WSOffset
		aggrParam.Type, aggrParam.CollationID = aggr.GetTypeCollation(ctx)
		oa.aggregates = append(oa.aggregates, aggrParam)
	}
	for _, groupBy := range op.Grouping {
		typ, col, _ := ctx.SemTable.TypeForExpr(groupBy.SimplifiedExpr)
		oa.groupByKeys = append(oa.groupByKeys, &engine.GroupByParams{
			KeyCol:          groupBy.ColOffset,
			WeightStringCol: groupBy.WSOffset,
			Expr:            groupBy.AsAliasedExpr().Expr,
			Type:            typ,
			CollationID:     col,
		})
	}

	if err != nil {
		return nil, err
	}
	oa.truncateColumnCount = op.ResultColumns
	return oa, nil
}

func transformFilterForSplitTable(ctx *plancontext.PlanningContext, op *operators.Filter) (logicalPlan, error) {
	plan, err := transformToTableLogicalPlan(ctx, op.Source, false)
	if err != nil {
		return nil, err
	}
	predicate := op.PredicateWithOffsets
	ast := ctx.SemTable.AndExpressions(op.Predicates...)
	if predicate == nil {
		return nil, fmt.Errorf("this should have already been done")
	}
	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate:    predicate,
			ASTPredicate: ast,
			Truncate:     op.Truncate,
		},
	}, nil
}

func routeToEngineTableRoute(ctx *plancontext.PlanningContext, shardRouteParam *engine.RoutingParameters, op *operators.TableRoute) (*engine.TableRoute, error) {
	tableNames, err := getAllTableNamesForSplitTable(op)
	if err != nil {
		return nil, err
	}

	rp := newTableRoutingParams(ctx, op.Routing.OpCode())
	logicTableMap := map[string]*vindexes.LogicTableConfig{}
	for _, tableName := range tableNames {
		value, exists := rp.LogicTable[tableName]
		if !exists {
			continue
		}
		logicTableMap[tableName] = value
	}
	rp.LogicTable = logicTableMap

	tableRouting, ok := op.Routing.(*operators.TableShardedRouting)
	if ok {
		err = tableRouting.UpdateTableRoutingParams(ctx, rp)
		if err != nil {
			return nil, err
		}
	}

	return &engine.TableRoute{
		TableName:       tableNames[0],
		ShardRouteParam: shardRouteParam,
		TableRouteParam: rp,
	}, nil

}

func newTableRoutingParams(ctx *plancontext.PlanningContext, opCode engine.Opcode) *engine.TableRoutingParameters {
	return &engine.TableRoutingParameters{
		TableOpcode: opCode,
		LogicTable:  ctx.SplitTableConfig,
	}
}

func getAllTableNamesForSplitTable(op *operators.TableRoute) ([]string, error) {
	tableNameMap := map[string]any{}
	err := rewrite.Visit(op, func(op ops.Operator) error {
		tbl, isTbl := op.(*operators.Table)
		var name string
		if isTbl {
			name = sqlparser.String(tbl.QTable.Table.Name)
			tableNameMap[name] = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames, nil
}

func transformInsertPlanForSplitTable(ctx *plancontext.PlanningContext, op *operators.TableRoute, ins *operators.TableInsert) (i *insert, err error) {
	eins := ctx.GetInsert()
	eins.Opcode = mapToInsertOpCodeForSplitTable(op.Routing.OpCode(), ins.Input != nil)
	eins.TableColVindexes = ins.TableColVindexes
	eins.TableVindexValues = ins.TableVindexValues
	eins.TableVindexValueOffset = ins.TableVindexValueOffset
	i = &insert{eInsert: &eins}
	if eins.Opcode != engine.InsertUnsharded || ins.Input != nil {
		eins.Prefix, eins.Columns, eins.Mid = GenerateInsertShardedQueryForSplitTable(eins.AST)
	}
	if ins.Input == nil {
		eins.Query = generateQuery(eins.AST)
	} else {
		return nil, vterrors.VT12001("Unsupport split table insert into select")
	}
	return
}

func GenerateInsertShardedQueryForSplitTable(ins *sqlparser.Insert) (prefix string, columns string, mids sqlparser.Values) {
	mids, isValues := ins.Rows.(sqlparser.Values)
	prefixFormat := "insert %v%sinto "

	prefixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	prefixBuf.Myprintf(prefixFormat,
		ins.Comments, ins.Ignore.ToString(),
	)
	prefix = prefixBuf.String()
	columns = "%v "
	if isValues {
		// the mid values are filled differently
		// with select uses sqlparser.String for sqlparser.Values
		// with rows uses string.
		columns += "values "
	}
	columnsBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	columnsBuf.Myprintf(columns, ins.Columns)
	columns = columnsBuf.String()

	return
}

func mapToInsertOpCodeForSplitTable(code engine.Opcode, insertSelect bool) engine.InsertOpcode {
	if code == engine.Unsharded {
		return engine.InsertTableUnsharded
	}
	if insertSelect {
		return engine.InsertTableSelect
	}
	return engine.InsertTableSharded
}
