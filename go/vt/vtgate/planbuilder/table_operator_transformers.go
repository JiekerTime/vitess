package planbuilder

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
		return useSimpleProjection(op, cols, src)
	}

	expressions := slices2.Map(op.Projections, func(from operators.ProjExpr) sqlparser.Expr {
		return from.GetExpr()
	})

	failed := false
	evalengineExprs := slices2.Map(op.Projections, func(from operators.ProjExpr) evalengine.Expr {
		switch e := from.(type) {
		case operators.Eval:
			return e.EExpr
		case operators.Offset:
			t := ctx.SemTable.ExprTypes[e.Expr]
			return &evalengine.Column{
				Offset:    e.Offset,
				Type:      t.Type,
				Collation: collations.TypedCollation{},
			}
		default:
			failed = true
			return nil
		}
	})
	var primitive *engine.Projection
	columnNames := slices2.Map(op.Columns, func(from *sqlparser.AliasedExpr) string {
		return from.ColumnName()
	})

	if !failed {
		primitive = &engine.Projection{
			Cols:  columnNames,
			Exprs: evalengineExprs,
		}
	}

	return &projection{
		source:      src,
		columnNames: columnNames,
		columns:     expressions,
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

	sel, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	ksERoute := ctx.GetRoute()
	eroute, err := routeToEngineTableRoute(ctx, ksERoute.RoutingParameters, op)
	if err != nil {
		return nil, err
	}

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

	return &tableRoute{
		Select: sel,
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
		Table:           ctx.DMLEngine.Table,
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
		Table:           ctx.DMLEngine.Table,
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
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
		},
	}

	for _, aggr := range op.Aggregations {
		if aggr.OpCode == opcode.AggregateUnassigned {
			return nil, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
		}
		typ, col := aggr.GetTypeCollation(ctx)
		oa.aggregates = append(oa.aggregates, &engine.AggregateParams{
			Opcode:      aggr.OpCode,
			Col:         aggr.ColOffset,
			Alias:       aggr.Alias,
			Expr:        aggr.Func,
			Original:    aggr.Original,
			OrigOpcode:  aggr.OriginalOpCode,
			WCol:        aggr.WSOffset,
			Type:        typ,
			CollationID: col,
		})
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

	oa.truncateColumnCount = op.ResultColumns
	return oa, nil
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
		eins.Prefix, eins.Mid, eins.Suffix = generateInsertShardedQuery(eins.AST)
	}
	if ins.Input == nil {
		eins.Query = generateQuery(eins.AST)
	} else {
		i.source, err = transformToLogicalPlan(ctx, ins.Input, true)
		if err != nil {
			return
		}
	}
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
