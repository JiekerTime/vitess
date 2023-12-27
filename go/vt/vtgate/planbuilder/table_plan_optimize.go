package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// isSameFieldSplitKey 切分片字段和切分表字段是否相同
func isSameFieldSplitKey(ctx *plancontext.PlanningContext, tableNames ...string) bool {
	// 后期可以在这加个缓存
	if len(tableNames) != 1 {
		return false
	}
	tableName := tableNames[0]
	for _, vtable := range ctx.SemTable.Tables {
		if vtable == nil || vtable.GetVindexTable() == nil {
			continue
		}
		if tableName != vtable.GetVindexTable().Name.String() {
			continue
		}
		// 分片键数量不为1暂时不走下面流程
		if len(vtable.GetVindexTable().ColumnVindexes) != 1 {
			continue
		}
		// 分表键数量不为1暂时不走下面流程
		if len(ctx.SplitTableConfig[tableName].TableIndexColumn) != 1 {
			continue
		}
		vindex := vtable.GetVindexTable().ColumnVindexes[0]
		if len(vindex.Columns) != 1 {
			continue
		}
		singleColumn := vindex.Columns[0]
		tableIndexColumn := ctx.SplitTableConfig[tableName].TableIndexColumn[0]
		if singleColumn.Equal(tableIndexColumn.Column) {
			return true
		}
	}
	return false
}

func doBuildSelectBestTablePlan(ctx *plancontext.PlanningContext, node *route) (tablePlan logicalPlan, err error) {
	splitRoute, err := engineRouteToTableRoute(ctx, ctx.GetRoute(), node)
	if err != nil {
		return nil, err
	}

	tablePlan = &tableRoute{
		Select: node.Select,
		eroute: splitRoute,
	}

	if err = tablePlan.Wireup(ctx); err != nil {
		return nil, err
	}
	return tablePlan, nil
}

func engineRouteToTableRoute(ctx *plancontext.PlanningContext, route engine.Route, node *route) (*engine.TableRoute, error) {

	rp := &engine.TableRoutingParameters{
		TableOpcode: route.RoutingParameters.Opcode,
		LogicTable:  ctx.SplitTableConfig,
	}

	logicTableMap := map[string]*vindexes.LogicTableConfig{}
	for _, tableName := range node.eroute.TableNameSlice {
		value, ok := rp.LogicTable[tableName]
		if ok {
			logicTableMap[tableName] = value
		}
		rp.LogicTable = logicTableMap
	}

	if route.RoutingParameters.Values != nil {
		rp.TableValues = route.RoutingParameters.Values
	}

	return &engine.TableRoute{
		TableNames:      node.eroute.TableNameSlice,
		ShardRouteParam: route.RoutingParameters,
		TableRouteParam: rp,
		OrderBy:         node.eroute.OrderBy,
	}, nil
}

func mapToInsertOpCodeByEngineInsertOpCode(code engine.InsertOpcode, insertSelect bool) engine.InsertOpcode {
	if code == engine.InsertUnsharded {
		return engine.InsertTableUnsharded
	}
	if insertSelect {
		return engine.InsertTableSelect
	}
	return engine.InsertTableSharded
}

func doBuildUpdateBestTablePlan(ctx *plancontext.PlanningContext) (logicalPlan, error) {
	ast := ctx.DMLEngine.AST
	rp := &engine.TableRoutingParameters{
		TableOpcode: ctx.DMLEngine.Opcode,
		LogicTable:  ctx.SplitTableConfig,
	}
	rp.TableValues = ctx.DMLEngine.Values

	edml := &engine.TableDML{
		AST:             ast,
		KsidVindex:      ctx.DMLEngine.KsidVindex,
		KsidLength:      ctx.DMLEngine.KsidLength,
		TableNames:      ctx.DMLEngine.TableNames,
		ShardRouteParam: ctx.DMLEngine.RoutingParameters,
		TableRouteParam: rp,
	}

	e := &engine.TableUpdate{
		TableDML: edml,
	}
	tablePlan := &primitiveWrapper{prim: e}

	if err := tablePlan.Wireup(ctx); err != nil {
		return nil, err
	}
	return tablePlan, nil
}

func doBuildDeleteBestTablePlan(ctx *plancontext.PlanningContext) (logicalPlan, error) {
	ast := ctx.DMLEngine.AST
	rp := &engine.TableRoutingParameters{
		TableOpcode: ctx.DMLEngine.Opcode,
		LogicTable:  ctx.SplitTableConfig,
	}
	rp.TableValues = ctx.DMLEngine.Values

	edml := &engine.TableDML{
		AST:             ast,
		KsidVindex:      ctx.DMLEngine.KsidVindex,
		KsidLength:      ctx.DMLEngine.KsidLength,
		TableNames:      ctx.DMLEngine.TableNames,
		ShardRouteParam: ctx.DMLEngine.RoutingParameters,
		TableRouteParam: rp,
	}

	e := &engine.TableDelete{
		TableDML: edml,
	}
	tablePlan := &primitiveWrapper{prim: e}

	if err := tablePlan.Wireup(ctx); err != nil {
		return nil, err
	}
	return tablePlan, nil
}

func doBuildInsertBestTablePlan(ctx *plancontext.PlanningContext, tableName string) (i *insert, err error) {

	eins := ctx.GetInsert()
	eins.Opcode = mapToInsertOpCodeByEngineInsertOpCode(eins.Opcode, eins.Input != nil)
	eins.TableColVindexes = ctx.SplitTableConfig[tableName]

	switch rows := eins.AST.Rows.(type) {
	case sqlparser.Values:
		routeValues, _ := insertRowsPlanForEngineInsert(eins, rows)
		eins.TableVindexValues = routeValues
		i = &insert{eInsert: &eins}

	case sqlparser.SelectStatement:
		return nil, vterrors.VT12001("Unsupport split table insert into select")
	}

	eins.Prefix, eins.Columns, eins.Mid = GenerateInsertShardedQueryForSplitTable(eins.AST)

	return
}

func insertRowsPlanForEngineInsert(insert engine.Insert, rows sqlparser.Values) ([][]evalengine.Expr, error) {
	colTableVindexes := insert.TableColVindexes.TableIndexColumn

	routeValues := make([][]evalengine.Expr, len(colTableVindexes))
	if len(insert.VindexValues) == 1 {
		routeValues = insert.VindexValues[0]
		return routeValues, nil
	}
	return routeValues, nil
}
