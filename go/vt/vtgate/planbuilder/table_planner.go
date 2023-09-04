package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

func buildTableSelectPlan(ctx *plancontext.PlanningContext, ksPlan logicalPlan,
) (ksAndTablePlan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	// get split table metadata
	config, found := getLogicTableConfig(ksPlan.Primitive().GetTableName())
	if !found {
		return ksPlan, ctx.SemTable, nil, nil
	}

	ctx.SplitTableConfig[config.LogicTableName] = config

	// The routePlan is used as input to generate the tablePlan
	// Replace routePlan with tablePlan
	ksAndTablePlan, err = visit(ksPlan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
		switch node := logicalPlan.(type) {
		case *routeGen4:
			tablePlan, err := doBuildTableSelectPlan(ctx, node.Select, node.eroute.RoutingParameters)
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

func doBuildTableSelectPlan(ctx *plancontext.PlanningContext, Select sqlparser.SelectStatement, shardRouteParam *engine.RoutingParameters,
) (tablePlan logicalPlan, err error) {
	tableOperator, err := operators.TablePlanQuery(ctx, Select)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToTableLogicalPlan(ctx, tableOperator, true, shardRouteParam)
	if err != nil {
		return nil, err
	}

	err = tablePlan.WireupGen4(ctx)
	if err != nil {
		return tablePlan, err
	}
	return tablePlan, nil
}

func getLogicTableConfig(tableName string) (logical tableindexes.LogicTableConfig, found bool) {
	tableMap := fakeLogicTableMap()
	if logical, ok := tableMap[tableName]; ok {
		return logical, true
	}
	return logical, false
}

func fakeLogicTableMap() (logicTableMap tableindexes.SplitTableMap) {
	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "t_user",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "t_user" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "t_user" + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: &tableindexes.Column{ColumnName: "col", ColType: query.Type_VARCHAR},
	}

	logicTable2 := tableindexes.LogicTableConfig{
		LogicTableName: "table_engine_test",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "table_engine_test" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "table_engine_test" + "_2",
				Index:           2,
			},
			{
				ActualTableName: "table_engine_test" + "_3",
				Index:           3,
			},
			{
				ActualTableName: "table_engine_test" + "_4",
				Index:           4,
			},
		},
		TableIndexColumn: &tableindexes.Column{ColumnName: "f_int", ColType: query.Type_VARCHAR},
	}

	logicTable3 := tableindexes.LogicTableConfig{
		LogicTableName: "t_authoritative",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "t_authoritative" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "t_authoritative" + "_2",
				Index:           2,
			},
			{
				ActualTableName: "t_authoritative" + "_3",
				Index:           3,
			},
			{
				ActualTableName: "t_authoritative" + "_4",
				Index:           4,
			},
		},
		TableIndexColumn: &tableindexes.Column{ColumnName: "col1", ColType: query.Type_VARCHAR},
	}

	logicTableMap = make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable
	logicTableMap[logicTable2.LogicTableName] = logicTable2
	logicTableMap[logicTable3.LogicTableName] = logicTable3
	return logicTableMap
}
