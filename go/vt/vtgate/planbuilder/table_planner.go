package planbuilder

import (
	"fmt"

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
	config := fakeLogicTable()
	name := ksPlan.Primitive().GetTableName()
	if name != config.LogicTableName {
		return ksPlan, ctx.SemTable, nil, nil
	}

	// get routePlan of ksPlan
	// The routePlan is used as input to generate the tableRoutePlan
	// Replace routePlan with tableRoutePlan
	ksAndTablePlan, err = visit(ksPlan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
		switch node := logicalPlan.(type) {
		case *routeGen4:
			tablePlan, err := doBuildTableSelectPlan(config, ctx, node.Select, ksPlan)
			if err != nil {
				return false, nil, err
			}

			// >>>
			// todo(jinyue): 后续删除
			tempPlan, _ := tempTableRoutePlan(config, node, tablePlan)
			// <<<

			return true, tempPlan, nil
		}
		return true, logicalPlan, nil
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return ksAndTablePlan, semTable, nil, nil
}

func doBuildTableSelectPlan(config tableindexes.LogicTable, ctx *plancontext.PlanningContext, Select sqlparser.SelectStatement, ksPlan logicalPlan,
) (tablePlan logicalPlan, err error) {
	tableOperator, err := operators.TablePlanQuery(ctx, Select)
	if err != nil {
		return nil, err
	}
	tablePlan, err = transformToLogicalPlan(ctx, tableOperator, true)
	if err != nil {
		return nil, err
	}

	err = tablePlan.WireupGen4(ctx)
	if err != nil {
		return tablePlan, err
	}
	return tablePlan, nil
}

func tempTableRoutePlan(config tableindexes.LogicTable, ksRoutePlan *routeGen4, tablePlan logicalPlan) (logicalPlan, error) {
	if route, ok := tablePlan.(*routeGen4); ok {
		tableRoute := &tableRoute{
			ERoute: &engine.TableRoute{
				TableName:       route.eroute.TableName,
				Query:           route.Select,
				FieldQuery:      route.eroute.FieldQuery,
				ShardRouteParam: ksRoutePlan.eroute.RoutingParameters,
				TableRouteParam: &engine.TableRoutingParameters{
					Opcode:     engine.TableScatter,
					LogicTable: config,
					Values:     route.eroute.Values,
				},
			},
		}
		return tableRoute, nil
	}
	return nil, fmt.Errorf("must routeGen4 plan. %v", tablePlan)
}

func fakeLogicTable() tableindexes.LogicTable {
	actualTableList := []tableindexes.ActualTable{
		{
			ActualTableName: "t_user" + "_1",
			Index:           1,
		},
		{
			ActualTableName: "t_user" + "_2",
			Index:           2,
		},
	}

	logicTable := tableindexes.LogicTable{
		LogicTableName:   "t_user",
		ActualTableList:  actualTableList,
		TableIndexColumn: tableindexes.Column{ColumnName: "col", ColType: query.Type_VARCHAR},
	}
	return logicTable
}
