package planbuilder

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func transformToTableLogicalPlan(ctx *plancontext.PlanningContext, op ops.Operator, isRoot bool, shardRouteParam *engine.RoutingParameters) (logicalPlan, error) {
	switch op := op.(type) {
	case *operators.TableRoute:
		return transformTableRoutePlan(ctx, shardRouteParam, op)
	}

	return nil, vterrors.VT13001(fmt.Sprintf("unknown type encountered: %T (transformToLogicalPlan)", op))
}

func transformTableRoutePlan(ctx *plancontext.PlanningContext, shardRouteParam *engine.RoutingParameters, op *operators.TableRoute) (logicalPlan, error) {
	sel, err := operators.ToSQL(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	eroute, err := routeToEngineTableRoute(ctx, shardRouteParam, op)
	if err != nil {
		return nil, err
	}

	return &tableRoute{
		Select: sel,
		eroute: eroute,
	}, nil
}

func routeToEngineTableRoute(ctx *plancontext.PlanningContext, shardRouteParam *engine.RoutingParameters, op *operators.TableRoute) (*engine.TableRoute, error) {
	tableNames, err := getAllTableNamesForSplitTable(op)
	if err != nil {
		return nil, err
	}

	rp := newTableRoutingParams(ctx, op.Routing.OpCode())
	err = op.Routing.UpdateTableRoutingParams(ctx, rp)
	if err != nil {
		return nil, err
	}

	return &engine.TableRoute{
		TableName:       tableNames[0],
		ShardRouteParam: shardRouteParam,
		TableRouteParam: rp,
	}, nil

}

func newTableRoutingParams(ctx *plancontext.PlanningContext, opCode engine.Opcode) *engine.TableRoutingParameters {
	return &engine.TableRoutingParameters{
		Opcode:     opCode,
		LogicTable: ctx.SplitTableConfig,
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
