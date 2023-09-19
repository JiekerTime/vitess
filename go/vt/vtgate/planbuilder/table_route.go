package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ logicalPlan = (*tableRoute)(nil)

type tableRoute struct {
	gen4Plan

	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

	eroute *engine.TableRoute
}

func (t *tableRoute) WireupGen4(context *plancontext.PlanningContext) error {

	t.eroute.Query = t.Select
	nodeClone, _ := sqlparser.DeepCloneStatement(t.Select).(*sqlparser.Select)
	logicTable := t.eroute.TableRouteParam.LogicTable
	tableMap := vindexes.GetFirstActualTableMap(logicTable)
	sqlparser.RewirteSplitTableName(nodeClone, tableMap)
	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(nodeClone)
	parsedQuery := node.ParsedQuery()
	t.eroute.FieldQuery = parsedQuery.Query
	return nil
}

func (t *tableRoute) Primitive() engine.Primitive {
	return t.eroute
}

func (t *tableRoute) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func (t *tableRoute) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.VT13001("route: wrong number of inputs")
	}
	return nil
}

func (t *tableRoute) ContainsTables() semantics.TableSet {
	panic("implement me")
}

func (t *tableRoute) OutputColumns() []sqlparser.SelectExpr {
	panic("implement me")
}
