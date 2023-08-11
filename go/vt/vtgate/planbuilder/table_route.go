package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*tableRoute)(nil)

type tableRoute struct {
	gen4Plan

	Select sqlparser.SelectStatement

	eroute *engine.TableRoute
}

func (t tableRoute) WireupGen4(context *plancontext.PlanningContext) error {
	panic("implement me")
}

func (t tableRoute) Primitive() engine.Primitive {
	panic("implement me")
}

func (t tableRoute) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func (t tableRoute) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.VT13001("route: wrong number of inputs")
	}
	return nil
}

func (t tableRoute) ContainsTables() semantics.TableSet {
	panic("implement me")
}

func (t tableRoute) OutputColumns() []sqlparser.SelectExpr {
	panic("implement me")
}
