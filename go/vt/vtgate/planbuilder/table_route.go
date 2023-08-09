package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*tableRoute)(nil)

type tableRoute struct {

}

func (t tableRoute) Order() int {
	panic("implement me")
}

func (t tableRoute) ResultColumns() []*resultColumn {
	panic("implement me")
}

func (t tableRoute) Reorder(i int) {
	panic("implement me")
}

func (t tableRoute) Wireup(lp logicalPlan, jt *jointab) error {
	panic("implement me")
}

func (t tableRoute) WireupGen4(context *plancontext.PlanningContext) error {
	panic("implement me")
}

func (t tableRoute) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (t tableRoute) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (t tableRoute) SupplyWeightString(colNumber int, alsoAddToGroupBy bool) (weightcolNumber int, err error) {
	panic("implement me")
}

func (t tableRoute) Primitive() engine.Primitive {
	panic("implement me")
}

func (t tableRoute) Inputs() []logicalPlan {
	panic("implement me")
}

func (t tableRoute) Rewrite(inputs ...logicalPlan) error {
	panic("implement me")
}

func (t tableRoute) ContainsTables() semantics.TableSet {
	panic("implement me")
}

func (t tableRoute) OutputColumns() []sqlparser.SelectExpr {
	panic("implement me")
}

