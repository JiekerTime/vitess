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

	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

	eroute *engine.TableRoute
}

func (t *tableRoute) Wireup(_ *plancontext.PlanningContext) error {
	t.prepareTheAST()
	t.eroute.Query = t.Select

	err := t.eroute.TableRouteParam.LoadRewriteCache(t.Select, "")
	if err != nil {
		return err
	}

	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(t.eroute.TableRouteParam.CachedNode)
	parsedQuery := node.ParsedQuery()
	// Get one query for field query.
	for logTb, tbConfig := range t.eroute.TableRouteParam.LogicTable {
		firstActualTable := vindexes.GetFirstActualTable(tbConfig)
		if token, ok := t.eroute.TableRouteParam.LogicalNameTokens[logTb]; ok {
			parsedQuery.Query = sqlparser.ReplaceToken(parsedQuery.Query, token, firstActualTable)
		}
	}
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
	return sqlparser.GetFirstSelect(t.Select).SelectExprs
}

// prepareTheAST does minor fixups of the SELECT struct before producing the query string
func (rb *tableRoute) prepareTheAST() {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = []sqlparser.SelectExpr{
					&sqlparser.AliasedExpr{
						Expr: sqlparser.NewIntLiteral("1"),
					},
				}
			}
		case *sqlparser.ComparisonExpr:
			// 42 = colName -> colName = 42
			b := node.Operator == sqlparser.EqualOp
			value := sqlparser.IsValue(node.Left)
			name := sqlparser.IsColName(node.Right)
			if b &&
				value &&
				name {
				node.Left, node.Right = node.Right, node.Left
			}
		}
		return true, nil
	}, rb.Select)
}
