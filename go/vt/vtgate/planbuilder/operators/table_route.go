package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	TableRoute struct {
		Source ops.Operator

		// Routes that have been merged into this one.
		MergedWith []*TableRoute

		Routing Routing

		Ordering []RouteOrdering

		ResultColumns int
	}

	// TableVindexPlusPredicates is a struct used to store all the predicates that the vindex can be used to query
	TableVindexPlusPredicates struct {
		TableID        semantics.TableSet
		ColTableVindex []*vindexes.TableColumn

		// during planning, we store the alternatives found for this route in this slice
		Options []*TableVindexOption
	}

	// TableVindexOption stores the information needed to know if we have all the information needed to use a vindex
	TableVindexOption struct {
		Ready      bool
		Values     []evalengine.Expr
		ValueExprs []sqlparser.Expr
		Predicates []sqlparser.Expr
		OpCode     engine.Opcode
	}
)

// Cost implements the Operator interface
func (r *TableRoute) Cost() int {
	return r.Routing.Cost()
}

// Clone implements the Operator interface
func (r *TableRoute) Clone(inputs []ops.Operator) ops.Operator {
	cloneRoute := *r
	cloneRoute.Source = inputs[0]
	cloneRoute.Routing = r.Routing.Clone()
	return &cloneRoute
}

// Inputs implements the Operator interface
func (r *TableRoute) Inputs() []ops.Operator {
	return []ops.Operator{r.Source}
}

// SetInputs implements the Operator interface
func (r *TableRoute) SetInputs(ops []ops.Operator) {
	r.Source = ops[0]
}

func (r *TableRoute) IsSingleSplitTable() bool {
	switch r.Routing.OpCode() {
	case engine.EqualUnique:
		return true
	}
	return false
}

func (r *TableRoute) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// first we see if the predicate changes how we route
	newRouting, err := UpdateRoutingLogic(ctx, expr, r.Routing)
	if err != nil {
		return nil, err
	}
	r.Routing = newRouting

	// we also need to push the predicate down into the query
	newSrc, err := r.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	r.Source = newSrc
	return r, err
}

func (r *TableRoute) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) (int, error) {
	removeKeyspaceFromSelectExpr(expr)

	if reuse {
		offset, err := r.FindCol(ctx, expr.Expr, true)
		if err != nil {
			return 0, err
		}
		if offset != -1 {
			return offset, nil
		}
	}

	// if at least one column is not already present, we check if we can easily find a projection
	// or aggregation in our source that we can add to
	op, ok, offsets := addMultipleColumnsToInput(ctx, r.Source, reuse, []bool{gb}, []*sqlparser.AliasedExpr{expr})
	r.Source = op
	if ok {
		return offsets[0], nil
	}

	// If no-one could be found, we probably don't have one yet, so we add one here
	src, err := createProjection(ctx, r.Source)
	if err != nil {
		return 0, err
	}
	r.Source = src

	offsets, _ = src.addColumnsWithoutPushing(ctx, reuse, []bool{gb}, []*sqlparser.AliasedExpr{expr})
	return offsets[0], nil
}

func (r *TableRoute) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) (int, error) {
	return r.Source.FindCol(ctx, expr, true)
}

func (r *TableRoute) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return r.Source.GetColumns(ctx)
}

func (r *TableRoute) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return r.Source.GetSelectExprs(ctx)
}

func (r *TableRoute) GetOrdering() ([]ops.OrderBy, error) {
	return r.Source.GetOrdering()
}

// TablesUsed returns tables used by MergedWith routes, which are not included
// in Inputs() and thus not a part of the operator tree
func (r *TableRoute) TablesUsed() []string {
	addString, collect := collectSortedUniqueStrings()
	for _, mw := range r.MergedWith {
		for _, u := range TablesUsed(mw) {
			addString(u)
		}
	}
	return collect()
}

func (r *TableRoute) planOffsets(ctx *plancontext.PlanningContext) (err error) {
	// if operator is returning data from a single table, we don't need to do anything more
	if r.IsSingleSplitTable() && !isCrossShard(ctx.GetRoute()) {
		return nil
	}

	// if we are getting results from multiple shards, we need to do a merge-sort
	// between them to get the final output correctly sorted
	ordering, err := r.Source.GetOrdering()
	if err != nil || len(ordering) == 0 {
		return err
	}

	for _, order := range ordering {
		if isSpecialOrderBy(order) {
			continue
		}
		offset, err := r.AddColumn(ctx, true, false, aeWrap(order.SimplifiedExpr))
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		o := RouteOrdering{
			AST:       order.Inner.Expr,
			Offset:    offset,
			WOffset:   -1,
			Direction: order.Inner.Direction,
		}
		if ctx.SemTable.NeedsWeightString(order.SimplifiedExpr) {
			ws := weightStringFor(order.SimplifiedExpr)
			offset, err := r.AddColumn(ctx, true, false, aeWrap(ws))
			if err != nil {
				return err
			}
			o.WOffset = offset
		}
		r.Ordering = append(r.Ordering, o)
	}

	return nil
}

func (r *TableRoute) ShortDescription() string {
	first := r.Routing.OpCode().String()

	ks := r.Routing.Keyspace()
	if ks != nil {
		first = fmt.Sprintf("%s on %s", r.Routing.OpCode().String(), ks.Name)
	}

	orderBy, err := r.Source.GetOrdering()
	if err != nil {
		return first
	}

	ordering := ""
	if len(orderBy) > 0 {
		var oo []string
		for _, o := range orderBy {
			oo = append(oo, sqlparser.String(o.Inner))
		}
		ordering = " order by " + strings.Join(oo, ",")
	}

	return first + ordering
}

func (r *TableRoute) setTruncateColumnCount(offset int) {
	r.ResultColumns = offset
}

// createTableRoute returns either an information_schema route, or else consults the
// VSchema to find a suitable table, and then creates a route from that.
func createTableRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	solves semantics.TableSet,
) (ops.Operator, error) {
	config := ctx.SplitTableConfig[queryTable.Table.Name.String()]
	plan := &TableRoute{
		Source: &Table{
			QTable: queryTable,
			VTable: nil,
		},
	}

	// We create the appropiate Routing struct here, depending on the type of table we are dealing with.
	routing := newTableShardedRouting(config, solves)
	for _, predicate := range queryTable.Predicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	plan.Routing = routing
	tableShardedRouting, ok := routing.(*TableShardedRouting)
	if ok {
		if tableShardedRouting.isScatter() && len(queryTable.Predicates) > 0 {
			var err error
			// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
			plan.Routing, err = tableShardedRouting.tryImprove(ctx, queryTable)
			if err != nil {
				return nil, err
			}
		}
	}
	return plan, nil
}

func (r *TableRoute) TableNamesUsed() []string {
	return nil
}
