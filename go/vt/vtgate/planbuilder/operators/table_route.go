package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type TableRoute struct {
	Source ops.Operator

	// Routes that have been merged into this one.
	MergedWith []*TableRoute

	Routing Routing

	Ordering []RouteOrdering

	ResultColumns int
}

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
	case engine.Unsharded, engine.EqualUnique:
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

func (r *TableRoute) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, _, addToGroupBy bool) (ops.Operator, int, error) {
	// check if columns is already added.
	cols, err := r.GetColumns()
	if err != nil {
		return nil, 0, err
	}
	colAsExpr := func(e *sqlparser.AliasedExpr) sqlparser.Expr {
		return e.Expr
	}
	if offset, found := canReuseColumn(ctx, cols, expr.Expr, colAsExpr); found {
		return r, offset, nil
	}

	// if column is not already present, we check if we can easily find a projection
	// or aggregation in our source that we can add to
	if ok, offset := addColumnToInput(r.Source, expr, addToGroupBy); ok {
		return r, offset, nil
	}

	// If no-one could be found, we probably don't have one yet, so we add one here
	src, err := createProjection(r.Source)
	if err != nil {
		return nil, 0, err
	}
	r.Source = src

	// And since we are under the route, we don't need to continue pushing anything further down
	offset := src.addColumnWithoutPushing(expr, false)
	if err != nil {
		return nil, 0, err
	}
	return r, offset, nil
}

func (r *TableRoute) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return r.Source.GetColumns()
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
	return fmt.Errorf("todo: TableRoute.planOffsets")
}

func (r *TableRoute) getOffsetFor(ctx *plancontext.PlanningContext, order ops.OrderBy, columns []*sqlparser.AliasedExpr) (int, error) {
	for idx, column := range columns {
		if sqlparser.Equals.Expr(order.SimplifiedExpr, column.Expr) {
			return idx, nil
		}
	}

	_, offset, err := r.AddColumn(ctx, aeWrap(order.Inner.Expr), true, false)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (r *TableRoute) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Route",
		Other: map[string]any{
			"OpCode":   r.Routing.OpCode(),
			"Keyspace": r.Routing.Keyspace(),
		},
	}
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
	return findVSchemaTableAndCreateTableRoute(ctx, queryTable, queryTable.Table, solves, true /*planAlternates*/)
}

// findVSchemaTableAndCreateTableRoute consults the VSchema to find a suitable
// table, and then creates a route from that.
func findVSchemaTableAndCreateTableRoute(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	tableName sqlparser.TableName,
	solves semantics.TableSet,
	planAlternates bool,
) (*TableRoute, error) {
	vschemaTable, _, _, _, target, err := ctx.VSchema.FindTableOrVindex(tableName)
	if target != nil {
		return nil, vterrors.VT12001("SELECT with a target destination")
	}
	if err != nil {
		return nil, err
	}

	return createTableRouteFromVSchemaTable(
		ctx,
		queryTable,
		vschemaTable,
		solves,
		planAlternates,
	)
}

// createRouteFromTable creates a route from the given VSchema table.
func createTableRouteFromVSchemaTable(
	ctx *plancontext.PlanningContext,
	queryTable *QueryTable,
	vschemaTable *vindexes.Table,
	solves semantics.TableSet,
	planAlternates bool,
) (*TableRoute, error) {
	if vschemaTable.Name.String() != queryTable.Table.Name.String() {
		// we are dealing with a routed table
		queryTable = queryTable.Clone()
		name := queryTable.Table.Name
		queryTable.Table.Name = vschemaTable.Name
		astTable, ok := queryTable.Alias.Expr.(sqlparser.TableName)
		if !ok {
			return nil, vterrors.VT13001("a derived table should never be a routed table")
		}
		realTableName := sqlparser.NewIdentifierCS(vschemaTable.Name.String())
		astTable.Name = realTableName
		if queryTable.Alias.As.IsEmpty() {
			// if the user hasn't specified an alias, we'll insert one here so the old table name still works
			queryTable.Alias.As = sqlparser.NewIdentifierCS(name.String())
		}
	}
	plan := &TableRoute{
		Source: &Table{
			QTable: queryTable,
			VTable: vschemaTable,
		},
	}

	// We create the appropiate Routing struct here, depending on the type of table we are dealing with.
	routing := createRoutingForVTable(vschemaTable, solves)
	for _, predicate := range queryTable.Predicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	plan.Routing = routing

	switch routing := routing.(type) {
	case *ShardedRouting:
		if routing.isScatter() && len(queryTable.Predicates) > 0 {
			var err error
			// If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
			plan.Routing, err = routing.tryImprove(ctx, queryTable)
			if err != nil {
				return nil, err
			}
		}
	case *AnyShardRouting:
		if planAlternates {
			alternates, err := createAlternateRoutesFromVSchemaTable(ctx, queryTable, vschemaTable, solves)
			if err != nil {
				return nil, err
			}
			routing.Alternates = alternates
		}
	}

	return plan, nil
}
