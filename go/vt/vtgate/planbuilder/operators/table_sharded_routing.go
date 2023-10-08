package operators

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TableShardedRouting is what we use for all tables that exist in a sharded keyspace
// It knows about available vindexes and can use them for routing when applicable
type TableShardedRouting struct {
	// here we store the possible vindexes we can use so that when we add predicates to the plan,
	// we can quickly check if the new predicates enables any new vindex Options
	TindexPreds []*TableVindexPlusPredicates

	// the best option available is stored here
	Selected *TableVindexOption

	RouteOpCode engine.Opcode

	// SeenPredicates contains all the predicates that have had a chance to influence routing.
	// If we need to replan routing, we'll use this list
	SeenPredicates []sqlparser.Expr
}

var _ Routing = (*TableShardedRouting)(nil)

func newTableShardedRouting(logicTableConfig *vindexes.LogicTableConfig, id semantics.TableSet) Routing {
	routing := &TableShardedRouting{
		RouteOpCode: engine.Scatter,
	}
	routing.TindexPreds = append(routing.TindexPreds, &TableVindexPlusPredicates{ColTableVindex: logicTableConfig.TableIndexColumn, TableID: id})
	return routing
}

func (tableRouting *TableShardedRouting) UpdateRoutingParams(_ *plancontext.PlanningContext, _ *engine.RoutingParameters) error {
	return nil
}

func (tableRouting *TableShardedRouting) Clone() Routing {
	var selected *TableVindexOption
	if tableRouting.Selected != nil {
		t := *tableRouting.Selected
		selected = &t
	}
	return &TableShardedRouting{
		TindexPreds: slices2.Map(tableRouting.TindexPreds, func(from *TableVindexPlusPredicates) *TableVindexPlusPredicates {
			// we do this to create a copy of the struct
			p := *from
			return &p
		}),
		Selected:       selected,
		RouteOpCode:    tableRouting.RouteOpCode,
		SeenPredicates: slices.Clone(tableRouting.SeenPredicates),
	}
}

func (tableRouting *TableShardedRouting) updateRoutingLogic(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Routing, error) {
	tableRouting.SeenPredicates = append(tableRouting.SeenPredicates, expr)

	newRouting, newTableVindexFound, err := tableRouting.searchForNewTableVindexes(ctx, expr)
	if err != nil {
		return nil, err
	}

	if newRouting != nil {
		// we found something that we can route with something other than ShardedRouting
		return newRouting, nil
	}

	// if we didn't open up any new vindex Options, no need to enter here
	if newTableVindexFound {
		tableRouting.PickBestAvailableTableVindex()
	}

	return tableRouting, nil
}

func (tableRouting *TableShardedRouting) Cost() int {
	return 0
}

func (tableRouting *TableShardedRouting) OpCode() engine.Opcode {
	return tableRouting.RouteOpCode
}

func (tableRouting *TableShardedRouting) Keyspace() *vindexes.Keyspace {
	return nil
}

func (tableRouting *TableShardedRouting) searchForNewTableVindexes(ctx *plancontext.PlanningContext, predicate sqlparser.Expr) (Routing, bool, error) {
	newVindexFound := false
	switch node := predicate.(type) {
	case *sqlparser.ComparisonExpr:
		return tableRouting.planComparison(ctx, node)

	case *sqlparser.IsExpr:
		found := tableRouting.planIsExpr(ctx, node)
		newVindexFound = newVindexFound || found
	}

	return nil, newVindexFound, nil
}

// PickBestAvailableTableVindex goes over the available vindexes for this route and picks the best one available.
func (tableRouting *TableShardedRouting) PickBestAvailableTableVindex() {
	for _, t := range tableRouting.TindexPreds {
		option := t.bestOption()
		if option != nil && (tableRouting.Selected == nil || option.OpCode < tableRouting.Selected.OpCode) {
			tableRouting.Selected = option
			tableRouting.RouteOpCode = option.OpCode
		}
	}
}

func (vpp *TableVindexPlusPredicates) bestOption() *TableVindexOption {
	var best *TableVindexOption
	var keepOptions []*TableVindexOption
	for _, option := range vpp.Options {
		if option.Ready {
			if best == nil || option.OpCode < best.OpCode {
				best = option
			}
		} else {
			keepOptions = append(keepOptions, option)
		}
	}
	if best != nil {
		keepOptions = append(keepOptions, best)
	}
	vpp.Options = keepOptions
	return best
}

func (tableRouting *TableShardedRouting) planComparison(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) (routing Routing, foundNew bool, err error) {
	switch cmp.Operator {
	case sqlparser.EqualOp:
		found := tableRouting.planEqualOp(ctx, cmp)
		return nil, found, nil
	case sqlparser.InOp:
		found := tableRouting.planInOp(ctx, cmp)
		return nil, found, nil
	}
	return nil, false, nil
}

func (tableRouting *TableShardedRouting) planIsExpr(ctx *plancontext.PlanningContext, node *sqlparser.IsExpr) bool {
	// we only handle IS NULL correct. IsExpr can contain other expressions as well
	if node.Right != sqlparser.IsNullOp {
		return false
	}
	column, ok := node.Left.(*sqlparser.ColName)
	if !ok {
		return false
	}
	vdValue := &sqlparser.NullVal{}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}
	return tableRouting.haveMatchingVindex(ctx, node, vdValue, column, val, engine.EqualUnique)
}

func (tableRouting *TableShardedRouting) planEqualOp(ctx *plancontext.PlanningContext, node *sqlparser.ComparisonExpr) bool {
	column, ok := node.Left.(*sqlparser.ColName)
	other := node.Right
	vdValue := other
	if !ok {
		column, ok = node.Right.(*sqlparser.ColName)
		if !ok {
			// either the LHS or RHS have to be a column to be useful for the vindex
			return false
		}
		vdValue = node.Left
	}
	val := makeEvalEngineExpr(ctx, vdValue)
	if val == nil {
		return false
	}

	return tableRouting.haveMatchingVindex(ctx, node, vdValue, column, val, engine.EqualUnique)
}

func (tableRouting *TableShardedRouting) planInOp(ctx *plancontext.PlanningContext, cmp *sqlparser.ComparisonExpr) bool {
	switch left := cmp.Left.(type) {
	case *sqlparser.ColName:
		vdValue := cmp.Right

		valTuple, isTuple := vdValue.(sqlparser.ValTuple)
		if isTuple && len(valTuple) == 1 {
			return tableRouting.planEqualOp(ctx, &sqlparser.ComparisonExpr{Left: left, Right: valTuple[0], Operator: sqlparser.EqualOp})
		}

		value := makeEvalEngineExpr(ctx, vdValue)
		if value == nil {
			return false
		}
		return tableRouting.haveMatchingVindex(ctx, cmp, vdValue, left, value, engine.IN)
	case sqlparser.ValTuple:
		right, rightIsValTuple := cmp.Right.(sqlparser.ValTuple)
		if !rightIsValTuple {
			return false
		}
		return tableRouting.planCompositeInOpRecursive(ctx, cmp, left, right, nil)
	}

	return false
}

func (tableRouting *TableShardedRouting) planCompositeInOpRecursive(
	ctx *plancontext.PlanningContext,
	cmp *sqlparser.ComparisonExpr,
	left, right sqlparser.ValTuple,
	coordinates []int,
) bool {
	foundVindex := false
	cindex := len(coordinates)
	coordinates = append(coordinates, 0)
	for i, expr := range left {
		coordinates[cindex] = i
		switch expr := expr.(type) {
		case sqlparser.ValTuple:
			ok := tableRouting.planCompositeInOpRecursive(ctx, cmp, expr, right, coordinates)
			return ok || foundVindex
		case *sqlparser.ColName:
			// check if left col is a vindex
			if !tableRouting.hasVindex(expr) {
				continue
			}

			rightVals := make(sqlparser.ValTuple, len(right))
			for j, currRight := range right {
				switch currRight := currRight.(type) {
				case sqlparser.ValTuple:
					val := tupleAccess(currRight, coordinates)
					if val == nil {
						return false
					}
					rightVals[j] = val
				default:
					return false
				}
			}
			newPlanValues := makeEvalEngineExpr(ctx, rightVals)
			if newPlanValues == nil {
				return false
			}

			newVindex := tableRouting.haveMatchingVindex(ctx, cmp, rightVals, expr, newPlanValues, engine.MultiEqual)
			foundVindex = newVindex || foundVindex
		}
	}
	return foundVindex
}

func (tableRouting *TableShardedRouting) hasVindex(column *sqlparser.ColName) bool {
	for _, v := range tableRouting.TindexPreds {
		for _, col := range v.ColTableVindex {
			if column.Name.Equal(sqlparser.NewIdentifierCI(col.Column)) {
				return true
			}
		}
	}
	return false
}

func (tableRouting *TableShardedRouting) haveMatchingVindex(
	ctx *plancontext.PlanningContext,
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode engine.Opcode,
) bool {
	newVindexFound := false

	for _, t := range tableRouting.TindexPreds {
		// Check if the dependency is solved by the table ID.
		if !ctx.SemTable.DirectDeps(column).IsSolvedBy(t.TableID) {
			continue
		}
		newVindexFound = tableRouting.processSingleColumnVindex(node, valueExpr, column, value, opcode, t, newVindexFound)
	}

	return newVindexFound
}

func (tableRouting *TableShardedRouting) processSingleColumnVindex(
	node sqlparser.Expr,
	valueExpr sqlparser.Expr,
	column *sqlparser.ColName,
	value evalengine.Expr,
	opcode engine.Opcode,
	TableVindexPlusPredicates *TableVindexPlusPredicates,
	newVindexFound bool,
) bool {
	for _, col := range TableVindexPlusPredicates.ColTableVindex {
		if !column.Name.Equal(sqlparser.NewIdentifierCI(col.Column)) {
			return newVindexFound
		}
	}
	routeOpcode := opcode
	if routeOpcode == engine.Scatter {
		return newVindexFound
	}

	TableVindexPlusPredicates.Options = append(TableVindexPlusPredicates.Options, &TableVindexOption{
		Values:     []evalengine.Expr{value},
		ValueExprs: []sqlparser.Expr{valueExpr},
		Predicates: []sqlparser.Expr{node},
		OpCode:     routeOpcode,
		Ready:      true,
	})
	return true
}

func (tableRouting *TableShardedRouting) isScatter() bool {
	return tableRouting.RouteOpCode == engine.Scatter
}

// tryImprove rewrites the predicates for this query to see if we can produce a better plan.
// The rewrites are two:
//  1. first we turn the predicate a conjunctive normal form - an AND of ORs.
//     This can sometimes push a predicate to the top, so it's not hiding inside an OR
//  2. If that is not enough, an additional rewrite pass is performed where we try to
//     turn ORs into IN, which is easier for the planner to plan
func (tableRouting *TableShardedRouting) tryImprove(ctx *plancontext.PlanningContext, queryTable *QueryTable) (Routing, error) {
	oldPredicates := queryTable.Predicates
	queryTable.Predicates = nil
	tableRouting.SeenPredicates = nil
	var routing Routing = tableRouting
	var err error
	for _, pred := range oldPredicates {
		rewritten := sqlparser.RewritePredicate(pred)
		predicates := sqlparser.SplitAndExpression(nil, rewritten.(sqlparser.Expr))
		for _, predicate := range predicates {
			queryTable.Predicates = append(queryTable.Predicates, predicate)

			routing, err = UpdateRoutingLogic(ctx, predicate, routing)
			if err != nil {
				return nil, err
			}
		}
	}

	// If we have something other than a sharded routing with scatter, we are done
	if sr, ok := routing.(*TableShardedRouting); !ok || !sr.isScatter() {
		return routing, nil
	}

	// if we _still_ haven't found a better route, we can run this additional rewrite on any ORs we have
	for _, expr := range queryTable.Predicates {
		or, ok := expr.(*sqlparser.OrExpr)
		if !ok {
			continue
		}
		for _, predicate := range sqlparser.ExtractINFromOR(or) {
			routing, err = UpdateRoutingLogic(ctx, predicate, routing)
			if err != nil {
				return nil, err
			}
		}
	}

	return routing, nil
}

func (tableRouting *TableShardedRouting) UpdateTableRoutingParams(_ *plancontext.PlanningContext, rp *engine.TableRoutingParameters) error {
	if tableRouting.Selected != nil {
		rp.TableValues = tableRouting.Selected.Values
	}
	return nil
}
