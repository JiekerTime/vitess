package operators

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func tablePlanQuery(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
	output, err = runPhasesForSplitTable(ctx, root)
	if err != nil {
		return nil, err
	}

	output, err = planOffsets(ctx, output)
	if err != nil {
		return nil, err
	}

	if rewrite.DebugOperatorTree {
		fmt.Println("After offset planning:")
		fmt.Println(ops.ToTree(output))
	}

	return addTruncationOrProjectionToReturnOutput(ctx, root, output)
}

func tryPushingDownLimitForSplitTable(ctx *plancontext.PlanningContext, in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return tryPushingDownLimitInRouteForSplitTable(ctx, in, src)
	case *Route:
		return tryPushingDownLimitInRoute(in, src)
	case *Projection:
		return nil, nil, vterrors.VT13001("unexpect case Projection")
	case *Aggregator:
		if isCrossShard(ctx.GetRoute()) && len(ctx.GetRoute().TableNameSlice) <= 1 {
			return rewrite.Swap(in, src, "limit pushed into aggregator")
		}
		return in, rewrite.SameTree, nil
	default:
		return setUpperLimitForSplitTable(in)
	}
}

func tryPushingDownLimitInRouteForSplitTable(ctx *plancontext.PlanningContext, in *Limit, src *TableRoute) (ops.Operator, *rewrite.ApplyResult, error) {
	if len(ctx.GetRoute().TableNameSlice) <= 1 {
		if src.IsSingleSplitTable() || isCrossShard(ctx.GetRoute()) {
			return rewrite.Swap(in, src, "limit pushed into tableRoute")
		}
	}
	return setUpperLimitForSplitTable(in)
}

func isCrossShard(route engine.Route) bool {
	switch route.Opcode {
	case engine.Unsharded, engine.DBA, engine.Next, engine.EqualUnique, engine.Reference:
		return false
	}
	return true
}

func setUpperLimitForSplitTable(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Pushed {
		return in, rewrite.SameTree, nil
	}
	in.Pushed = true
	visitor := func(op ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		return op, rewrite.SameTree, nil
	}
	shouldVisit := func(op ops.Operator) rewrite.VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin:
			// we can't push limits down on either side
			return rewrite.SkipChildren
		case *TableRoute:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			return rewrite.SkipChildren
		default:
			return rewrite.VisitChildren
		}
	}

	_, err := rewrite.TopDown(in.Source, TableID, visitor, shouldVisit)
	if err != nil {
		return nil, nil, err
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownOrderingForSplitTable(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(in, src, "push ordering under tableRoute")
	case *Route:
		return rewrite.Swap(in, src, "push ordering under route")
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, rewrite.NewTree("remove double ordering", src), nil
	case *ApplyJoin:
		if canPushLeft(ctx, src, in.Order) {
			// ApplyJoin is stable in regard to the columns coming from the LHS,
			// so if all the ordering columns come from the LHS, we can push down the Ordering there
			src.LHS, in.Source = in, src.LHS
			return src, rewrite.NewTree("push down ordering on the LHS of a join", in), nil
		}
	case *Projection:
		// we can move ordering under a projection if it's not introducing a column we're sorting by
		for _, by := range in.Order {
			if !fetchByOffset(by.SimplifiedExpr) {
				return in, rewrite.SameTree, nil
			}
		}
		return rewrite.Swap(in, src, "push ordering under projection")
	case *Aggregator:
		if !(src.QP.AlignGroupByAndOrderBy(ctx) || overlaps(ctx, in.Order, src.Grouping)) {
			return in, rewrite.SameTree, nil
		}
		return pushOrderingUnderAggr(ctx, in, src)
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownProjectionForSplitTable(ctx *plancontext.PlanningContext, p *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(p, src, "pushed projection under tableRoute")
	case *Route:
		return rewrite.Swap(p, src, "push projection under route")
	case *ApplyJoin:
		if p.FromAggr || !p.canPush(ctx) {
			return p, rewrite.SameTree, nil
		}
		return pushProjectionInApplyJoin(ctx, p, src)
	case *Limit:
		return rewrite.Swap(p, src, "push projection under limit")
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushOrExpandHorizonForSplitTable(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.IsDerived() {
		newOp, result, err := pushDerivedForSplitTable(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		if result != rewrite.SameTree {
			return newOp, result, nil
		}
	}
	if !reachedPhase(ctx, initialPlanning) {
		return in, rewrite.SameTree, nil
	}

	rb, isTableRoute := in.src().(*TableRoute)
	if isTableRoute && rb.IsSingleSplitTable() {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	hasHaving := isSel && sel.Having != nil

	canPush := isTableRoute &&
		!hasHaving &&
		!needsOrdering &&
		!qp.NeedsAggregation() &&
		!in.selectStatement().IsDistinct() &&
		in.selectStatement().GetLimit() == nil

	if canPush {
		return rewrite.Swap(in, rb, "push horizon into tableRoute")
	}

	return expandHorizonForSplitTable(ctx, in)
}

func pushDerivedForSplitTable(ctx *plancontext.PlanningContext, op *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	innerRoute, ok := op.Source.(*TableRoute)
	if !ok {
		return op, rewrite.SameTree, nil
	}

	if !(innerRoute.Routing.OpCode() == engine.EqualUnique) && !op.IsMergeable(ctx) {
		// no need to check anything if we are sure that we will only hit a single shard
		return op, rewrite.SameTree, nil
	}

	return rewrite.Swap(op, op.Source, "push derived under route")
}

func checkForSupportSql(_ *plancontext.PlanningContext, sel *sqlparser.Select) error {
	if hasSubqueryInExprsAndWhere(sel) {
		return vterrors.VT12001("subquery in split table")
	}
	return nil
}

func expandHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	statement := horizon.selectStatement()
	switch sel := statement.(type) {
	case *sqlparser.Select:
		return expandSelectHorizonForSplitTable(ctx, horizon, sel)
	case *sqlparser.Union:
		return expandUnionHorizonForSplitTable(ctx, horizon, sel)
	}
	return nil, nil, vterrors.VT13001(fmt.Sprintf("unexpected statement type %T", statement))
}

func expandUnionHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon, union *sqlparser.Union) (ops.Operator, *rewrite.ApplyResult, error) {
	op := horizon.Source

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
	}

	if union.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    union.Limit,
		}
	}

	if horizon.TableId != nil {
		proj := newAliasedProjection(op)
		proj.DT = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
		op = proj
	}

	if op == horizon.Source {
		return op, rewrite.NewTree("removed UNION horizon not used", op), nil
	}

	return op, rewrite.NewTree("expand UNION horizon into smaller components", op), nil
}

func expandSelectHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon, sel *sqlparser.Select) (ops.Operator, *rewrite.ApplyResult, error) {

	op, err := createProjectionFromSelectForSplitTable(ctx, horizon)
	if err != nil {
		return nil, nil, err
	}

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	var extracted []string
	if qp.HasAggr {
		extracted = append(extracted, "Aggregation")
	} else {
		extracted = append(extracted, "Projection")
	}

	if qp.NeedsDistinct() {
		op = &Distinct{
			Required: true,
			Source:   op,
			QP:       qp,
		}
		extracted = append(extracted, "Distinct")
	}

	if sel.Having != nil {
		op, err = addWherePredicatesForSplitTable(ctx, sel.Having.Expr, op)
		if err != nil {
			return nil, nil, err
		}
		extracted = append(extracted, "Filter")
	}

	if len(qp.OrderExprs) > 0 {
		op = &Ordering{
			Source: op,
			Order:  qp.OrderExprs,
		}
		extracted = append(extracted, "Ordering")
	}

	if sel.Limit != nil {
		op = &Limit{
			Source: op,
			AST:    sel.Limit,
		}
		extracted = append(extracted, "Limit")
	}

	return op, rewrite.NewTree(fmt.Sprintf("expand SELECT horizon into (%s)", strings.Join(extracted, ", ")), op), nil
}

// createProjectionFromSelectForSplitTable is simplified to createProjectionFromSelect.
func createProjectionFromSelectForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon) (out ops.Operator, err error) {
	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}

	var dt *DerivedTable
	if horizon.TableId != nil {
		dt = &DerivedTable{
			TableID: *horizon.TableId,
			Alias:   horizon.Alias,
			Columns: horizon.ColumnAliases,
		}
	}

	if !qp.NeedsAggregation() {
		projX, err := createProjectionWithoutAggr(ctx, qp, horizon.src())
		if err != nil {
			return nil, err
		}
		projX.DT = dt
		out = projX

		return out, nil
	}

	aggregations, complexAggr, err := qp.AggregationExpressions(ctx, true)
	if err != nil {
		return nil, err
	}

	a := &Aggregator{
		Source:       horizon.src(),
		Original:     true,
		QP:           qp,
		Grouping:     qp.GetGrouping(),
		Aggregations: aggregations,
		DT:           dt,
	}

	if complexAggr {
		return createProjectionForComplexAggregation(a, qp)
	}
	return createProjectionForSimpleAggregation(ctx, a, qp)
}

func tryPushDistinctForSplitTable(ctx *plancontext.PlanningContext, in *Distinct) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Required && in.PushedPerformance {
		return in, rewrite.SameTree, nil
	}
	switch src := in.Source.(type) {
	case *TableRoute:
		if isDistinct(src.Source) && src.IsSingleSplitTable() {
			return src, rewrite.NewTree("distinct not needed", in), nil
		}
		if src.IsSingleSplitTable() || !in.Required {
			return rewrite.Swap(in, src, "push distinct under tableRoute")
		}
		if isCrossShard(ctx.GetRoute()) && len(ctx.GetRoute().TableNameSlice) <= 1 {
			return rewrite.Swap(in, src, "push distinct under tableRoute, Cross-shard Distinct")
		}

		if isDistinct(src.Source) {
			return in, rewrite.SameTree, nil
		}

		src.Source = &Distinct{Source: src.Source}
		in.PushedPerformance = true

		return in, rewrite.NewTree("added distinct under tableRoute - kept original", src), nil
	case *Route:
		if isDistinct(src.Source) && src.IsSingleShard() {
			return src, rewrite.NewTree("distinct not needed", in), nil
		}
		if src.IsSingleShard() || !in.Required {
			return rewrite.Swap(in, src, "push distinct under route")
		}

		if isDistinct(src.Source) {
			return in, rewrite.SameTree, nil
		}

		src.Source = &Distinct{Source: src.Source}
		in.PushedPerformance = true

		return in, rewrite.NewTree("added distinct under route - kept original", src), nil
	case *Distinct:
		src.Required = false
		src.PushedPerformance = false
		return src, rewrite.NewTree("remove double distinct", src), nil
	case *Union:
		for i := range src.Sources {
			src.Sources[i] = &Distinct{Source: src.Sources[i]}
		}
		in.PushedPerformance = true

		return in, rewrite.NewTree("push down distinct under union", src), nil
	case *ApplyJoin:
		return nil, nil, vterrors.VT12001(fmt.Sprintf("unable to use: %T table type in split table", src))
	case *Ordering:
		in.Source = src.Source
		return in, rewrite.NewTree("remove ordering under distinct", in), nil
	}

	return in, rewrite.SameTree, nil
}

func tryPushUnionForSplitTable(ctx *plancontext.PlanningContext, op *Union) (ops.Operator, *rewrite.ApplyResult, error) {
	if res := compactUnion(op); res != rewrite.SameTree {
		return op, res, nil
	}

	var sources []ops.Operator
	var selects []sqlparser.SelectExprs
	var err error

	if op.distinct {
		sources, selects, err = mergeUnionInputInAnyOrderForSplitTable(ctx, op)
	} else {
		sources, selects, err = mergeUnionInputsInOrderForSplitTable(ctx, op)
	}
	if err != nil {
		return nil, nil, err
	}

	if len(sources) == 1 {
		result := sources[0].(*TableRoute)
		if result.IsSingleSplitTable() || !op.distinct {
			return result, rewrite.NewTree("push union under route", op), nil
		}

		return &Distinct{
			Source:   result,
			Required: true,
		}, rewrite.NewTree("push union under route", op), nil
	}

	if len(sources) == len(op.Sources) {
		return op, rewrite.SameTree, nil
	}
	return newUnion(sources, selects, op.unionColumns, op.distinct), rewrite.NewTree("merge union inputs", op), nil
}
