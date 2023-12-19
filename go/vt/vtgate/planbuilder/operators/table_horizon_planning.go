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
	// for DML
	if _, ok := root.(*Horizon); !ok {
		return root, nil
	}

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

	return addTruncationOrProjectionToReturnOutputForSplitTable(ctx, root, output)
}

func tryPushingDownLimitForSplitTable(ctx *plancontext.PlanningContext, in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *TableRoute:
		return tryPushingDownLimitInRouteForSplitTable(ctx, in, src)
	case *Projection:
		return nil, nil, vterrors.VT13001("unexpect case Projection")
	case *Aggregator:
		if isCrossShard(ctx.GetRoute()) {
			return rewrite.Swap(in, src, "limit pushed into aggregator")
		}
		return in, rewrite.SameTree, nil
	default:
		return setUpperLimitForSplitTable(in)
	}
}

func tryPushingDownLimitInRouteForSplitTable(ctx *plancontext.PlanningContext, in *Limit, src *TableRoute) (ops.Operator, *rewrite.ApplyResult, error) {
	if src.IsSingleSplitTable() || isCrossShard(ctx.GetRoute()) {
		return rewrite.Swap(in, src, "limit pushed into tableRoute")
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
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, rewrite.NewTree("remove double ordering", src), nil
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

func tryPushingDownProjectionForSplitTable(_ *plancontext.PlanningContext, p *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *TableRoute:
		return rewrite.Swap(p, src, "pushed projection under tableRoute")
	case *Limit:
		return rewrite.Swap(p, src, "push projection under limit")
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushOrExpandHorizonForSplitTable(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
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

func checkForSupportSql(_ *plancontext.PlanningContext, sel *sqlparser.Select) error {
	if hasSubqueryInExprsAndWhere(sel) {
		return vterrors.VT12001("subquery in split table")
	}
	return nil
}

func expandHorizonForSplitTable(ctx *plancontext.PlanningContext, horizon *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	sel, _ := horizon.selectStatement().(*sqlparser.Select)

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

func addTruncationOrProjectionToReturnOutputForSplitTable(ctx *plancontext.PlanningContext, oldHorizon ops.Operator, output ops.Operator) (ops.Operator, error) {
	cols, err := output.GetSelectExprs(ctx)
	if err != nil {
		return nil, err
	}

	horizon := oldHorizon.(*Horizon)

	sel := sqlparser.GetFirstSelect(horizon.Query)

	if len(sel.SelectExprs) == len(cols) {
		return output, nil
	}

	if tryTruncateColumnsAt(output, len(sel.SelectExprs)) {
		return output, nil
	}

	return nil, vterrors.VT13001("split table not implement yet")
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
		if isCrossShard(ctx.GetRoute()) {
			return rewrite.Swap(in, src, "push distinct under tableRoute, Cross-shard Distinct")
		}

		if isDistinct(src.Source) {
			return in, rewrite.SameTree, nil
		}

		src.Source = &Distinct{Source: src.Source}
		in.PushedPerformance = true

		return in, rewrite.NewTree("added distinct under tableRoute - kept original", src), nil
	case *Distinct:
		src.Required = false
		src.PushedPerformance = false
		return src, rewrite.NewTree("remove double distinct", src), nil
	case *Union:
		return nil, nil, vterrors.VT12001(fmt.Sprintf("unable to use: %T table type in split table", src))
	case *ApplyJoin:
		return nil, nil, vterrors.VT12001(fmt.Sprintf("unable to use: %T table type in split table", src))
	case *Ordering:
		in.Source = src.Source
		return in, rewrite.NewTree("remove ordering under distinct", in), nil
	}

	return in, rewrite.SameTree, nil
}
