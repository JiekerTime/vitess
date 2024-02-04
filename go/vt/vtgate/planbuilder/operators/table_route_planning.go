/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package operators

import (
	"io"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func optimizeJoinForSplitTable(ctx *plancontext.PlanningContext, op *Join) (ops.Operator, *rewrite.ApplyResult, error) {
	return mergeOrJoinForSplitTable(ctx, op.LHS, op.RHS, sqlparser.SplitAndExpression(nil, op.Predicate), !op.LeftJoin)
}

func findBestJoinForSplitTable(
	ctx *plancontext.PlanningContext,
	qg *QueryGraph,
	plans []ops.Operator,
	planCache opCacheMap,
	crossJoinsOK bool,
) (bestPlan ops.Operator, lIdx int, rIdx int, err error) {
	for i, lhs := range plans {
		for j, rhs := range plans {
			if i == j {
				continue
			}
			joinPredicates := qg.GetPredicates(TableID(lhs), TableID(rhs))
			if len(joinPredicates) == 0 && !crossJoinsOK {
				// if there are no predicates joining the two tables,
				// creating a join between them would produce a
				// cartesian product, which is almost always a bad idea
				continue
			}
			plan, err := getJoinForSplitTable(ctx, planCache, lhs, rhs, joinPredicates)
			if err != nil {
				return nil, 0, 0, err
			}
			if bestPlan == nil || CostOf(plan) < CostOf(bestPlan) {
				bestPlan = plan
				// remember which plans we based on, so we can remove them later
				lIdx = i
				rIdx = j
			}
		}
	}
	return bestPlan, lIdx, rIdx, nil
}

func getJoinForSplitTable(ctx *plancontext.PlanningContext, cm opCacheMap, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr) (ops.Operator, error) {
	solves := tableSetPair{left: TableID(lhs), right: TableID(rhs)}
	cachedPlan := cm[solves]
	if cachedPlan != nil {
		return cachedPlan, nil
	}

	join, _, err := mergeOrJoinForSplitTable(ctx, lhs, rhs, joinPredicates, true)
	if err != nil {
		return nil, err
	}
	cm[solves] = join
	return join, nil
}

// requiresSwitchingSidesForSplitTable will return true if any of the operators with the root from the given operator tree
// is of the type that should not be on the RHS of a join
func requiresSwitchingSidesForSplitTable(ctx *plancontext.PlanningContext, op ops.Operator) bool {
	required := false

	_ = rewrite.Visit(op, func(current ops.Operator) error {
		horizon, isHorizon := current.(*Horizon)

		if isHorizon && horizon.IsDerived() && !horizon.IsMergeable(ctx) {
			required = true
			return io.EOF
		}

		return nil
	})

	return required
}

func mergeOrJoinForSplitTable(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr, inner bool) (ops.Operator, *rewrite.ApplyResult, error) {
	newPlan, err := mergeJoinInputsForSplitTable(ctx, lhs, rhs, joinPredicates, newJoinMerge(joinPredicates, inner))
	if err != nil {
		return nil, nil, err
	}
	if newPlan != nil {
		return newPlan, rewrite.NewTree("merge routes into single operator", newPlan), nil
	}

	if len(joinPredicates) > 0 && requiresSwitchingSidesForSplitTable(ctx, rhs) {
		if !inner {
			return nil, nil, vterrors.VT12001("LEFT JOIN with LIMIT on the outer side")
		}

		if requiresSwitchingSidesForSplitTable(ctx, lhs) {
			return nil, nil, vterrors.VT12001("JOIN between derived tables with LIMIT")
		}

		join := NewApplyJoin(Clone(rhs), Clone(lhs), nil, !inner)
		newOp, err := pushJoinPredicates(ctx, joinPredicates, join)
		if err != nil {
			return nil, nil, err
		}
		return newOp, rewrite.NewTree("logical join to applyJoin, switching side because LIMIT", newOp), nil
	}

	join := NewApplyJoin(Clone(lhs), Clone(rhs), nil, !inner)
	newOp, err := pushJoinPredicates(ctx, joinPredicates, join)
	if err != nil {
		return nil, nil, err
	}
	return newOp, rewrite.NewTree("logical join to applyJoin ", newOp), nil
}

func operatorsToRoutesForSplitTable(a, b ops.Operator) (*TableRoute, *TableRoute) {
	aRoute, ok := a.(*TableRoute)
	if !ok {
		return nil, nil
	}
	bRoute, ok := b.(*TableRoute)
	if !ok {
		return nil, nil
	}
	return aRoute, bRoute
}
