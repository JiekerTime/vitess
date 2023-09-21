/*
Copyright 2019 The Vitess Authors.

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

package engine

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*TableDelete)(nil)

// TableDelete represents the instructions to perform a spliting delete.
type TableDelete struct {
	*TableDML

	// Delete does not take inputs
	noInputs
}

// TryExecute performs a non-streaming exec.
func (del *TableDelete) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	// 计算分片
	rss, _, err := del.ShardRouteParam.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	// 计算涉及到的分表
	actualTableMap, err := del.TableRouteParam.findTableRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	// 排序分表
	SortTableList(actualTableMap)

	// todo:根据分片和分表的Opcode确实是否开启事务
	switch del.ShardRouteParam.Opcode {
	case IN, Scatter, EqualUnique, MultiEqual:
		return del.execMultiDestination(ctx, del, vcursor, bindVars, rss, nil, actualTableMap)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", del.ShardRouteParam.Opcode)
	}
}

// TryStreamExecute performs a streaming exec.
func (del *TableDelete) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields fetches the field info.
func (del *TableDelete) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", del.Queries)
}

func (del *TableDelete) description() PrimitiveDescription {
	other := map[string]any{
		"Queries":              sqlparser.String(del.AST),
		"Table":                del.GetTableName(),
		"MultiShardAutocommit": del.MultiShardAutocommit,
		"QueryTimeout":         del.QueryTimeout,
	}

	if del.ShardRouteParam.Vindex != nil {
		other["Vindex"] = del.ShardRouteParam.Vindex.String()
	}
	if del.KsidVindex != nil {
		other["KsidVindex"] = del.KsidVindex.String()
		other["KsidLength"] = del.KsidLength
	}
	if len(del.ShardRouteParam.Values) > 0 {
		s := []string{}
		for _, value := range del.ShardRouteParam.Values {
			s = append(s, evalengine.FormatExpr(value))
		}
		other["Values"] = s
	}
	if len(del.TableRouteParam.TableValues) > 0 {
		s := []string{}
		for _, value := range del.TableRouteParam.TableValues {
			s = append(s, evalengine.FormatExpr(value))
		}
		other["TableValues"] = s
	}

	return PrimitiveDescription{
		OperatorType:     "TableDelete",
		Keyspace:         del.ShardRouteParam.Keyspace,
		Variant:          del.ShardRouteParam.Opcode.String() + "-" + del.TableRouteParam.TableOpcode.String(),
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}
