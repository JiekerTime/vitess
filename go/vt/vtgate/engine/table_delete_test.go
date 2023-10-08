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
	"testing"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TODO: support Equal&&SubShard scenario.

func TestDeleteShardedTableEqualUnique(t *testing.T) {
	delStmt, _, _ := sqlparser.Parse2("delete from t_user where col = 1")

	del := &TableDelete{
		TableDML: &TableDML{
			AST:   delStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: EqualUnique,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: delete from t_user_1 where col = 1 {} ks.20-: delete from t_user_1 where col = 1 {} true false`,
	})
}

func TestDeleteShardedTableEqualUniqueWithDifColm(t *testing.T) {
	delStmt, _, _ := sqlparser.Parse2("delete from t_user where col = 2 and id = 1")
	vindex, _ := vindexes.NewHash("", nil)

	del := &TableDelete{
		TableDML: &TableDML{
			AST:   delStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: EqualUnique,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: EqualUnique,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewLiteralInt(2)},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: delete from t_user_2 where col = 2 and id = 1 {} true true`,
	})
}

func TestDeleteShardedTableIn(t *testing.T) {
	delStmt, _, _ := sqlparser.Parse2("delete from t_user where col in (1, 2, 3)")

	del := &TableDelete{
		TableDML: &TableDML{
			AST:   delStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: IN,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewTupleExpr(
					evalengine.NewLiteralInt(1),
					evalengine.NewLiteralInt(2),
					evalengine.NewLiteralInt(3))},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: delete from t_user_1 where col in (1, 2, 3) {} ks.20-: delete from t_user_1 where col in (1, 2, 3) {} true false`,
		`ExecuteMultiShard ks.-20: delete from t_user_1 where col in (1, 2, 3) {} ks.20-: delete from t_user_1 where col in (1, 2, 3) {} true false`,
		`ExecuteMultiShard ks.-20: delete from t_user_2 where col in (1, 2, 3) {} ks.20-: delete from t_user_2 where col in (1, 2, 3) {} true false`,
	})
}

func TestDeleteShardedTableMultiEqual(t *testing.T) {
	delStmt, _, _ := sqlparser.Parse2("delete from t_user where (name, col) in (('aa', 'bb'), ('cc', 'dd'))")

	del := &TableDelete{
		TableDML: &TableDML{
			AST:   delStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: MultiEqual,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
				TableValues: []evalengine.Expr{evalengine.NewTupleExpr(
					evalengine.NewLiteralString([]byte("bb"), collations.SystemCollation),
					evalengine.NewLiteralString([]byte("dd"), collations.SystemCollation))},
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		"ExecuteMultiShard ks.-20: delete from t_user_1 where (`name`, col) in (('aa', 'bb'), ('cc', 'dd')) {} ks.20-: delete from t_user_1 where (`name`, col) in (('aa', 'bb'), ('cc', 'dd')) {} true false",
		"ExecuteMultiShard ks.-20: delete from t_user_2 where (`name`, col) in (('aa', 'bb'), ('cc', 'dd')) {} ks.20-: delete from t_user_2 where (`name`, col) in (('aa', 'bb'), ('cc', 'dd')) {} true false",
	})
}

func TestDeleteShardedTableSharded(t *testing.T) {
	delStmt, _, _ := sqlparser.Parse2("delete from t_user")

	del := &TableDelete{
		TableDML: &TableDML{
			AST:   delStmt,
			Table: []*vindexes.Table{{Name: sqlparser.NewIdentifierCS("t_user")}},
			ShardRouteParam: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
			},
			TableRouteParam: &TableRoutingParameters{
				TableOpcode: Scatter,
				LogicTable:  getTestLogicTableConfig("", nil, nil),
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: delete from t_user_1 {} ks.20-: delete from t_user_1 {} true false`,
		`ExecuteMultiShard ks.-20: delete from t_user_2 {} ks.20-: delete from t_user_2 {} true false`,
	})
}
