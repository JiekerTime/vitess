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

package sqlparser

import (
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkVisitLargeExpression(b *testing.B) {
	gen := NewGenerator(rand.New(rand.NewSource(1)), 5)
	exp := gen.Expression(ExprGeneratorConfig{})

	depth := 0
	for i := 0; i < b.N; i++ {
		_ = Rewrite(exp, func(cursor *Cursor) bool {
			depth++
			return true
		}, func(cursor *Cursor) bool {
			depth--
			return true
		})
	}
}

func TestReplaceWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	stmt, err := Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case *Select:
			node.SelectExprs[0] = &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			node.SelectExprs = append(node.SelectExprs, &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestReplaceAndRevisitWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	stmt, err := Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case SelectExprs:
			if len(node) != 1 {
				return true
			}
			expr1 := &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			expr2 := &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			}
			cursor.ReplaceAndRevisit(SelectExprs{expr1, expr2})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestChangeValueTypeGivesError(t *testing.T) {
	parse, err := Parse("select * from a join b on a.id = b.id")
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "[BUG] tried to replace 'On' on 'JoinCondition'", r)
		}
	}()
	_ = Rewrite(parse, func(cursor *Cursor) bool {
		_, ok := cursor.Node().(*ComparisonExpr)
		if ok {
			cursor.Replace(&NullVal{}) // this is not a valid replacement because the container is a value type
		}
		return true
	}, nil)

}

func TestSelectScenarioRewriteSplitTableName(t *testing.T) {

	type testCase struct {
		originSql string
		expect    string
		tableMap  map[string]string
	}
	ts := []testCase{{
		originSql: "select * from t_user",
		expect:    "select * from t_user_1",
		tableMap: map[string]string{
			"t_user": "t_user_1",
		},
	}, {
		originSql: "select * from `user`.t_user",
		expect:    "select * from `user`.t_user_2",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select * from `user`.t_user join t_msg",
		expect:    "select * from `user`.t_user_2 join t_msg_2",
		tableMap: map[string]string{
			"t_user": "t_user_2",
			"t_msg":  "t_msg_2",
		},
	}, {
		originSql: "select * from `user`.t_user join t_msg",
		expect:    "select * from `user`.t_user_2 join t_msg",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select * from `user`.t_user as a join t_msg",
		expect:    "select * from `user`.t_user_2 as a join t_msg",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select * from `user`.t_user as a join t_msg as b",
		expect:    "select * from `user`.t_user_2 as a join t_msg_2 as b",
		tableMap: map[string]string{
			"t_user": "t_user_2",
			"t_msg":  "t_msg_2",
		},
	}, {
		originSql: "select t_user.* from t_user where t_user.col = 5 and t_user.id = 1",
		expect:    "select t_user_2.* from t_user_2 where t_user_2.col = 5 and t_user_2.id = 1",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select t_user.* from t_user t_user where t_user.col = 5 and t_user.id = 1",
		expect:    "select t_user.* from t_user_2 as t_user where t_user.col = 5 and t_user.id = 1",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select t_user.* from t_user as t_user where t_user.col = 5 and t_user.id = 1",
		expect:    "select t_user.* from t_user_2 as t_user where t_user.col = 5 and t_user.id = 1",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select t_user.* from t_user.t_user as t_user where t_user.col = 5 and t_user.id = 1",
		expect:    "select t_user.* from t_user.t_user_2 as t_user where t_user.col = 5 and t_user.id = 1",
		tableMap: map[string]string{
			"t_user": "t_user_2",
		},
	}, {
		originSql: "select t_order.* from t_user.t_user as t_order left join t_order as t_user on t_order.id = t_user.id where t_user.col = 5",
		expect:    "select t_order.* from t_user.t_user_2 as t_order left join t_order_1 as t_user on t_order.id = t_user.id where t_user.col = 5",
		tableMap: map[string]string{
			"t_user":  "t_user_2",
			"t_order": "t_order_1",
		},
	},
	}

	for _, tc := range ts {
		t.Run(tc.originSql, func(t *testing.T) {
			sqlNode, err := Parse(tc.originSql)
			if err != nil {
				t.Errorf("SQL parse error")
			}
			ReplaceTbName(sqlNode, tc.tableMap, false)
			assert.Equal(t, tc.expect, String(sqlNode))

		})

	}

}

func TestDMLScenarioRewriteSplitTableName(t *testing.T) {

	type testCase struct {
		originSql string
		expect    string
		tableMap  map[string]string
	}
	ts := []testCase{
		{
			originSql: "delete from t_user",
			expect:    "delete from t_user_1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
		}, {
			originSql: "delete from t_user as t_user where t_user.id = 1",
			expect:    "delete from t_user_1 as t_user where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
		}, {
			originSql: "delete t_user from t_user as t_user where t_user.id = 1",
			expect:    "delete t_user from t_user_1 as t_user where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
		}, {
			originSql: "update t_user set val = 1 where id = 1",
			expect:    "update t_user_1 set val = 1 where id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
		}, {
			originSql: "update t_user as t_user set t_user.val = 1 where t_user.id = 1",
			expect:    "update t_user_1 as t_user set t_user.val = 1 where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
		},
		// TODO: add more complicate statements.
	}

	for _, tc := range ts {
		t.Run(tc.originSql, func(t *testing.T) {
			sqlNode, err := Parse(tc.originSql)
			if err != nil {
				t.Errorf("SQL parse error")
			}
			ReplaceTbName(sqlNode, tc.tableMap, false)
			assert.Equal(t, tc.expect, String(sqlNode))

		})

	}

}

type (
	jsonTestCase struct {
		OriginSQL string            `json:"originSql"`
		Expect    string            `json:"expect"`
		TableMap  map[string]string `json:"tableMap"`
	}
)

func readJSONTests(filename string) []jsonTestCase {
	var output []jsonTestCase
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(file)
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func TestCopyOnRewriteTableName(t *testing.T) {
	testCases := readJSONTests("rewrite_table_name.json")
	for _, tcase := range testCases {
		t.Run(tcase.OriginSQL, func(t *testing.T) {
			sqlNode, err := Parse(tcase.OriginSQL)
			if err != nil {
				t.Errorf("SQL parse error")
			}
			newNode := ReplaceTbName(sqlNode, tcase.TableMap, true)
			assert.Equal(t, tcase.Expect, String(newNode))
		})
	}
}

func BenchmarkCopyOnRewriteTableName(b *testing.B) {
	testCases := readJSONTests("rewrite_table_name.json")

	for _, tcase := range testCases {
		b.Run(tcase.OriginSQL, func(t *testing.B) {
			sqlNode, err := Parse(tcase.OriginSQL)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for i := 0; i < 10000; i++ {
					newNode := ReplaceTbName(sqlNode, tcase.TableMap, true)
					String(newNode)
				}
			}
		})
	}
}

func BenchmarkRewriteSplitTableName(b *testing.B) {
	testCases := readJSONTests("rewrite_table_name.json")
	for _, tcase := range testCases {
		b.Run(tcase.OriginSQL, func(t *testing.B) {
			sqlNode, err := Parse(tcase.OriginSQL)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for i := 0; i < 10000; i++ {
					newNode := DeepCloneStatement(sqlNode)
					ReplaceTbName(newNode, tcase.TableMap, true)
					String(sqlNode)
				}
			}
		})
	}
}

func TestReplaceDQLTbName2Token(t *testing.T) {

	type testCase struct {
		originSql    string
		expect       string
		tableMap     map[string]string
		replacements map[string]string
	}
	ts := []testCase{
		{
			originSql: "select * from t_user",
			expect:    "select * from `:tb_vtg0`",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select * from `user`.t_user",
			expect:    "select * from `user`.`:tb_vtg0`",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select * from `user`.t_user join t_order",
			expect:    "select * from `user`.`:tb_vtg0` join t_order",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select * from `user`.t_user join t_order",
			expect:    "select * from `user`.`:tb_vtg0` join `:tb_vtg1`",
			tableMap: map[string]string{
				"t_user":  "t_user_1",
				"t_order": "t_order_1",
			},
			replacements: map[string]string{
				"t_user":  ":tb_vtg0",
				"t_order": ":tb_vtg1",
			},
		},
		{
			originSql: "select * from `user`.t_user as t_user join t_order",
			expect:    "select * from `user`.`:tb_vtg0` as t_user join t_order",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select * from `user`.t_user as t_user join t_order",
			expect:    "select * from `user`.`:tb_vtg0` as t_user join `:tb_vtg1`",
			tableMap: map[string]string{
				"t_user":  "t_user_1",
				"t_order": "t_order_1",
			},
			replacements: map[string]string{
				"t_user":  ":tb_vtg0",
				"t_order": ":tb_vtg1",
			},
		},
		{
			originSql: "select * from `user`.t_user as t_user join t_order as t_order",
			expect:    "select * from `user`.`:tb_vtg0` as t_user join t_order as t_order",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select * from `user`.t_user as t_user join t_order as t_order",
			expect:    "select * from `user`.`:tb_vtg0` as t_user join `:tb_vtg1` as t_order",
			tableMap: map[string]string{
				"t_user":  "t_user_1",
				"t_order": "t_order_1",
			},
			replacements: map[string]string{
				"t_user":  ":tb_vtg0",
				"t_order": ":tb_vtg1",
			},
		},
		{
			originSql: "select t_user.* from t_user where t_user.col = 5 and t_user.id = 1",
			expect:    "select `:tb_vtg0`.* from `:tb_vtg0` where `:tb_vtg0`.col = 5 and `:tb_vtg0`.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select t_user.* from t_user as t_user where t_user.col = 5 and t_user.id = 1",
			expect:    "select t_user.* from `:tb_vtg0` as t_user where t_user.col = 5 and t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select t_order.* from t_user.t_user as t_order left join t_order as t_user on t_order.id = t_user.id where t_user.col = 5",
			expect:    "select t_order.* from t_user.`:tb_vtg0` as t_order left join t_order as t_user on t_order.id = t_user.id where t_user.col = 5",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "select t_order.* from t_user.t_user as t_order left join t_order as t_user on t_order.id = t_user.id where t_user.col = 5",
			expect:    "select t_order.* from t_user.`:tb_vtg0` as t_order left join `:tb_vtg1` as t_user on t_order.id = t_user.id where t_user.col = 5",
			tableMap: map[string]string{
				"t_user":  "t_user_1",
				"t_order": "t_order_1",
			},
			replacements: map[string]string{
				"t_user":  ":tb_vtg0",
				"t_order": ":tb_vtg1",
			},
		},
	}

	for _, tc := range ts {
		t.Run(tc.originSql, func(t *testing.T) {
			sqlNode, err := Parse(tc.originSql)
			if err != nil {
				t.Errorf("SQL parse error")
			}
			assert.Equal(t, tc.expect, String(ReplaceTbName(sqlNode, tc.replacements, true)))
			assert.Equal(t, tc.originSql, String(sqlNode))
		})
	}
}

func TestReplaceDMLTbName2Token(t *testing.T) {

	type testCase struct {
		originSql    string
		expect       string
		tableMap     map[string]string
		replacements map[string]string
	}
	ts := []testCase{
		{
			originSql: "delete from t_user",
			expect:    "delete from `:tb_vtg0`",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "delete from t_user as t_user where t_user.id = 1",
			expect:    "delete from `:tb_vtg0` as t_user where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "delete t_user from t_user as t_user where t_user.id = 1",
			expect:    "delete t_user from `:tb_vtg0` as t_user where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "update t_user set val = 1 where id = 1",
			expect:    "update `:tb_vtg0` set val = 1 where id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
		{
			originSql: "update t_user as t_user set t_user.val = 1 where t_user.id = 1",
			expect:    "update `:tb_vtg0` as t_user set t_user.val = 1 where t_user.id = 1",
			tableMap: map[string]string{
				"t_user": "t_user_1",
			},
			replacements: map[string]string{
				"t_user": ":tb_vtg0",
			},
		},
	}

	for _, tc := range ts {
		t.Run(tc.originSql, func(t *testing.T) {
			sqlNode, err := Parse(tc.originSql)
			if err != nil {
				t.Errorf("SQL parse error")
			}
			assert.Equal(t, tc.expect, String(ReplaceTbName(sqlNode, tc.replacements, true)))
			assert.Equal(t, tc.originSql, String(sqlNode))
		})
	}
}

func BenchmarkReplaceMethod(b *testing.B) {
	tests := []struct {
		query  string
		token  string
		tbName string
	}{
		{
			query:  "select id from `:tb_vtg0`",
			token:  ":tb_vtg0",
			tbName: "t_user_1",
		},
		{
			query:  "select id from `:tb_vtg0` join `:tb_vtg1`",
			token:  ":tb_vtg0",
			tbName: "t_user_1",
		},
		{
			query:  "select * from `user`.`:tb_vtg0` as t_user join `:tb_vtg1`",
			token:  ":tb_vtg0",
			tbName: "t_user_1",
		},
		{
			query:  "select `:tb_vtg1`.* from t_user.`:tb_vtg0` as `:tb_vtg1` left join `:tb_vtg1` as `:tb_vtg0` on `:tb_vtg1`.id = `:tb_vtg0`.id where `:tb_vtg0`.col = 5",
			token:  ":tb_vtg0",
			tbName: "t_user_1",
		},
	}
	for _, t := range tests {
		b.Run("systemMethod-"+t.query, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < 100000; i++ {
				strings.Replace(t.query, FormateToken(t.token), t.tbName, -1)
			}
		})
		b.Run("-"+t.query, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < 100000; i++ {
				ReplaceToken(t.query, t.token, t.tbName)
			}
		})
	}
}
