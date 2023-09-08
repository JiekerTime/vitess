package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestGetTableQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		logicTb  *tableindexes.LogicTableConfig
		bv       map[string]*querypb.BindVariable
		expected []*querypb.BoundQuery
	}{
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM my_actual_table_2 AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM my_actual_table_1 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM my_actual_table_2 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with subquery",
			query: "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM (SELECT * FROM my_actual_table_1) AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM my_actual_table_2) AS t WHERE t.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2) AS t1) AS t2 WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 JOIN my_actual_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_2) AS t1) AS t2 JOIN my_actual_table_2 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases",
			query: `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 JOIN my_actual_table_2 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_2 AS t1) AS t2) AS t3 JOIN my_actual_table_2 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		}, {
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2) as t3 join my_actual_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_2 as t1 limit 5, 10) as t2) as t3 join my_actual_table_2 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: &tableindexes.LogicTableConfig{
				LogicTableName: "my_table",
				ActualTableList: []tableindexes.ActualTable{
					{
						ActualTableName: "my_actual_table_1",
					},
					{
						ActualTableName: "my_actual_table_2",
					},
				},
			},
			bv: map[string]*querypb.BindVariable{},
			expected: []*querypb.BoundQuery{
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_1 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_2 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_2 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			actual, err := getTableQueries(stmt, test.logicTb, test.bv)
			if err != nil {
				t.Fatalf(err.Error())
			}

			if len(actual) != len(test.expected) {
				t.Errorf("Unexpected result. Expected: %v, Actual: %v", test.expected, actual)
			}

			for i, query := range test.expected {
				if !strings.EqualFold(query.Sql, actual[i].Sql) {
					t.Errorf("Unexpected result. Expected: %v, Actual: %v", test.expected, actual)
				}
			}
		})
	}
}

func TestRewriteQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		act      tableindexes.ActualTable
		expected string
	}{
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM my_actual_table_1 WHERE id = 1`,
		},
		{
			name:  "Select query with subquery",
			query: "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM (SELECT * FROM my_actual_table_1) AS t WHERE t.id = 1`,
		},
		{
			name:  "Select query with multiple subqueries",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 WHERE t2.id = 1`,
		},
		{
			name:  "Select query with multiple subqueries and aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
		},
		{
			name:  "Select query with multiple subqueries and table aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 JOIN my_actual_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases",
			query: `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
		}, {
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2) as t3 join my_actual_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2 limit 5, 10) as t3 limit 5, 10) as t4 join my_actual_table_1 as t5 on t4.id = t5.id where t4.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			actual, err := rewriteQuery(stmt, test.act, "my_table")
			if err != nil {
				t.Fatalf("Failed to rewrite query: %v", err)
			}

			if !strings.EqualFold(actual, test.expected) {
				t.Errorf("Unexpected result. Expected: %s, Actual: %s", test.expected, actual)
			}
		})
	}
}

func TestTableRouteGetFields(t *testing.T) {

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: []*tableindexes.Column{{Column: "f1", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]*tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = &logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	Values := []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "select f1, f2 from lkp",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Scatter,
			LogicTable: logicTableMap,
			Values:     Values,
		},
	}

	resultSlice := make([]*sqltypes.Result, 0)

	result1 := &sqltypes.Result{

		Fields: []*querypb.Field{
			// 定义字段
			{
				Name: "id",
				Type: sqltypes.Int64,
			},
			{
				Name: "name",
				Type: sqltypes.VarChar,
			},
		},
	}

	resultSlice = append(resultSlice, result1)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: resultSlice,
	}

	got, err := TableRoute.GetFields(context.Background(), vc, map[string]*querypb.BindVariable{})

	require.NoError(t, err)
	if !got.Equal(result1) {
		t.Errorf("l.GetFields:\n%v, want\n%v", got, result1)
	}

}

func TestTableRouteTryExecute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"f1":    "f1",
		"f2":    "f2",
	})
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	logicTable := &tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           1,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: []*tableindexes.Column{{Column: "f1", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]*tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	Values := []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Scatter,
			LogicTable: logicTableMap,
			Values:     Values,
		},
	}

	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: select f1, f2 from lkp_1 {} ks.20-: select f1, f2 from lkp_1 {} false false`,
		`ExecuteMultiShard ks.-20: select f1, f2 from lkp_2 {} ks.20-: select f1, f2 from lkp_2 {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}

func TestTableRouteSort(t *testing.T) {
	shardRouteParam := &RoutingParameters{
		Opcode: Unsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
	}
	tableIndexColumn := []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}}
	tableName := "t_user"
	sel := newTestTableRoute(shardRouteParam, tableName, tableIndexColumn, Scatter)
	sqlStmt, _, _ := sqlparser.Parse2("select id from t_user order by id")
	sel.Query = sqlStmt
	sel.FieldQuery = "select col1 from t_user where 1 != 1"
	sel.OrderBy = []OrderByParams{{
		Col:             0,
		WeightStringCol: -1,
	}}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
				"1",
				"3",
				"2",
			),
		},
	}

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFieldsWithTableName(
			"id",
			"int64",
			tableName,
		),
		"1",
		"1",
		"2",
		"3",
	)
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	expectResult(t, "sel.Execute", result, wantResult)

	sel.OrderBy[0].Desc = true
	vc.Rewind()
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	wantResult = sqltypes.MakeTestResult(
		sqltypes.MakeTestFieldsWithTableName(
			"id",
			"int64",
			tableName,
		),
		"3",
		"2",
		"1",
		"1",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestTableRouteSortTruncate(t *testing.T) {
	shardRouteParam := &RoutingParameters{
		Opcode: Unsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
	}
	tableIndexColumn := []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}}
	tableName := "t_user"
	sel := newTestTableRoute(shardRouteParam, tableName, tableIndexColumn, Scatter)
	sqlStmt, _, _ := sqlparser.Parse2("dummy_select")
	sel.Query = sqlStmt
	sel.FieldQuery = "dummy_select_field"
	sel.OrderBy = []OrderByParams{{
		Col: 0,
	}}
	sel.TruncateColumnCount = 1

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|col",
					"int64|int64",
				),
				"1|1",
				"1|1",
				"3|1",
				"2|1",
			),
		},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFieldsWithTableName(
			"id",
			"int64",
			tableName,
		),
		"1",
		"1",
		"2",
		"3",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func newTestTableRoute(shardRouteParam *RoutingParameters, tableName string, tableIndexColumn []*tableindexes.Column, tableOpcode Opcode) *TableRoute {
	logicTableMap := make(map[string]*tableindexes.LogicTableConfig)
	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: tableName,
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: tableName + "_1",
				Index:           1,
			},
			{
				ActualTableName: tableName + "_2",
				Index:           2,
			},
		},
		TableIndexColumn: tableIndexColumn,
	}
	logicTableMap[tableName] = &logicTable

	return &TableRoute{
		TableName:       tableName,
		ShardRouteParam: shardRouteParam,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     tableOpcode,
			LogicTable: logicTableMap,
		},
	}
}
