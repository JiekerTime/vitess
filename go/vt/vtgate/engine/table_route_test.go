package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

func TestGetTableQueries(t *testing.T) {
	tests := []struct {
		name               string
		query              string
		logicTb            tableindexes.LogicTableConfig
		bv                 map[string]*querypb.BindVariable
		expected           []*querypb.BoundQuery
		actualTableNameMap map[string]ActualTableName
	}{
		{
			name:  "Select query with table alias",
			query: `SELECT * FROM my_table AS t WHERE t.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with table name",
			query: `SELECT * FROM my_table WHERE id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with subquery",
			query: "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries and aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases",
			query: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases",
			query: `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		}, {
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
		{
			name:  "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query: `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			logicTb: tableindexes.LogicTableConfig{
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
			actualTableNameMap: map[string]ActualTableName{
				"my_table": {"my_actual_table_1", "my_actual_table_2"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := sqlparser.Parse(test.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			actual, err := getTableQueries(stmt, test.logicTb, test.bv, test.actualTableNameMap)
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
		act      string
		expected string
	}{
		{
			name:     "Select query with table alias",
			query:    `SELECT * FROM my_table AS t WHERE t.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM my_actual_table_1 AS t WHERE t.id = 1`,
		},
		{
			name:     "Select query with table name",
			query:    `SELECT * FROM my_table WHERE id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM my_actual_table_1 WHERE id = 1`,
		},
		{
			name:     "Select query with subquery",
			query:    "SELECT * FROM (SELECT * FROM my_table) AS t WHERE t.id = 1",
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM my_actual_table_1) AS t WHERE t.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 WHERE t2.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 WHERE t2.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and aliases",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 WHERE t3.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 WHERE t3.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and table aliases",
			query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM my_table) AS t1) AS t2 JOIN my_table AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
			act:      "my_actual_table_1",
			expected: `SELECT * FROM (SELECT * FROM (SELECT * FROM my_actual_table_1) AS t1) AS t2 JOIN my_actual_table_1 AS t3 ON t2.id = t3.id WHERE t2.id = 1`,
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases",
			query:    `SELECT t1.id, t2.name FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1`,
			act:      "my_actual_table_1",
			expected: "SELECT t1.id, t2.`name` FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1",
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name`,
			act:      "my_actual_table_1",
			expected: "SELECT t1.id, t2.`name`, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_actual_table_1 AS t1) AS t2) AS t3 JOIN my_actual_table_1 AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.`name`",
		}, {
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2) AS t3 JOIN my_table AS t4 ON t3.id = t4.id WHERE t3.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act:      "my_actual_table_1",
			expected: "select t1.id, t2.`name`, max(t3.age) from (select * from (select * from my_actual_table_1 as t1 limit 5, 10) as t2) as t3 join my_actual_table_1 as t4 on t3.id = t4.id where t3.id = 1 group by t1.id, t2.`name` order by max(t3.age) asc limit 5, 10",
		},
		{
			name:     "Select query with multiple subqueries and table aliases and column aliases and functions and order by and limit and offset and subquery with limit and offset and subquery with limit and offset and subquery with limit and offset",
			query:    `SELECT t1.id, t2.name, MAX(t3.age) FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM my_table AS t1 LIMIT 10 OFFSET 5) AS t2 LIMIT 10 OFFSET 5) AS t3 LIMIT 10 OFFSET 5) AS t4 JOIN my_table AS t5 ON t4.id = t5.id WHERE t4.id = 1 GROUP BY t1.id, t2.name ORDER BY MAX(t3.age) LIMIT 10 OFFSET 5`,
			act:      "my_actual_table_1",
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
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableIndexColumn: []*tableindexes.Column{{Column: "f1", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
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

func TestTableRouteSelectScatter(t *testing.T) {

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Scatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Scatter,
			LogicTable: logicTableMap,
			Values: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
			Vindex: vindex.(vindexes.TableSingleColumn),
		},
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

func TestTableRouteSelectEqualUnique(t *testing.T) {

	selvIndex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	sel.Vindex = selvIndex.(vindexes.SingleColumn)

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: EqualUnique,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Values: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
		Vindex: selvIndex.(vindexes.SingleColumn),
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     EqualUnique,
			LogicTable: logicTableMap,
			Values: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
			Vindex: vindex.(vindexes.TableSingleColumn),
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: select f1, f2 from lkp_1 {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}

func TestTableRouteSelectEqual(t *testing.T) {

	selvIndex, _ := vindexes.NewLookup("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
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

	sel.Vindex = selvIndex.(vindexes.SingleColumn)

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	routingParameters := &RoutingParameters{
		Opcode: Equal,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Values: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
		Vindex: selvIndex.(vindexes.SingleColumn),
	}

	statement, _, _ := sqlparser.Parse2("select f1, f2 from lkp")

	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)

	TableRoute := TableRoute{
		TableName:       "lkp",
		Query:           statement,
		FieldQuery:      "dummy_select_field",
		ShardRouteParam: routingParameters,
		TableRouteParam: &TableRoutingParameters{
			Opcode:     Equal,
			LogicTable: logicTableMap,
			Values: []evalengine.Expr{
				evalengine.NewLiteralInt(1),
			},
			Vindex: vindex.(vindexes.TableSingleColumn),
		},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRoute.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ExecuteMultiShard false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

}
