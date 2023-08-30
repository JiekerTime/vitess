package engine

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
)

func TestGetTableQueries(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		logicTb  tableindexes.LogicTableConfig
		bv       map[string]*querypb.BindVariable
		expected []*querypb.BoundQuery
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

func TestResultMerge(t *testing.T) {
	resultSlice := []sqltypes.Result{
		{
			Fields: []*querypb.Field{
				// 定义字段
				{
					Name:  "id",
					Type:  sqltypes.Int64,
					Table: "test_001",
				},
				{
					Name:  "name",
					Type:  sqltypes.VarChar,
					Table: "test_002",
				},
			},
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				// 定义行数据
				{
					sqltypes.NewInt64(1),
					sqltypes.NewVarChar("John"),
				},
				{
					sqltypes.NewInt64(2),
					sqltypes.NewVarChar("Jane"),
				},
			},
		},

		{
			Fields: []*querypb.Field{
				// 定义字段
				{
					Name:  "id",
					Type:  sqltypes.Int64,
					Table: "test_003",
				},
				{
					Name:  "name",
					Type:  sqltypes.VarChar,
					Table: "test_004",
				},
			},
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				// 定义行数据
				{
					sqltypes.NewInt64(3),
					sqltypes.NewVarChar("Sto"),
				},
				{
					sqltypes.NewInt64(4),
					sqltypes.NewVarChar("Uve"),
				},
			},
		},
	}

	wantResult := &sqltypes.Result{

		Fields: []*querypb.Field{
			// 定义字段
			{
				Name:  "id",
				Type:  sqltypes.Int64,
				Table: "test",
			},
			{
				Name:  "name",
				Type:  sqltypes.VarChar,
				Table: "test",
			},
		},
		RowsAffected: 4,
		Rows: [][]sqltypes.Value{
			// 定义行数据
			{
				sqltypes.NewInt64(1),
				sqltypes.NewVarChar("John"),
			},
			{
				sqltypes.NewInt64(2),
				sqltypes.NewVarChar("Jane"),
			},
			// 定义行数据
			{
				sqltypes.NewInt64(3),
				sqltypes.NewVarChar("Sto"),
			},
			{
				sqltypes.NewInt64(4),
				sqltypes.NewVarChar("Uve"),
			},
		},
	}

	finalResult, _ := resultMerge("test", resultSlice)

	if !finalResult.Equal(wantResult) {
		t.Errorf("merge error !")
	}

}

func printResult(result sqltypes.Result) {
	// 打印字段名称
	for _, field := range result.Fields {
		fmt.Printf("%s(%s)\t", field.Name, field.Table)
	}

	fmt.Println()

	// 打印行数据
	for _, row := range result.Rows {
		for _, value := range row {
			fmt.Printf("%s\t", value.String())
		}
		fmt.Println()
	}
}
