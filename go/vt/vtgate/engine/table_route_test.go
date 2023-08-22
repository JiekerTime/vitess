package engine

import (
	"strings"
	"testing"

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
			name:  "Update query",
			query: `UPDATE my_table SET my_column = 'new_value' WHERE id = 1`,
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
					Sql:           `UPDATE my_actual_table_1 SET my_column = 'new_value' WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `UPDATE my_actual_table_2 SET my_column = 'new_value' WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Delete query",
			query: `DELETE FROM my_table WHERE id = 1`,
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
					Sql:           `DELETE FROM my_actual_table_1 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `DELETE FROM my_actual_table_2 WHERE id = 1`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
		{
			name:  "Insert query",
			query: `INSERT INTO my_table (my_column) VALUES ('new_value')`,
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
					Sql:           `INSERT INTO my_actual_table_1(my_column) VALUES ('new_value')`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
				{
					Sql:           `INSERT INTO my_actual_table_2(my_column) VALUES ('new_value')`,
					BindVariables: map[string]*querypb.BindVariable{},
				},
			},
		},
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
			name:  "Update query",
			query: `UPDATE my_table SET my_column = 'new_value' WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `UPDATE my_actual_table SET my_column = 'new_value' WHERE id = 1`,
		},
		{
			name:  "Delete query",
			query: `DELETE FROM my_table WHERE id = 1`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `DELETE FROM my_actual_table WHERE id = 1`,
		},
		{
			name:  "Insert query",
			query: `INSERT INTO my_table (my_column)VALUES ('new_value')`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table",
			},
			expected: `INSERT INTO my_actual_table(my_column) VALUES ('new_value')`,
		},
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
		// TODO: support joining
		{
			name:  "join query with table name",
			query: `select id from t_unshard join my_table`,
			act: tableindexes.ActualTable{
				ActualTableName: "my_actual_table_1",
			},
			expected: `select id from t_unshard join my_actual_table_1`,
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
