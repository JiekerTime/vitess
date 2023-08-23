package engine

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestResultAggr(t *testing.T) {
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
