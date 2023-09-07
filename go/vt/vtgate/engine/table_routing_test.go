package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestFindTableRouteSelectEqual(t *testing.T) {

	logicTable := tableindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []tableindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_0",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_1",
				Index:           1,
			},
		},
		TableCount:       2,
		TableIndexColumn: []*tableindexes.Column{{Column: "col", ColumnType: querypb.Type_VARCHAR}},
	}

	logicTableMap := make(map[string]tableindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)

	TableRouteParam := &TableRoutingParameters{
		Opcode:     Equal,
		LogicTable: logicTableMap,
		Values: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
		Vindex: vindex.(vindexes.TableSingleColumn),
	}

	wantResult := map[string][]tableindexes.ActualTable{
		"lkp": {{ActualTableName: "lkp_0", Index: 0}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRouteParam.findRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result, wantResult) {
		//t.Errorf("find table routing error")
	}

}

func TestOrderbyIndex(t *testing.T) {

	ActualTable := []tableindexes.ActualTable{
		{
			ActualTableName: "lpk_1",
			Index:           1,
		},
		{
			ActualTableName: "lpk_2",
			Index:           0,
		},
	}

	sort.Slice(ActualTable, func(i, j int) bool {
		return ActualTable[i].Index < ActualTable[j].Index
	})

	for _, table := range ActualTable {
		fmt.Println(table.ActualTableName, table.Index)
	}
}
