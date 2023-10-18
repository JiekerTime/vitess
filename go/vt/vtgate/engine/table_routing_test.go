package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestFindTableRouteSelectEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("splitTableHashMod", "splitTableHashMod", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
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
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	TableRouteParam := &TableRoutingParameters{
		TableOpcode: Equal,
		LogicTable:  logicTableMap,
		TableValues: []evalengine.Expr{
			evalengine.NewLiteralInt(1),
		},
	}

	wantResult := map[string][]vindexes.ActualTable{
		"lkp": {{ActualTableName: "lkp_0", Index: 0}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("find table routing error")
	}

}

func TestOrderbyIndex(t *testing.T) {

	ActualTable := []vindexes.ActualTable{
		{
			ActualTableName: "lpk_0",
			Index:           1,
		},
		{
			ActualTableName: "lpk_1",
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
