package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestFindTableRouteSelectEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("split_table_binaryhash", "split_table_binaryhash", nil)
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

func TestFindTableRouteSplitTableListSelectEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("split_table_list", "split_table_list", map[string]string{"json": `{"east":["1","2","3"],"north":["4","5","6"],"south":["7","8","9"],"west":["10","11","12"]}`})
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_east",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_north",
				Index:           1,
			},
			{
				ActualTableName: "lkp" + "_south",
				Index:           2,
			},
			{
				ActualTableName: "lkp" + "_west",
				Index:           3,
			},
		},
		TableCount:       4,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_VARCHAR}},
		TableVindex:      vindex,
		Params: map[string]*vindexes.TableParams{
			"0":  {Name: "east", Index: 0},
			"1":  {Name: "east", Index: 0},
			"2":  {Name: "east", Index: 0},
			"3":  {Name: "east", Index: 0},
			"4":  {Name: "south", Index: 1},
			"5":  {Name: "south", Index: 1},
			"6":  {Name: "south", Index: 1},
			"7":  {Name: "west", Index: 2},
			"8":  {Name: "west", Index: 2},
			"9":  {Name: "north", Index: 3},
			"10": {Name: "north", Index: 3},
			"11": {Name: "north", Index: 3},
		},
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
		"lkp": {{ActualTableName: "lkp_east", Index: 0}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("find table routing error")
	}

}

func TestFindTableRouteSelectRangeMMVindexEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("split_table_range_mm", "split_table_range_mm", nil)
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName: "lkp",
		ActualTableList: []vindexes.ActualTable{
			{
				ActualTableName: "lkp" + "_1",
				Index:           0,
			},
			{
				ActualTableName: "lkp" + "_2",
				Index:           1,
			},
		},
		TableCount:       12,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_DATETIME}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	TableRouteParam := &TableRoutingParameters{
		TableOpcode: Equal,
		LogicTable:  logicTableMap,
		TableValues: []evalengine.Expr{
			evalengine.NewLiteralString([]byte("2023-01-11"), collations.SystemCollation),
		},
	}

	wantResult := map[string][]vindexes.ActualTable{
		"lkp": {{ActualTableName: "lkp_1", Index: 0}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result, wantResult) {
		t.Errorf("find table routing error")
	}

}

func TestFindTableRouteSelectRangeMMDDVindexEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("split_table_range_mmdd", "split_table_range_mmdd", nil)
	var table vindexes.LogicTableConfig
	table.TableCount = vindexes.RangeMMDDTableCount
	for i := 0; i < vindexes.RangeMMDDTableCount; i++ {
		vindexes.LogicToActualTable("t_user", i, "split_table_range_mmdd", &table)
	}
	logicTable := &vindexes.LogicTableConfig{
		LogicTableName:   "t_user",
		ActualTableList:  table.ActualTableList,
		TableCount:       366,
		TableIndexColumn: []*vindexes.TableColumn{{Column: sqlparser.NewIdentifierCI("col"), ColumnType: querypb.Type_DATETIME}},
		TableVindex:      vindex,
	}

	logicTableMap := make(map[string]*vindexes.LogicTableConfig)
	logicTableMap[logicTable.LogicTableName] = logicTable

	TableRouteParam := &TableRoutingParameters{
		TableOpcode: Equal,
		LogicTable:  logicTableMap,
		TableValues: []evalengine.Expr{
			evalengine.NewLiteralString([]byte("2023-01-1"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2024-02-2"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2024-02-29"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2020-3-31"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2020-4-1"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2023-5-2"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2023-02-29"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2023-03-01"), collations.SystemCollation),
			evalengine.NewLiteralString([]byte("2022-02-11"), collations.SystemCollation),
		},
	}

	wantResult := map[string][]vindexes.ActualTable{
		"t_user": {
			{ActualTableName: "t_user_0", Index: 0},
			{ActualTableName: "t_user_32", Index: 32},
			{ActualTableName: "t_user_59", Index: 59},
			{ActualTableName: "t_user_90", Index: 90},
			{ActualTableName: "t_user_91", Index: 91},
			{ActualTableName: "t_user_122", Index: 122},
			{ActualTableName: "t_user_151", Index: 151},
			{ActualTableName: "t_user_212", Index: 212},
			{ActualTableName: "t_user_243", Index: 243},
			{ActualTableName: "t_user_304", Index: 304},
			{ActualTableName: "t_user_365", Index: 365},
			{ActualTableName: "t_user_365", Index: 365},
		},
	}
	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][0]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-02-2"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][1]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-02-29"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][2]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-03-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][3]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-04-1"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][4]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-05-2"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][5]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-05-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][6]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-07-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][7]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-08-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][8]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-10-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][9]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2024-12-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][10]) {
		t.Errorf("find table routing error")
	}
	TableRouteParam.TableValues[0] = evalengine.NewLiteralString([]byte("2023-12-31"), collations.SystemCollation)
	result, err = TableRouteParam.findTableRoute(context.Background(), vc, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	if !reflect.DeepEqual(result["t_user"][0], wantResult["t_user"][11]) {
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
