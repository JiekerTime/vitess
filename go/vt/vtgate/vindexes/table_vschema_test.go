package vindexes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_logicToActualTableMM(t *testing.T) {
	type args struct {
		logicTableName string
		tableIndex     int
		tableIndexType string
		table          *LogicTableConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []ActualTable
		wantErr bool
	}{
		{name: "Test_logicToActualTable",
			args: args{logicTableName: "t_user", tableIndex: 0, tableIndexType: TableIndexTypeRangeMM, table: &LogicTableConfig{TableCount: 12}},
			want: []ActualTable{{ActualTableName: "t_user_0", Index: 0},
				{ActualTableName: "t_user_1", Index: 1},
				{ActualTableName: "t_user_2", Index: 2},
				{ActualTableName: "t_user_3", Index: 3},
				{ActualTableName: "t_user_4", Index: 4},
				{ActualTableName: "t_user_5", Index: 5},
				{ActualTableName: "t_user_6", Index: 6},
				{ActualTableName: "t_user_7", Index: 7},
				{ActualTableName: "t_user_8", Index: 8},
				{ActualTableName: "t_user_9", Index: 9},
				{ActualTableName: "t_user_10", Index: 10},
				{ActualTableName: "t_user_11", Index: 11},
			},

			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := int32(0); i < tt.args.table.TableCount; i++ {
				if _, err := LogicToActualTable(tt.args.logicTableName, int(i), TableIndexTypeRangeMM, tt.args.table); (err != nil) != tt.wantErr {
					t.Errorf("logicToActualTable() error = %v, wantErr %v", err, tt.wantErr)
				}
				assert.Equal(t, tt.want[i], tt.args.table.ActualTableList[i])
			}

		})
	}
}
func Test_logicToActualTableMMDD(t *testing.T) {
	type args struct {
		logicTableName string
		tableIndex     int
		tableIndexType string
		table          *LogicTableConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []ActualTable
		wantErr bool
	}{
		{name: "Test_logicToActualTable",
			args: args{logicTableName: "t_user", tableIndex: 0, tableIndexType: TableIndexTypeRangeMMDD, table: &LogicTableConfig{TableCount: 366}},
			want: []ActualTable{
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
			},

			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for idx, i := range tt.want {
				if _, err := LogicToActualTable(tt.args.logicTableName, i.Index, TableIndexTypeRangeMMDD, tt.args.table); (err != nil) != tt.wantErr {
					t.Errorf("logicToActualTable() error = %v, wantErr %v", err, tt.wantErr)
				}
				assert.Equal(t, tt.want[idx], tt.args.table.ActualTableList[idx])
			}

		})
	}
}

func Test_logicToActualTableList(t *testing.T) {
	type args struct {
		logicTableName string
		tableIndex     int
		tableIndexType string
		table          *LogicTableConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []ActualTable
		wantErr bool
	}{
		{name: "Test_logicToActualTable",
			args: args{logicTableName: "t_user", tableIndex: 0, tableIndexType: TableIndexTypeRangeMMDD, table: &LogicTableConfig{TableCount: 366, Params: map[string]*TableParams{
				"0":  {"east", 0},
				"1":  {"east", 0},
				"2":  {"east", 0},
				"3":  {"east", 0},
				"4":  {"south", 1},
				"5":  {"south", 1},
				"6":  {"south", 1},
				"7":  {"west", 2},
				"8":  {"west", 2},
				"9":  {"north", 3},
				"10": {"north", 3},
				"11": {"north", 3},
			}}},
			want: []ActualTable{
				{ActualTableName: "t_user_east", Index: 0},
				{ActualTableName: "t_user_south", Index: 1},
				{ActualTableName: "t_user_west", Index: 2},
				{ActualTableName: "t_user_north", Index: 3},
			},

			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for idx, i := range tt.want {
				if _, err := LogicToActualTable(tt.args.logicTableName, i.Index, TableIndexTypeList, tt.args.table); (err != nil) != tt.wantErr {
					t.Errorf("logicToActualTable() error = %v, wantErr %v", err, tt.wantErr)
				}
				assert.Equal(t, tt.want[idx], tt.args.table.ActualTableList[idx])
			}

		})
	}
}
