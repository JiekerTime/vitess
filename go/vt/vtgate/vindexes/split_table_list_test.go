package vindexes

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"vitess.io/vitess/go/sqltypes"
)

func TestNewSplitTableList(t *testing.T) {
	type args struct {
		name string
		m    map[string]string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "TestNewSplitTableList", args: args{name: "split_table_list", m: map[string]string{"json": `{"east":["1","2","3"],"north":["4","5","6"],"south":["7","8","9"],"west":["10","11","12"]}`}}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSplitTableList(tt.args.name, tt.args.m)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSplitTableList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

var listSingleColumn TableSingleColumn

func init() {
	hv, err := CreateVindex("split_table_list", "split_table_list", map[string]string{"json": `{"east":["1","2","3","202311081801"],"north":["4","5","6","2023-11-11 11:12:11"],"south":["7","8","9","2023/11/11"],"west":["10","11","12","2023-01-11","stardb"]}`})
	if err != nil {
		return
	}
	listSingleColumn = hv.(TableSingleColumn)
}

func TestSplitTableList_Map(t *testing.T) {
	type fields struct {
		name string
	}
	type args struct {
		ctx     context.Context
		vcursor VCursor
		ids     []sqltypes.Value
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []TableDestination
		wantErr bool
	}{
		{name: "TestSplitTableRangeMM_Map",
			fields: fields{name: "split_table_list"},
			args: args{ctx: context.Background(),
				vcursor: nil,
				ids: []sqltypes.Value{sqltypes.NewTimestamp("202311081801"),
					sqltypes.NewDate("2023-11-11 11:12:11"),
					sqltypes.NewDate("2023/11/11"),
					sqltypes.NewDate("2023-01-11"),
					sqltypes.NewInt32(1),
					sqltypes.NewVarChar("stardb"),
				}},
			want: []TableDestination{TableDestinationList("202311081801"),
				TableDestinationList("2023-11-11 11:12:11"),
				TableDestinationList("2023/11/11"),
				TableDestinationList("2023-01-11"),
				TableDestinationList(strconv.Itoa(1)),
				TableDestinationList("stardb"),
			},
			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := listSingleColumn
			got, err := m.Map(tt.args.ctx, tt.args.vcursor, tt.args.ids)
			if (err != nil) != tt.wantErr {
				t.Errorf("Map() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() got = %v, want %v", got, tt.want)
			}
		})
	}
}
