package vindexes

import (
	"context"
	"reflect"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

var rangeMMSingleColumn TableSingleColumn

func init() {
	hv, err := CreateVindex("split_table_range_mm", "split_table_range_mm", map[string]string{"Table": "t", "Column": "c"})
	if err != nil {
		panic(err)
	}
	rangeMMSingleColumn = hv.(TableSingleColumn)
}
func TestSplitTableRangeMM_Map(t *testing.T) {
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
			fields: fields{name: "split_table_range_mm"},
			args: args{ctx: context.Background(),
				vcursor: nil,
				ids: []sqltypes.Value{sqltypes.NewTimestamp("202311081801"),
					sqltypes.NewDate("2023-11-11 11:12:11"),
					sqltypes.NewDate("2023/11/11"),
					sqltypes.NewDate("2023-01-11"),
					sqltypes.NewDate("2023-1-11"),
					sqltypes.NewDate("2023/1/11"),
					sqltypes.NewDate("2023/01/11"),
					sqltypes.NewDatetime("2023-12-31 11:23:59"),
					sqltypes.NewDatetime("2023-12-31 23:59:59.9999999"),
					sqltypes.NewDatetime("2024-01-01 00:00:00"),
					sqltypes.NewDatetime("2023/11/11 12:35:00")}},
			want: []TableDestination{TableDestinationRangeMM(11),
				TableDestinationRangeMM(11),
				TableDestinationRangeMM(11),
				TableDestinationRangeMM(01),
				TableDestinationRangeMM(01),
				TableDestinationRangeMM(01),
				TableDestinationRangeMM(01),
				TableDestinationRangeMM(12),
				TableDestinationRangeMM(12),
				TableDestinationRangeMM(01),
				TableDestinationRangeMM(11)},
			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &SplitTableRangeMM{
				name: tt.fields.name,
			}
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
