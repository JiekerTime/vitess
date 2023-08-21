package engine

import (
	"context"
	"reflect"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/tableindexes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestTableRoutingParameters_findTableRoute(t *testing.T) {
	type fields struct {
		Opcode     TableOpCode
		Vindex     vindexes.Vindex
		LogicTable tableindexes.LogicTableConfig
		Values     []evalengine.Expr
	}
	type args struct {
		ctx      context.Context
		vcursor  VCursor
		bindVars map[string]*querypb.BindVariable
	}
	params := map[string]string{"table_count": "2", "column_type": "int32"}
	hash, _ := vindexes.CreateVindex("tableHashMod", "tableHashMod", params)
	logicTable := tableindexes.LogicTableConfig{LogicTableName: "t_user",
		ActualTableList:  []tableindexes.ActualTable{{"t_user_1", 0}, {"t_user_2", 1}},
		TableIndexColumn: tableindexes.Column{"id", querypb.Type_INT64},
		ActualTableCount: 2,
	}
	//vc := &loggingVCursor{
	//	shards:  []string{"0"},
	//	results: []*sqltypes.Result{defaultSelectResult},
	//}
	bindVars := map[string]*querypb.BindVariable{
		"id":   sqltypes.Int64BindVariable(1),
		"name": sqltypes.Int64BindVariable(2),
	}
	values := []evalengine.Expr{
		evalengine.NewLiteralInt(3),
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{name: "findTableRoute",
			fields: fields{Opcode: TableEqualUnique,
				Vindex:     hash,
				LogicTable: logicTable,
				Values:     values,
			},
			args: args{ctx: context.Background(),
				vcursor:  nil,
				bindVars: bindVars},
			want: []string{"t_user_1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rp := &TableRoutingParameters{
				Opcode:     tt.fields.Opcode,
				Vindex:     tt.fields.Vindex,
				LogicTable: tt.fields.LogicTable,
				Values:     tt.fields.Values,
			}
			got, err := rp.findTableRoute(tt.args.ctx, tt.args.vcursor, tt.args.bindVars)
			if (err != nil) != tt.wantErr {
				t.Errorf("findTableRoute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findTableRoute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
