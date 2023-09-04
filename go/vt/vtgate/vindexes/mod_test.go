package vindexes

import (
	"context"
	"reflect"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func TestMod_Map(t *testing.T) {

	type args struct {
		ctx    context.Context
		cursor VCursor
		ids    []sqltypes.Value
	}

	tests := []struct {
		name    string
		args    args
		want    []key.Destination
		wantErr bool
	}{
		{"mod", args{context.Background(), nil, []sqltypes.Value{sqltypes.NewUint64(111)}}, []key.Destination{
			key.DestinationKeyspaceID([]byte{0, 0, 0, 0, 0, 0, 0, 111}),
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vind, err := CreateVindex("mod", "mod", map[string]string{
				"mod_size": "4",
			})
			vindMod := vind.(SingleColumn)
			got, err := vindMod.Map(tt.args.ctx, tt.args.cursor, tt.args.ids)
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
