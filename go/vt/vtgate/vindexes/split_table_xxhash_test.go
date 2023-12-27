package vindexes

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestSplitTableXXHash_Map(t *testing.T) {
	type fields struct {
		name string
	}
	type args struct {
		ctx     context.Context
		vcursor VCursor
		ids     []sqltypes.Value
	}
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(17807180282401718102))
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []TableDestination
		wantErr bool
	}{
		{"split_table_xxhash",
			fields{"split_table_xxhash"},
			args{context.Background(), nil,
				[]sqltypes.Value{sqltypes.NewVarBinary("TEst"),
					sqltypes.NewUint64(12300),
					sqltypes.NewVarChar("12300"),
					sqltypes.NewUint64(0x14),
					sqltypes.NewVarChar("0x14"),
					sqltypes.NewDate("2023-09-08 15:10:52"),
					sqltypes.NewTimestamp("1694157052"),
					sqltypes.NewDatetime("2023-09-08 15:10:52"),
					sqltypes.NewInt64(-7148613445350202178),
					sqltypes.NewVarChar("-7148613445350202178")}}, []TableDestination{TableDestinationUint64KeyspaceID(2975196391887483431)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &SplitTableXXHash{
				name: tt.fields.name,
			}
			got, err := m.Map(tt.args.ctx, tt.args.vcursor, tt.args.ids)
			if (err != nil) != tt.wantErr {
				t.Errorf("Map() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			tables := &LogicTableConfig{
				LogicTableName: "t_user",
				TableCount:     256,
			}
			for _, g := range got {
				g.(TableDestinationUint64KeyspaceID).Resolve(tables, func(table int) error {
					println(table)
					return nil
				})
			}

		})
	}
}

func BenchmarkSplitTableXXHash(b *testing.B) {

	type fields struct {
		name string
	}

	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(17807180282401718102))
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []TableDestination
		wantErr bool
	}{
		{"splitTableXXHash",
			fields{"splitTableXXHash"},
			args{context.Background(),
				nil,
				[]sqltypes.Value{sqltypes.NewVarBinary("TEst")}},
			[]TableDestination{TableDestinationUint64KeyspaceID(123456789)},
			false},
	}

	for _, benchSize := range []struct {
		name string
		n    int
	}{
		{"8B", 8},
		{"32B", 32},
		{"64B", 64},
		{"512B", 512},
		{"1KB", 1e3},
		{"4KB", 4e3},
	} {
		input := make([]byte, benchSize.n)
		for i := range input {
			input[i] = byte(i)
		}

		name := fmt.Sprintf("split_table_xxhash,direct,bytes,n=%s", benchSize.name)
		b.Run(name, func(b *testing.B) {
			benchmarkSplitTableXXHashBytes(b, tests[0].args)
		})

	}
}

var sinXXHash []byte

type args struct {
	ctx     context.Context
	vcursor VCursor
	ids     []sqltypes.Value
}

func benchmarkSplitTableXXHashBytes(b *testing.B, tt args) {
	b.SetBytes(int64(len(tt.ids)))
	for i := 0; i < b.N; i++ {
		m := &SplitTableXXHash{
			name: "split_table_xxhash",
		}
		got, _ := m.Map(tt.ctx, tt.vcursor, tt.ids)
		tables := &LogicTableConfig{
			LogicTableName: "t_user",
			TableCount:     5,
		}
		got[0].(TableDestinationUint64KeyspaceID).Resolve(tables, func(table int) error {
			return nil
		})
	}
}

func BenchmarkSplitTable1XXHash(b *testing.B) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(17807180282401718102))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyspaceID := vXXHash(bytes)
		a := binary.BigEndian.Uint64(keyspaceID) % 5
		if false {
			println(a)
		}
	}
}

func BenchmarkSplitTableXXHashNew(b *testing.B) {
	type fields struct {
		name string
	}

	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(17807180282401718102))
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []TableDestination
		wantErr bool
	}{
		{"split_table_xxhash",
			fields{"split_table_xxhash"},
			args{context.Background(),
				nil,
				[]sqltypes.Value{sqltypes.NewUint64(123456789)}},
			[]TableDestination{TableDestinationUint64KeyspaceID(123456789)},
			false},
	}
	tables := &LogicTableConfig{
		LogicTableName: "t_user",
		TableCount:     5,
	}
	m := &SplitTableXXHash{
		name: "split_table_xxhash",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		got, _ := m.Map(tests[0].args.ctx, tests[0].args.vcursor, tests[0].args.ids)

		got[0].(TableDestinationUint64KeyspaceID).Resolve(tables, func(table int) error {
			return nil
		})
	}
}
