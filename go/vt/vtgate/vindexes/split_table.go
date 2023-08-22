/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Vindex = (*SplitTableHashMod)(nil)

type SplitTableHashMod struct {
	name       string
	cost       int
	tableCount int
	columnType string
	columnVdx  map[int]Hashing
}

const (
	paramTableCount        = "table_count"
	paramColumnType        = "column_type"
	paramTableColumnVindex = "column_vindex"
	defaultTableVindex     = "hash"
)

// NewSplitTable creates a new SplitTableHashMod.
func NewSplitTable(name string, m map[string]string) (Vindex, error) {
	tableCount, err := getTableCount(m)
	if err != nil {
		return nil, err
	}
	columnType, err := getTableColumnType(m)
	if err != nil {
		return nil, err
	}
	columnVdx, vindexCost, err := getTableColumnVindex(m, tableCount)
	if err != nil {
		return nil, err
	}

	return &SplitTableHashMod{
		name:       name,
		cost:       vindexCost,
		tableCount: tableCount,
		columnVdx:  columnVdx,
		columnType: columnType,
	}, nil
}

func (m *SplitTableHashMod) String() string {
	return m.name
}

func (m *SplitTableHashMod) Cost() int {
	return m.cost
}

func (m *SplitTableHashMod) IsUnique() bool {
	return true
}

func (m *SplitTableHashMod) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (m *SplitTableHashMod) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.TableDestination, error) {
	out := make([]key.TableDestination, len(ids))
	for i, id := range ids {
		var num uint64
		var err error

		if id.IsSigned() {
			// This is ToUint64 with no check on negative values.
			str := id.ToString()
			var ival int64
			ival, err = strconv.ParseInt(str, 10, 64)
			num = uint64(ival)
		} else {
			num, err = evalengine.ToUint64(id)
		}

		if err != nil {
			h64 := New64()
			_, err = h64.Write([]byte(strings.ToLower(strings.TrimSpace(string(id.Raw())))))
			if err != nil {
				out[i] = key.TableDestinationNone{}
				continue
			}
			num = h64.Sum64()
		}

		out[i] = key.TableDestinationKeyspaceID(vhash(num))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (m *SplitTableHashMod) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := evalengine.ToUint64(ids[i])
		if err != nil {
			return nil, err
		}
		out[i] = bytes.Equal(vhash(num), ksids[i])
	}
	return out, nil
}

func (m *SplitTableHashMod) PartialVindex() bool {
	return true
}

func init() {
	Register("tableHashMod", NewSplitTable)
}

func getTableColumnVindex(m map[string]string, colCount int) (map[int]Hashing, int, error) {
	var colVdxs []string
	colVdxsStr, ok := m[paramColumnVindex]
	if ok {
		colVdxs = strings.Split(colVdxsStr, ",")
	}
	if len(colVdxs) > colCount {
		return nil, 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of vindex function provided are more than column count in the parameter '%s'", paramColumnVindex)
	}
	columnVdx := make(map[int]Hashing, colCount)
	vindexCost := 0
	for i := 0; i < colCount; i++ {
		selVdx := defaultVindex
		if len(colVdxs) > i {
			providedVdx := strings.TrimSpace(colVdxs[i])
			if providedVdx != "" {
				selVdx = providedVdx
			}
		}
		// TODO: reuse vindex. avoid creating same vindex.
		vdx, err := CreateVindex(selVdx, selVdx, m)
		if err != nil {
			return nil, 0, err
		}
		hashVdx, isHashVdx := vdx.(Hashing)
		if !isHashVdx || !vdx.IsUnique() || vdx.NeedsVCursor() {
			return nil, 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multicol vindex supports vindexes that exports hashing function, are unique and are non-lookup vindex, passed vindex '%s' is invalid", selVdx)
		}
		vindexCost = vindexCost + vdx.Cost()
		columnVdx[i] = hashVdx
	}
	return columnVdx, vindexCost, nil
}

func getTableColumnBytes(m map[string]string, colCount int) (map[int]int, error) {
	var colByteStr []string
	colBytesStr, ok := m[paramColumnBytes]
	if ok {
		colByteStr = strings.Split(colBytesStr, ",")
	}
	if len(colByteStr) > colCount {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of column bytes provided are more than column count in the parameter '%s'", paramColumnBytes)
	}
	// validate bytes count
	bytesUsed := 0
	columnBytes := make(map[int]int, colCount)
	for idx, byteStr := range colByteStr {
		if byteStr == "" {
			continue
		}
		colByte, err := strconv.Atoi(byteStr)
		if err != nil {
			return nil, err
		}
		bytesUsed = bytesUsed + colByte
		columnBytes[idx] = colByte
	}
	pendingCol := colCount - len(columnBytes)
	remainingBytes := 8 - bytesUsed
	if pendingCol > remainingBytes {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column bytes count exceeds the keyspace id length (total bytes count cannot exceed 8 bytes) in the parameter '%s'", paramColumnBytes)
	}
	if pendingCol <= 0 {
		return columnBytes, nil
	}
	for idx := 0; idx < colCount; idx++ {
		if _, defined := columnBytes[idx]; defined {
			continue
		}
		bytesToAssign := int(math.Ceil(float64(remainingBytes) / float64(pendingCol)))
		columnBytes[idx] = bytesToAssign
		remainingBytes = remainingBytes - bytesToAssign
		pendingCol--
	}
	return columnBytes, nil
}

func getTableCount(m map[string]string) (int, error) {
	tableCountStr, ok := m[paramTableCount]
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns not provided in the parameter '%s'", paramTableCount)
	}
	tableCount, err := strconv.Atoi(tableCountStr)
	if err != nil {
		return 0, err
	}
	if tableCount < 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns should be between 1 and 8 in the parameter '%s'", paramTableCount)
	}
	return tableCount, nil
}
func getTableColumnType(m map[string]string) (string, error) {
	tableCountStr, ok := m[paramColumnType]
	if !ok {
		return "null", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "number of columns not provided in the parameter '%s'", paramTableCount)
	}
	return tableCountStr, nil
}
