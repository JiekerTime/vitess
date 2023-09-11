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
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Vindex = (*SplitTableHash)(nil)

type SplitTableHash struct {
	name string
}

// NewSplitTable creates a new SplitTableHashMod.
func NewSplitTable(name string, m map[string]string) (Vindex, error) {
	return &SplitTableHash{
		name: name,
	}, nil
}

func (m *SplitTableHash) String() string {
	return m.name
}

func (m *SplitTableHash) Cost() int {
	return 1
}

func (m *SplitTableHash) IsUnique() bool {
	return true
}

func (m *SplitTableHash) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (m *SplitTableHash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.TableDestination, error) {
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
func (m *SplitTableHash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
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
func init() {
	Register("splitTableHashMod", NewSplitTable)
}
