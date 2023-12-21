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
	"github.com/cespare/xxhash/v2"
	"vitess.io/vitess/go/sqltypes"
)

var _ Vindex = (*SplitTableXXHash)(nil)

type SplitTableXXHash struct {
	name string
}

// NewSplitTableXXHash creates a new SplitTableHashMod.
func NewSplitTableXXHash(name string, m map[string]string) (Vindex, error) {
	return &SplitTableXXHash{
		name: name,
	}, nil
}

func (m *SplitTableXXHash) String() string {
	return m.name
}

func (m *SplitTableXXHash) Cost() int {
	return 1
}

func (m *SplitTableXXHash) IsUnique() bool {
	return true
}

func (m *SplitTableXXHash) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (m *SplitTableXXHash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]TableDestination, error) {
	out := make([]TableDestination, len(ids))
	for i, id := range ids {
		out[i] = TableDestinationUint64KeyspaceID(xxhash.Sum64(id.Raw()))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (m *SplitTableXXHash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		out[i] = bytes.Equal(vXXHash(ids[i].Raw()), ksids[i])
	}
	return out, nil
}
func init() {
	Register("split_table_xxhash", NewSplitTableXXHash)
}
