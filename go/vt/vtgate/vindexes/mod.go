/*
Copyright 2017 Google Inc.

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
	"encoding/binary"
	"fmt"
	"strconv"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ Vindex     = (*Mod)(nil)
	_ Reversible = (*Mod)(nil)
)

// Mod defines vindex that hashes an int64 to a KeyspaceId
// It's Unique, Reversible and Functional.
type Mod struct {
	name    string
	modSize uint64
}

const (
	paramModSize = "mod_size"
)

// NewMod creates a new Mod.
func NewMod(name string, para map[string]string) (Vindex, error) {
	modSize, ok := para[paramModSize]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "mod size not found ")
	}
	num, err := strconv.ParseUint(modSize, 10, 64)
	if err != nil {
		return nil, err
	}
	return &Mod{name: name, modSize: num}, nil
}

// String returns the name of the vindex.
func (vind *Mod) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *Mod) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *Mod) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (vind *Mod) IsFunctional() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (vind *Mod) Map(ctx context.Context, cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		var num uint64
		var err error

		if id.IsSigned() {
			// This is ToUint64 with no check on negative values.
			str := id.ToString()
			var iVal int64
			iVal, err = strconv.ParseInt(str, 10, 64)
			num = uint64(iVal)
		} else {
			num, err = evalengine.ToUint64(id)
		}

		if err != nil {
			out[i] = key.DestinationNone{}
			return nil, vterrors.Errorf(vtrpc.Code_OUT_OF_RANGE, "column data %s out of range for modulo operation ", id.String())
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, num%vind.modSize)
		out[i] = key.DestinationModKeyspaceID(buf)
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Mod) Verify(ctx context.Context, _ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := evalengine.ToUint64(ids[i])
		if err != nil {
			return nil, fmt.Errorf("hash.Verify: %v", err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, num)
		out[i] = bytes.Equal(buf, ksids[i])
	}
	return out, nil
}

// ReverseMap returns the ids from ksids.
func (vind *Mod) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	reverseIds := make([]sqltypes.Value, 0, len(ksids))
	for _, keyspaceID := range ksids {
		reverseIds = append(reverseIds, sqltypes.NewUint64(binary.BigEndian.Uint64(keyspaceID)))
	}
	return reverseIds, nil
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *Mod) NeedsVCursor() bool {
	return false
}

func init() {
	Register("mod", NewMod)
}
