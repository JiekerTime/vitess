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

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Vindex = (*SplitTableRangeMM)(nil)

type SplitTableRangeMM struct {
	name string
}

// NewSplitTableRangeMM creates a new split table time range mm.
func NewSplitTableRangeMM(name string, m map[string]string) (Vindex, error) {
	return &SplitTableRangeMM{
		name: name,
	}, nil
}

func (m *SplitTableRangeMM) String() string {
	return m.name
}

func (m *SplitTableRangeMM) Cost() int {
	return 1
}

func (m *SplitTableRangeMM) IsUnique() bool {
	return true
}

func (m *SplitTableRangeMM) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (m *SplitTableRangeMM) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]TableDestination, error) {
	out := make([]TableDestination, len(ids))
	for i, id := range ids {
		switch id.Type() {
		case sqltypes.Date:
			date, _ := datetime.ParseDate(id.ToString())
			if date.IsZero() {
				dateTime, _, _ := datetime.ParseDateTime(id.ToString(), -1)
				if dateTime.IsZero() {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect %s value: '%s'", id.Type().String(), id.ToString())
				}
				out[i] = TableDestinationRangeMM(dateTime.Date.Month())
			} else {
				out[i] = TableDestinationRangeMM(date.Month())
			}

		case sqltypes.Datetime, sqltypes.Timestamp, sqltypes.VarChar:
			dateTime, _, _ := datetime.ParseDateTime(id.ToString(), -1)
			if dateTime.IsZero() {
				date, _ := datetime.ParseDate(id.ToString())
				if date.IsZero() {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect %s value: '%s'", id.Type().String(), id.ToString())
				}
				out[i] = TableDestinationRangeMM(date.Month())
			} else {
				out[i] = TableDestinationRangeMM(dateTime.Date.Month())
			}
		default:
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect %s value: '%s'", id.Type().String(), id.ToString())
		}

	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (m *SplitTableRangeMM) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		date, _ := datetime.ParseDate(ids[i].ToString())
		if date.IsZero() {
			dateTime, _, _ := datetime.ParseDateTime(ids[i].ToString(), -1)
			if dateTime.IsZero() {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect %s value: '%s'", ids[i].ToString(), ids[i].ToString())
			}

			out[i] = bytes.Equal([]byte(strconv.Itoa(dateTime.Date.Month())), ksids[i])
		} else {
			out[i] = bytes.Equal([]byte(strconv.Itoa(date.Month())), ksids[i])
		}
	}
	return out, nil
}
func init() {
	Register("split_table_range_mm", NewSplitTableRangeMM)
}
