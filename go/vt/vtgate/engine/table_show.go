package engine

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*TableShow)(nil)

// TableShow is a primitive that renames the fields
type TableShow struct {
	rows   []sqltypes.Row
	Input  Primitive
	Like   string
	Tables map[string]*vindexes.LogicTableConfig
	//todo
	//Filter
	noTxNeeded
}

// NewTableShow creates a new rename field
func NewTableShow(input Primitive, tables map[string]*vindexes.LogicTableConfig, like string) *TableShow {
	return &TableShow{
		Input:  input,
		Tables: tables,
		Like:   like,
	}
}

// RouteType implements the primitive interface
func (r *TableShow) RouteType() string {
	return r.Input.RouteType()
}

// GetKeyspaceName implements the primitive interface
func (r *TableShow) GetKeyspaceName() string {
	return r.Input.GetKeyspaceName()
}

// GetTableName implements the primitive interface
func (r *TableShow) GetTableName() string {
	return r.Input.GetTableName()
}

// TryExecute implements the Primitive interface
func (r *TableShow) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(ctx, r.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	r.processFilterTableForSplitTable()
	r.addSplitTableName(qr)
	return qr, nil
}

func (r *TableShow) processFilterTableForSplitTable() {
	tablesResult := make([]sqltypes.Row, 0)
	if r.Like != "" {
		regexPattern := convertPatternToRegex(r.Like)
		for _, t := range r.Tables {
			item := t.LogicTableName
			if regexPattern.MatchString(item) {
				tablesResult = append(tablesResult, sqltypes.Row{sqltypes.NewVarChar(item)})
			}
		}
		r.rows = tablesResult
		return
	}
	for _, t := range r.Tables {
		tablesResult = append(tablesResult, sqltypes.Row{sqltypes.NewVarChar(t.LogicTableName)})
	}
	r.rows = tablesResult

}

func (r *TableShow) addSplitTableName(qr *sqltypes.Result) {
	qr.Rows = append(qr.Rows, r.rows...)

	sort.Slice(qr.Rows, func(i, j int) bool {
		return bytes.Compare(qr.Rows[i][0].Raw(), qr.Rows[j][0].Raw()) < 0
	})
}

// TryStreamExecute implements the Primitive interface
func (r *TableShow) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	innerCallback := callback
	callback = func(result *sqltypes.Result) error {
		r.processFilterTableForSplitTable()
		r.addSplitTableName(result)
		return innerCallback(result)
	}
	return vcursor.StreamExecutePrimitive(ctx, r.Input, bindVars, wantfields, callback)
}

// GetFields implements the primitive interface
func (r *TableShow) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := r.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// Inputs implements the primitive interface
func (r *TableShow) Inputs() []Primitive {
	return []Primitive{r.Input}
}

func convertPatternToRegex(pattern string) *regexp.Regexp {
	regexPattern := strings.ReplaceAll(pattern, ".", "\\.")
	regexPattern = strings.ReplaceAll(regexPattern, "\\%", "^^^")
	regexPattern = strings.ReplaceAll(regexPattern, "%", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "^^^", "%")
	regexPattern = fmt.Sprintf("^%s$", regexPattern)
	return regexp.MustCompile(regexPattern)
}

// description implements the primitive interface
func (r *TableShow) description() PrimitiveDescription {
	r.processFilterTableForSplitTable()
	sortRows := r.rows
	sort.Slice(sortRows, func(i, j int) bool {
		return bytes.Compare(sortRows[i][0].Raw(), sortRows[j][0].Raw()) < 0
	})
	return PrimitiveDescription{
		OperatorType: "TableShow",
		Other: map[string]any{
			"rows": sortRows,
			"like": r.Like,
		},
	}
}
