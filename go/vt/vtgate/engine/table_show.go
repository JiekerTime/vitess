package engine

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*TableShow)(nil)

// TableShow is a primitive that renames the fields
type TableShow struct {
	Input       Primitive
	Like        string
	SplitTables map[string]*vindexes.LogicTableConfig
	ShardTables map[string]*vindexes.Table
	Collation   collations.ID
	//todo
	//Filter
	noTxNeeded
}

// NewTableShow creates a new rename field
func NewTableShow(input Primitive, splitTables map[string]*vindexes.LogicTableConfig, shardTables map[string]*vindexes.Table, like string, collation collations.ID) *TableShow {
	return &TableShow{
		Input:       input,
		SplitTables: splitTables,
		ShardTables: shardTables,
		Like:        like,
		Collation:   collation,
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
	input, err := vcursor.ExecutePrimitive(ctx, r.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	out := &sqltypes.Result{
		Fields: input.Fields,
		Rows:   make([][]sqltypes.Value, 0, len(input.Rows)),
	}
	// filter seq and ActualTables
	ignoreTables := r.buildIgnoreTables()
	r.filterIgnoreTables(input, ignoreTables, out)
	// add logicTable Names
	logicTableNames := r.processSplitTable()
	out.Rows = append(out.Rows, logicTableNames...)
	// sort
	sort.Slice(out.Rows, func(i, j int) bool {
		return bytes.Compare(out.Rows[i][0].Raw(), out.Rows[j][0].Raw()) < 0
	})
	return out, nil
}

func (r *TableShow) filterIgnoreTables(input *sqltypes.Result, ignoreTables []sqltypes.Value, out *sqltypes.Result) {
	for _, row := range input.Rows {
		var kontinue bool

		for _, table := range ignoreTables {
			cmp, err := evalengine.NullsafeCompare(row[0], table, r.Collation)
			if err != nil {
				continue
			}
			if cmp == 0 {
				kontinue = true
			}
		}
		if kontinue {
			continue
		}
		out.Rows = append(out.Rows, row)
	}
}

func (r *TableShow) buildIgnoreTables() []sqltypes.Value {
	var ignoreTables []sqltypes.Value
	for _, table := range r.ShardTables {
		if table.AutoIncrement != nil {
			ignoreTables = append(ignoreTables, sqltypes.NewVarChar(table.AutoIncrement.Sequence.Name.String()))
		}
	}
	for _, table := range r.SplitTables {
		for _, actualTable := range table.ActualTableList {
			ignoreTables = append(ignoreTables, sqltypes.NewVarChar(actualTable.ActualTableName))
		}
	}
	return ignoreTables
}

func (r *TableShow) processSplitTable() []sqltypes.Row {
	var logicTableNames []sqltypes.Row
	if r.Like != "" {
		regexPattern := convertPatternToRegex(r.Like)
		logicTableNames = make([]sqltypes.Row, 0)
		for _, t := range r.SplitTables {
			item := t.LogicTableName
			if regexPattern.MatchString(item) {
				logicTableNames = append(logicTableNames, sqltypes.Row{sqltypes.NewVarChar(item)})
			}
		}
		return logicTableNames
	}
	logicTableNames = make([]sqltypes.Row, 0, len(r.SplitTables))
	for _, t := range r.SplitTables {
		logicTableNames = append(logicTableNames, sqltypes.Row{sqltypes.NewVarChar(t.LogicTableName)})
	}
	return logicTableNames
}

// TryStreamExecute implements the Primitive interface
func (r *TableShow) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := r.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
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
func (r *TableShow) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{r.Input}, nil
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
	return PrimitiveDescription{
		OperatorType: "TableShow",
		Other: map[string]any{
			"like": r.Like,
		},
	}
}
