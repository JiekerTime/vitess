package planbuilder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	oprewriters "vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
)

func TestSplitTablePlan(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/table_schema.json", true),
		TabletType_:   topodatapb.TabletType_PRIMARY,
		SysVarEnabled: true,
		Version:       Gen4,
		Keyspace: &vindexes.Keyspace{
			Name:    "user",
			Sharded: true,
		},
	}
	output := makeTestOutput(t)
	testTableFile(t, "table_ddl_cases.json", output, vschema, false)
	testTableFile(t, "table_aggr_cases.json", output, vschema, false)
	testTableFile(t, "table_dml_cases.json", output, vschema, false)
	testTableFile(t, "table_from_cases.json", output, vschema, false)
	testTableFile(t, "table_filter_cases.json", output, vschema, false)
	testTableFile(t, "table_postprocess_cases.json", output, vschema, false)
	testTableFile(t, "table_select_cases.json", output, vschema, false)
	testTableFile(t, "table_subquery_select_cases.json", output, vschema, false)
	testTableFile(t, "table_aggr_subquery_cases.json", output, vschema, false)
	testTableFile(t, "table_filter_subquery_cases.json", output, vschema, false)
	testTableFile(t, "table_postprocess_subquery_cases.json", output, vschema, false)
	testTableFile(t, "table_from_subquery_cases.json", output, vschema, false)
	testTableFile(t, "table_memory_sort_cases.json", output, vschema, false)
	testTableFile(t, "table_issue.json", output, vschema, false)
	testTableFile(t, "table_show_cases.json", output, vschema, false)
	testTableFile(t, "table_union_cases.json", output, vschema, false)
	testTableFile(t, "table_vexplain_cases.json", output, vschema, false)
	testTableFile(t, "table_explain_cases.json", output, vschema, false)
	testTableFile(t, "table_wireup_cases.json", output, vschema, false)
}

func TestSplitTableOne(t *testing.T) {
	oprewriters.DebugOperatorTree = true
	vschema := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/table_schema.json", true),
		TabletType_:   topodatapb.TabletType_PRIMARY,
		SysVarEnabled: true,
		Version:       Gen4,
	}
	output := makeTestOutput(t)
	testTableFile(t, "table_onecase.json", output, vschema, false)
}

// TestPrintSplitTable 用于生成集成测试用例
func TestPrintSplitTable(t *testing.T) {
	printTableFile(t, "table_aggr_cases.json")
}

func testTableFile(t *testing.T, filename, tempDir string, vschema *vschemawrapper.VSchemaWrapper, render bool) {
	opts := jsondiff.DefaultConsoleOptions()

	t.Run(filename, func(t *testing.T) {
		var expected []planTest
		for _, tcase := range readJSONTests(filename) {
			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}
			if tcase.Query == "" {
				continue
			}
			current := planTest{
				Comment: testName,
				Query:   tcase.Query,
			}
			out := getPlanOutput(tcase, vschema, render)

			// our expectation for the planner on the query is one of three
			// - produces same plan as expected
			// - produces a different plan than expected
			// - fails to produce a plan
			t.Run(testName, func(t *testing.T) {
				compare, s := jsondiff.Compare(tcase.Plan, []byte(out), &opts)
				if compare != jsondiff.FullMatch {
					t.Errorf("%s\nDiff:\n%s\n[%s] \n[%s]", filename, s, tcase.Plan, out)
				}
				current.Plan = []byte(out)
			})
			expected = append(expected, current)
		}
		if tempDir != "" {
			name := strings.TrimSuffix(filename, filepath.Ext(filename))
			name = filepath.Join(tempDir, name+".json")
			file, err := os.Create(name)
			require.NoError(t, err)
			enc := json.NewEncoder(file)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			err = enc.Encode(expected)
			if err != nil {
				require.NoError(t, err)
			}
		}
	})
}

func printTableFile(t *testing.T, filename string) {
	t.Run(filename, func(t *testing.T) {
		fmt.Println()
		for _, tcase := range readJSONTests(filename) {
			if tcase.Query == "" {
				continue
			}
			fmt.Println("// " + tcase.Comment)
			fmt.Println("mcmp.ExecWithColumnCompareAndNotEmpty(\"" + tcase.Query + "\")")
		}
		fmt.Println()
	})
}
