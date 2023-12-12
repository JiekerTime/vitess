package split_table

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestShowTable(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.ExecWithColumnCompareAndNotEmpty("show tables")
	mcmp.ExecAndNotEmpty("show tables like 't_user%'")
	mcmp.ExecAndNotEmpty("show tables like '%_1%'")
	mcmp.ExecAndNotEmpty("show tables like '%_2'")
}

func TestShowColumn(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("show COLUMNS FROM t_user")
}

func TestShowCreateTable(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	got := fmt.Sprintf("%v", utils.Exec(t, mcmp.VtConn, "show create table t_user_0").Rows)
	utils.AssertMatches(t, mcmp.VtConn, "show create table t_user", got)
}

func TestDesc(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("desc t_user")
}
