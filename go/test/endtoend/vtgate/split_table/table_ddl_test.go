package split_table

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"testing"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestTableDDL Split Table DDL
func TestTableDDL(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	qr := mcmp.ExecSplitTableDDL("drop table t_music")
	mcmp.DropTableResultCheck(qr)

	mcmp.ExecVExplainForNotExistTable("vexplain queries select * from t_music")

	mcmp.ExecSplitTableDDL("CREATE TABLE `t_music`\n(\n    `id`      bigint(20) NOT NULL AUTO_INCREMENT,\n    `user_id` bigint(20) NOT NULL,\n    `col`     varchar(100) DEFAULT NULL,\n    `a`       int(16)      DEFAULT NULL,\n    `bar`     int          DEFAULT NULL,\n    `foo`     varchar(16)  DEFAULT NULL,\n    `genre`   varchar(16)  DEFAULT NULL,\n    PRIMARY KEY (`id`)\n) ENGINE = InnoDB\n  AUTO_INCREMENT = 1\n  DEFAULT CHARSET = utf8mb4;")

	expectResult := [][]string{{"user", "-80", "select id, user_id, col, a, bar, foo, genre from t_music_1"}, {"user", "-80", "select id, user_id, col, a, bar, foo, genre from t_music_0"},
		{"user", "80-", "select id, user_id, col, a, bar, foo, genre from t_music_1"}, {"user", "80-", "select id, user_id, col, a, bar, foo, genre from t_music_0"}}
	ExecVExplainForSplitTable("vexplain queries select * from t_music", expectResult, &mcmp)
}

func ExecVExplainForSplitTable(query string, expectResult [][]string, mcmp *utils.MySQLCompare) *sqltypes.Result {
	mcmp.GetTest().Helper()
	qrs, err := mcmp.VtConn.ExecuteFetch(query, 100000, true)
	assert.NoError(mcmp.GetTest(), err, query)
	require.Equal(mcmp.GetTest(), int(4), len(qrs.Rows))

	sort.Slice(qrs.Rows, func(i, j int) bool {
		return strings.Compare(qrs.Rows[i][2].RawStr(), qrs.Rows[j][2].RawStr()) <= 0
	})

	assert.Equal(mcmp.GetTest(), len(qrs.Rows), len(expectResult))
	for index, row := range qrs.Rows {
		keyspace := row[1].RawStr()
		shard := row[2].RawStr()
		query := row[3].RawStr()
		assert.Equal(mcmp.GetTest(), keyspace, expectResult[index][0])
		assert.Equal(mcmp.GetTest(), shard, expectResult[index][1])
		assert.Equal(mcmp.GetTest(), query, expectResult[index][2])
	}
	return nil
}

func TestTableTruncateDDL(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,  '3',    'aaa', 1, false, 1, 2, 3, 100, 200, 'abc')")
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.* from t_user  t_user")

	mcmp.Exec("truncate table t_user")
	mcmp.ExecWithColumnCompare("select t_user.* from t_user  t_user")
}

func TestTableAlterDDL(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.ExecNoCompare("show create table t_user")

	mcmp.ExecNoCompare("alter table t_user add column memo varchar(256) default null comment '备注'")

	time.Sleep(5 * time.Second)
	mcmp.Exec("select memo from t_user")
}
