package split_table

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func getSeqResult(t *testing.T, mcmp utils.MySQLCompare) (int64, error) {
	seq := utils.Exec(t, mcmp.VtConn, "select next_id from user.t_seq where id = 0;")
	seqResult, err := evalengine.ToInt64(seq.Rows[0][0])
	if err != nil {
		t.Errorf("get sequence err %v", err)
		return 0, err
	}
	return seqResult, nil
}
func TestLastInsertId(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	result, _ := getSeqResult(t, mcmp)
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bit) values (1, 1, 1, false),(2, 2, 2, false)")
	//插入多个值的时候LAST_INSERT_ID()返回的是最小的
	mcmp.AssertMatches("SELECT LAST_INSERT_ID();", fmt.Sprintf("[[UINT64(%d)]]", result))
	result, _ = getSeqResult(t, mcmp)
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bit)values (3, 3, 1, false)")
	mcmp.AssertMatches("SELECT LAST_INSERT_ID();", fmt.Sprintf("[[UINT64(%d)]]", result))
}
func TestSimpleInsert(t *testing.T) {
	//测试插入多个值和单个值
	mcmp, closer := start(t)
	defer closer()
	result, _ := getSeqResult(t, mcmp)
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bool) values (1, 1, 1, false),(2, 2, 2, false),(3, 3, 3, false),(4, 4, 4, true)")
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 1 ;", fmt.Sprintf("[[INT64(%d) INT32(1) INT32(1) INT8(1) INT8(0)]]", result))
	result++
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 2 ;", fmt.Sprintf("[[INT64(%d) INT32(2) INT32(2) INT8(2) INT8(0)]]", result))
	result++
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 3 ;", fmt.Sprintf("[[INT64(%d) INT32(3) INT32(3) INT8(3) INT8(0)]]", result))
	result++
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 4 ;", fmt.Sprintf("[[INT64(%d) INT32(4) INT32(4) INT8(4) INT8(1)]]", result))
	result++
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bool) values (5, 5, 5, true)")
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 5 ;", fmt.Sprintf("[[INT64(%d) INT32(5) INT32(5) INT8(5) INT8(1)]]", result))
}

func TestInsertOnDuplicateKeyUpdate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	result, _ := getSeqResult(t, mcmp)
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bool) values (1, 1, 1, false)")
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 1 ;", fmt.Sprintf("[[INT64(%d) INT32(1) INT32(1) INT8(1) INT8(0)]]", result))
	mcmp.Exec(fmt.Sprintf("insert into t_1(id, f_shard,f_table,f_tinyint,f_bool) values ( %d,1, 1, 2, true) ON DUPLICATE KEY UPDATE f_tinyint = VALUES(f_tinyint), f_bool = VALUES(f_bool);", result))
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 1 ;", fmt.Sprintf("[[INT64(%d) INT32(1) INT32(1) INT8(2) INT8(1)]]", result))
}

func TestInsertIgnore(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	result, _ := getSeqResult(t, mcmp)
	mcmp.Exec("insert into t_1(f_shard,f_table,f_tinyint,f_bool) values (1, 1, 1, false)")
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 1 ;", fmt.Sprintf("[[INT64(%d) INT32(1) INT32(1) INT8(1) INT8(0)]]", result))
	mcmp.ExecAllowAndCompareError(fmt.Sprintf("insert into t_1(id, f_shard,f_table,f_tinyint,f_bool) values ( %d,1, 1, 2, true);", result))
	mcmp.Exec(fmt.Sprintf("insert ignore into t_1(id, f_shard,f_table,f_tinyint,f_bool) values ( %d,1, 1, 2, true);", result))
	mcmp.AssertMatches("SELECT id,f_shard,f_table,f_tinyint,f_bool from t_1 where f_table = 1 ;", fmt.Sprintf("[[INT64(%d) INT32(1) INT32(1) INT8(1) INT8(0)]]", result))
}
