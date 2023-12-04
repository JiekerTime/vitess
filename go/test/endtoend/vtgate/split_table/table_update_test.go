package split_table

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestTableBasticUpdate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false),(3, 'c', 'ccc', 3, true)")

	/* sharded tests */
	// update Scatter-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_key = 'ddd' WHERE f_bit = true", "WHERE f_bit = true", `[[INT64(3) CHAR("c") CHAR("ddd") INT8(3) BIT("\x01")]]`)
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_key = 'eee' WHERE f_tinyint in (1,2)", "WHERE f_tinyint in (1,2)", `[[INT64(1) CHAR("a") CHAR("eee") INT8(1) BIT("\x00")] [INT64(2) CHAR("b") CHAR("eee") INT8(2) BIT("\x00")]]`)
	// update EqualUnique-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_key = 'fff' WHERE id = 1", "WHERE id = 1", `[[INT64(1) CHAR("a") CHAR("fff") INT8(1) BIT("\x00")]]`)
	// update Scatter-EqualUnique type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_key = 'ggg' WHERE col = 'b'", "WHERE col = 'b'", `[[INT64(2) CHAR("b") CHAR("ggg") INT8(2) BIT("\x00")]]`)
	// update EqualUnique-EqualUnique type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_key = 'hhh' WHERE id = 3 AND col = 'c'", "WHERE id = 3 AND col = 'c'", `[[INT64(3) CHAR("c") CHAR("hhh") INT8(3) BIT("\x01")]]`)
	// update IN-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_bit = true WHERE id in (1,2)", "WHERE id in (1,2)", `[[INT64(1) CHAR("a") CHAR("fff") INT8(1) BIT("\x01")] [INT64(2) CHAR("b") CHAR("ggg") INT8(2) BIT("\x01")]]`)
	// update Scatter-IN type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_tinyint = 4 WHERE col in ('b','c')", "WHERE col in ('b','c')", `[[INT64(2) CHAR("b") CHAR("ggg") INT8(4) BIT("\x01")] [INT64(3) CHAR("c") CHAR("hhh") INT8(4) BIT("\x01")]]`)
	// update MultiEqual-MultiEqual type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user SET f_bit = true WHERE (col,f_key) in (('b','ggg'),('c','hhh'))", "WHERE (col,f_key) in (('b','ggg'),('c','hhh'))",
		`[[INT64(2) CHAR("b") CHAR("ggg") INT8(4) BIT("\x01")] [INT64(3) CHAR("c") CHAR("hhh") INT8(4) BIT("\x01")]]`)

	mcmp.Exec("insert into t_8(id,f_shard_table,f_int) VALUES (1,'1',1)")
	mcmp.ExecWithColumnCompare("update t_8 set f_int=2 where f_shard_table='1'")
	mcmp.ExecWithColumnCompare("select f_shard_table,f_int from t_8 where f_shard_table='1'")

}

func TestTableDifficultUpdate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit) values (1, 'a', 'aaa', 1, false),(2, 'b', 'bbb', 2, false),(3, 'c', 'ccc', 3, true)")

	/* difficult sql tests*/
	// update Scatter-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_key = 'ddd' WHERE user.f_bit = true", "WHERE f_bit = true", `[[INT64(3) CHAR("c") CHAR("ddd") INT8(3) BIT("\x01")]]`)
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_key = 'eee' WHERE user.f_tinyint in (1,2)", "WHERE f_tinyint in (1,2)", `[[INT64(1) CHAR("a") CHAR("eee") INT8(1) BIT("\x00")] [INT64(2) CHAR("b") CHAR("eee") INT8(2) BIT("\x00")]]`)
	// update EqualUnique-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_key = 'fff' WHERE user.id = 1", "WHERE id = 1", `[[INT64(1) CHAR("a") CHAR("fff") INT8(1) BIT("\x00")]]`)
	// update Scatter-EqualUnique type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_key = 'ggg' WHERE user.col = 'b'", "WHERE col = 'b'", `[[INT64(2) CHAR("b") CHAR("ggg") INT8(2) BIT("\x00")]]`)
	// update EqualUnique-EqualUnique type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_key = 'hhh' WHERE user.id = 3 AND user.col = 'c'", "WHERE id = 3 AND col = 'c'", `[[INT64(3) CHAR("c") CHAR("hhh") INT8(3) BIT("\x01")]]`)
	// update IN-Scatter type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_bit = true WHERE user.id in (1,2)", "WHERE id in (1,2)", `[[INT64(1) CHAR("a") CHAR("fff") INT8(1) BIT("\x01")] [INT64(2) CHAR("b") CHAR("ggg") INT8(2) BIT("\x01")]]`)
	// update Scatter-IN type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_tinyint = 4 WHERE user.col in ('b','c')", "WHERE col in ('b','c')", `[[INT64(2) CHAR("b") CHAR("ggg") INT8(4) BIT("\x01")] [INT64(3) CHAR("c") CHAR("hhh") INT8(4) BIT("\x01")]]`)
	// update MultiEqual-MultiEqual type
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_bit = true WHERE (user.col,user.f_key) in (('b','ggg'),('c','hhh'))", "WHERE (col,f_key) in (('b','ggg'),('c','hhh'))",
		`[[INT64(2) CHAR("b") CHAR("ggg") INT8(4) BIT("\x01")] [INT64(3) CHAR("c") CHAR("hhh") INT8(4) BIT("\x01")]]`)

	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_tinyint = user.f_tinyint + 1 WHERE user.id = 3 AND user.col = 'c'", "WHERE id = 3 AND col = 'c'",
		`[[INT64(3) CHAR("c") CHAR("hhh") INT8(5) BIT("\x01")]]`)
	execWithColumnCompareAndCheck(mcmp, "UPDATE t_user AS user SET user.f_bit = false where user.id > 1", "",
		`[[INT64(1) CHAR("a") CHAR("fff") INT8(1) BIT("\x01")] [INT64(2) CHAR("b") CHAR("ggg") INT8(4) BIT("\x00")] [INT64(3) CHAR("c") CHAR("hhh") INT8(5) BIT("\x00")]]`)
}

func execWithColumnCompareAndCheck(mcmp utils.MySQLCompare, query string, checkClause string, expected string) {
	mcmp.ExecWithColumnCompare(query)
	checkQuery := "SELECT id,col,f_key,f_tinyint,f_bit FROM t_user " + checkClause + " ORDER BY id"
	mcmp.AssertMatches(checkQuery, expected)
}
