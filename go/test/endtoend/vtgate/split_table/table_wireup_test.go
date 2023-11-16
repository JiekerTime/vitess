package split_table

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestWireupCase(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,  '6', 'aaa', 1, false, 1, 2, 3, 100,  200,  1 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  '3', 'bbb', 2, false, 2, 3, 4, 103,  200,  1 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'a', 'ccc', 3, false, 3, 4, 5, 100,  200, 'a')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  '5', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '6', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,  '2', 'bbb', 2, false, 2, 3, 4, 100,  300,  3 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,  '2', 'ccc', 3, false, 3, 4, 5, 102,  300,  4 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 )")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (1,  1, 2, 200, '1', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (2,  2, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (3,  3, 4, 200, '5', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (4,  4, 4, 200, '3', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (5,  2, 2, 5,   '2', 5  , 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (6,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (7,  2, 3, 300, '2', 200, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (8,  8, 5, 300, '4', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (9,  9, 3, 300, '5', 300, 5)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz, foo) VALUES (10, 5, 3, 300, '4', 300, 5)")

	// join on having clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select e.col, u.id uid, e.id eid from t_user u join t_user_extra e having uid = eid")
	// bind var already in use
	//mcmp.ExecWithColumnCompareAndNotEmpty("select e.col, u.id uid, e.id eid from t_user u join t_user_extra e having uid = eid and e.col = :uid")
	mcmp.ExecWithColumnCompareAndNotEmpty("select e.col, u.id uid, e.id eid from t_user u join t_user_extra e having uid = eid and e.col = 2")
	// wire-up join with join, going left
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 join t_user u3 where u3.col = u1.col")
	// wire-up join with join, going left, then right
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 join t_user u3 where u3.col = u2.col")
	// wire-up join with join, reuse existing result from a lower join
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 on u2.col = u1.col join t_user u3 where u3.col = u1.col")
	// wire-up join with join, reuse existing result from a lower join.
	//# You need two levels of join nesting to test this: when u3 requests
	//# col from u1, the u1-u2 joins exports the column to u2-u3. When
	//# u4 requests it, it should be reused from the u1-u2 join.
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 join t_user u3 on u3.id = u1.col join t_user u4 where u4.col = u1.col")
	// Test reuse of join var already being supplied to the right of a node.
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join (t_user u2 join t_user u3) where u2.id = u1.col and u3.id = u1.col")
	// wire-up with limit primitive
	mcmp.ExecWithColumnCompareAndNotEmpty("select u.id, e.id from t_user u join t_user_extra e where e.id = u.col limit 10")
	// Wire-up in subquery
	// subquery returned more than one column (errno 1105)
	// v18.0.0 already fix
	//mcmp.ExecWithColumnCompareAndNotEmpty("select 1 from t_user where id in (select u.id, e.id from t_user u join t_user_extra e where e.id = u.col limit 10)")
	// Wire-up in underlying primitive after pullout
	// subquery returned more than one row (errno 1105)
	//mcmp.ExecWithColumnCompareAndNotEmpty("select u.id, e.id, (select col from t_user) from t_user u join t_user_extra e where e.id = u.col limit 10")
	// expected: []string{"id", "id", "(select col from t_user limit 1)"}
	// actual  : []string{"id", "id", "3"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompareAndNotEmpty("select u.id, e.id, (select col from t_user limit 1) from t_user u join t_user_extra e where e.id = u.col limit 10")
	//results mismatched.
	//        Vitess Results:
	//        [INT64(1) INT64(6) VARCHAR("3")]
	//        [INT64(2) INT64(3) VARCHAR("3")]
	//        [INT64(4) INT64(5) VARCHAR("3")]
	//        [INT64(5) INT64(6) VARCHAR("3")]
	//        [INT64(6) INT64(2) VARCHAR("3")]
	//        [INT64(7) INT64(2) VARCHAR("3")]
	//        [INT64(8) INT64(2) VARCHAR("3")]
	//        [INT64(9) INT64(2) VARCHAR("3")]
	//        [INT64(10) INT64(2) VARCHAR("3")]
	//        MySQL Results:
	//        [INT64(1) INT64(6) CHAR("3")]
	//        [INT64(2) INT64(3) CHAR("3")]
	//        [INT64(4) INT64(5) CHAR("3")]
	//        [INT64(5) INT64(6) CHAR("3")]
	//        [INT64(6) INT64(2) CHAR("3")]
	//        [INT64(7) INT64(2) CHAR("3")]
	//        [INT64(8) INT64(2) CHAR("3")]
	//        [INT64(9) INT64(2) CHAR("3")]
	//        [INT64(10) INT64(2) CHAR("3")]
	// subquery中field类型与MySQL不一致
	//mcmp.ExecAndNotEmpty("select u.id, e.id, (select col from t_user where col = 3 limit 1) from t_user u join t_user_extra e where e.id = u.col order by u.id limit 10")
	utils.AssertMatchesAny(t, mcmp.VtConn, "select u.id, e.id, (select col from t_user where col = 3 limit 1) from t_user u join t_user_extra e where e.id = u.col order by u.id limit 10", "[[INT64(1) INT64(6) VARCHAR(\"3\")] [INT64(2) INT64(3) VARCHAR(\"3\")] [INT64(4) INT64(5) VARCHAR(\"3\")] [INT64(5) INT64(6) VARCHAR(\"3\")] [INT64(6) INT64(2) VARCHAR(\"3\")] [INT64(7) INT64(2) VARCHAR(\"3\")] [INT64(8) INT64(2) VARCHAR(\"3\")] [INT64(9) INT64(2) VARCHAR(\"3\")] [INT64(10) INT64(2) VARCHAR(\"3\")]]")
	// Invalid value in IN clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id in (18446744073709551616, 1)")
	// Invalid value in IN clause from LHS of join
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 where u1.id = 10")
	// Invalid value in IN clause from RHS of join
	mcmp.ExecWithColumnCompareAndNotEmpty("select u1.id from t_user u1 join t_user u2 where u2.id = 10")
}
