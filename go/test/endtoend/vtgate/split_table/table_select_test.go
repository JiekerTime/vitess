package split_table

import (
	"testing"
)

func TestSelect(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (1,  '3',    'aaa', 1, false, 1, 2, 3, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (2,  '3',    'bbb', 2, false, 2, 3, 4, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (3,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (4,  'abc',  'ccc', 3, true,  3, 4, 5, 100, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (5,  '12',   'ccc', 3, true,  3, 4, 5, 103, 200, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (6,  '2',    'aaa', 1, true,  1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (7,  '1024', 'bbb', 2, false, 2, 3, 4, 100, 300, 3)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (8,  '1024', 'ccc', 3, false, 3, 4, 5, 102, 300, 4)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (9,  '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (10, '1024', 'aaa', 1, false, 1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (11, '12',   'aaa', 1, true,  1, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (12, '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (13, '1024', 'aaa', 1, false, 3, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (14, '123',  'aaa', 1, false, 2, 2, 3, 100, 300, 'abc')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name) values (15, '1024', 'aaa', 1, false, 2, 2, 3, 100, 300, 2)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (1, 101, 101, 200, 'aaa', 200),(2, 102, 102, 200, 'xxx', 200),(3, 103, 103, 200, 'bbb', 200),(4, 104, 104, 200, 'aaa', 200),(5, 105, 105, 200, 'ada', 300)")
	mcmp.Exec("insert into t_user_extra(id, user_id, extra_id, bar, col, baz) VALUES (6, 101, 101, 300, 'aaa', 200),(7, 102, 102, 300, 'ddd', 200),(8, 103, 103, 300, 'ccc', 300),(9, 104, 104, 300, 'aaa', 300),(10, 105, 105, 300, 'axa', 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (101,  11, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (121,  10, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (131,  12, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (141,  13, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (161,  12, 'ccc', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (102,  11, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (122,  10, 'aaa', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (132,  12, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (143,  13, 'bbb', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (164,  12, 'ccc', 10, 200)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1010, 11, 'aaa', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1213, 10, 'aaa', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1314, 12, 'bbb', 10, 300)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar) VALUES (1415, 13, 'bbb', 10, 300)")

	// table_select_case.json
	mcmp.ExecWithColumnCompare("select t_user.* from t_user  t_user")

	//expected: []string{"1"}
	//actual  : []string{":vtg1 /* INT64 */"}
	// column names do not match - the expected values are what mysql produced
	mcmp.Exec("select 1 from dual")

	// No column referenced
	mcmp.ExecWithColumnCompare("select 1 from t_user")
	// '*' expression for simple route
	mcmp.ExecWithColumnCompare("select t_user.* from t_user")
	// unqualified '*' expression for simple route
	mcmp.ExecWithColumnCompare("select * from t_user")
	// qualified '*' expression for simple route
	mcmp.ExecWithColumnCompare("select t_user.* from t_user")
	// fully qualified '*' expression for simple route
	mcmp.ExecWithColumnCompare("select user.t_user.* from user.t_user")
	// select * from authoritative table
	//mcmp.ExecWithColumnCompare("select * from t_authoritative")
	// select * from qualified authoritative table
	//mcmp.ExecWithColumnCompare("select a.* from t_authoritative a")
	// sharded limit offset
	mcmp.ExecWithColumnCompare("select user_id from t_music order by user_id limit 10, 20")
	// Sharding Key Condition in Parenthesis
	mcmp.ExecWithColumnCompare("select * from t_user where name ='abc' AND (id = 14) and (col = 123) limit 5")
	// Multiple parenthesized expressions
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 4) AND (name ='abc') AND (col = 'abc') limit 5")
	// Multiple parenthesized expressions
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 4 AND name ='abc' AND col = 'abc') limit 5")
	// Column Aliasing with Table.Column
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 and col = 3 order by user0_.col desc limit 2")
	// Column Aliasing with Column
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 11 and col = 12 order by col0_ desc limit 3")
	// Column Aliasing with Table.Column,splitTable Limit
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 order by user0_.col desc limit 2")
	// Column Aliasing with Column,splitTable Limit
	mcmp.ExecWithColumnCompare("select user0_.col as col0_ from t_user user0_ where id = 1 order by col0_ desc limit 3")
	// Booleans and parenthesis
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 11) and (col = 12) AND f_bit = true limit 5")
	// Column as boolean-ish
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 11) and (col = 12) AND f_bit limit 5")
	// PK as fake boolean, and column as boolean-ish
	mcmp.ExecWithColumnCompare("select * from t_user where (id = 5) and (col = 12) AND f_bit = true limit 5")
	// group by with non aggregated columns and table alias
	mcmp.ExecWithColumnCompare("select u.id, u.intcol, u.col from t_user u group by u.id, u.col")
	// Auto-resolve should work if unique vindex columns are referenced
	mcmp.AssertContainsError("select id, user_id from t_user join t_user_extra", "Column 'id' in field list is ambiguous")
	mcmp.ExecWithColumnCompare("select t_user.id, t_user_extra.user_id from t_user join t_user_extra")
	// RHS TableRoute referenced
	mcmp.ExecWithColumnCompare("select t_user_extra.id from t_user join t_user_extra")
	// Both TableRoutes referenced
	mcmp.ExecWithColumnCompare("select t_user.col, t_user_extra.id from t_user join t_user_extra")
	// Expression with single-TableRoute reference
	// expected: []string{"col", "t_user_extra.id + t_user_extra.col"}
	// actual  : []string{"col", "t_user_extra_0.id + t_user_extra_0.col"}
	// column names do not match - the expected values are what mysql produced
	//mcmp.ExecWithColumnCompare("select t_user.col, t_user_extra.id + t_user_extra.col from t_user join t_user_extra")
	mcmp.Exec("select t_user.col, t_user_extra.id + t_user_extra.col from t_user join t_user_extra")
	// Jumbled references
	mcmp.ExecWithColumnCompare("select t_user.col, t_user_extra.id, t_user.col2 from t_user join t_user_extra")
	// Comments
	mcmp.ExecWithColumnCompare("select /* comment */ t_user.col from t_user join t_user_extra")
	// Case preservation
	mcmp.ExecWithColumnCompare("select t_user.Col, t_user_extra.Id from t_user join t_user_extra")
	// select expression having dependencies on both sides of a join
	mcmp.ExecWithColumnCompare("select t_user.id * user_id as amount from t_user, t_user_extra")
	// use output column containing data from both sides of the join
	// expected: []string{"t_user_extra.col + t_user.col"}
	// actual  : []string{"'bbb' + t_user_0.col"}
	//mcmp.ExecWithColumnCompare("select t_user_extra.col + t_user.col from t_user join t_user_extra on t_user.id = t_user_extra.id")
	mcmp.Exec("select t_user_extra.col + t_user.col from t_user join t_user_extra on t_user.id = t_user_extra.id")
}

// sql_mode not only_full_group_by
func TestSqlMode(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")

	mcmp.Exec("insert into t_user(id,col,f_key) values (1, 'a1', 'aaa') ")
	mcmp.ExecWithColumnCompare("select f_key from t_user group by col")
}
