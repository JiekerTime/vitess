package split_table

import (
	"testing"
)

func TestTableFilterCases(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key,f_tinyint,f_bit,someColumn,name) values (1, '5', 'aaa', 1, false,null,'a'),(2, 'b', 'bbb', 2, false,null,'b'),(3, '1', 'ccc', 3, false,'test','c'),(4, '1', 'ccc', 3, false,'test','c'),(5, 'a', 'aaa', 1,false,'test', '\\'')")

	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (11, 'a', 'aaa', 1, false, 1, 2, 3, 100,  200,  1 ,'a',        'a')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (12, '3', 'bbb', 2, false, 2, 3, 4, 103,  200,  1 ,'a',        'c')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (13, 'a', 'ccc', 3, false, 3, 4, 5, 100,  200, 'a','b',        'Anddress')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (15, '5', 'ccc', 3, false, 3, 4, 5, 103,  200,  4 ,'b',        'c')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (6,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,'b',        'Anddress')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (7,  '2', 'bbb', 2, false, 2, 3, 4, 100,  300,  3 ,'And1res',  'Anddress')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (8,  '2', 'ccc', 3, false, 3, 4, 5, 102,  300,  4 ,'Anddress', 'c')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (9,  '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,'And1res',  'And1res')")
	mcmp.Exec("insert into t_user(id, col, f_key, f_tinyint, f_bit, a, b, c, intcol, foo, name, textcol1, textcol2) values (10, '2', 'aaa', 1, false, 1, 2, 3, 100,  300,  2 ,'And1res',  'a')")
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
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (1, 11, '42',  10, 1, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (2, 10, '42',  10, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (3, 12, 'bbb', 10, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (4, 13, 'bbb', 10, 2, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (5, 12, 'ccc', 10, 3, 202)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (6, 11, '42',  10, 2, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (7, 10, '42',  10, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (8, 12, 'bbb', 10, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (9, 13, 'bbb', 10, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (10, 13, 'bbb', 10, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (11, 13, 'bbb', 10, 1, 302)")
	mcmp.Exec("insert into t_music(id, user_id, col, a, bar, foo) VALUES (12, 13, 'bbb', 10, 1, 302)")

	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.* from t_user t_user where col = 5 and id = 1")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT t_user.id,name FROM t_user t_user WHERE t_user.id = 3 AND col = '1'")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT * FROM t_user  WHERE id = 3 AND col = '1'")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT * FROM t_user  WHERE col = '1' AND col = '1'")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT * /* this is &#x000D;&#x000A; block comment */ FROM /* this is another &#x000A; block comment */ t_user where name='1'")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT * FROM t_user where name='\\'' ")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT name as 'name' FROM t_user")
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT INTERVAL(name,1,5) func_status FROM t_user WHERE id = 3 AND col = '1'")

	// table_filter_cases.json

	// No where clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user")
	// Query that always return empty
	mcmp.ExecWithColumnCompare("select id from t_user where someColumn = null")
	// Null Safe Equality Operator is handled correctly
	mcmp.ExecWithColumnCompareAndNotEmpty("SELECT id from t_user where someColumn <=> null")
	// Composite IN clause vs equality
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where (col, name) in (('b', 'b')) and id = 2")
	// Composite IN: tuple inside tuple, mismiatched values
	mcmp.AssertContainsError("select id from t_user where ((col1, name), col2) in (('aa', 'bb', 'cc'), (('dd', 'ee'), 'ff'))", "Operand should contain 2 column(s)")
	// IN clause: LHS is neither column nor composite tuple
	mcmp.ExecWithColumnCompare("select Id from t_user where 1 in ('aa', 'bb')")
	// Single table complex in clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where name in (col, 'c')")
	// Single table unique vindex route
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.id = 2")
	// Single table unique vindex route, but complex expr
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.id = 1+2")
	// Route with multiple route constraints, SelectIN is the best constraint.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col = 'b' and t_user.id in (1, 2)")
	// Route with multiple route constraints and boolean, SelectIN is the best constraint.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col = case t_user.col when 'a' then true else false end and t_user.id in (1, 2)")
	// Route with multiple route constraints, SelectEqualUnique is the best constraint.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col = 'a' and t_user.id in (11, 12) and t_user.name = '1' and t_user.id = 11")
	// Route with multiple route constraints, SelectEqualUnique is the best constraint, order reversed.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.id = 11 and t_user.name = '1' and t_user.id in (11, 12) and t_user.col = 'a'")
	// Route with OR and AND clause, must parenthesize correctly.
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.id = 1 or t_user.name = 'a' and t_user.id in (1, 2)")
	// database() call in where clause.
	mcmp.ExecWithColumnCompare("select id from t_user where database()")
	// non unique predicate on vindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.id > 1")
	// transitive closures for the win
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.name = t_user.col and t_user.col = 'a'")
	// deconstruct tuple equality comparisons
	mcmp.ExecWithColumnCompare("select id from t_user where (id, name) = (1, '')")
	// optimize ORs to IN route op codes #1
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where id = 1 or id = 2")
	// optimize ORs to IN route op codes #2
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where id = 1 or id = 2 or id = 3")
	// optimize ORs to IN route op codes #3
	mcmp.ExecWithColumnCompareAndNotEmpty("select col from t_user where (id = 1 or id = 2) or (id = 3 or id = 4)")
	// Self referencing columns in HAVING should work
	mcmp.ExecWithColumnCompare("select a+2 as a from t_user having a = 1")
	// HAVING predicates that use table columns are safe to rewrite if we can move them to the WHERE clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.col + 2 as a from t_user having a = 2")
	// Single table unique vindex route hiding behind a silly OR
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where (id = 1 and name ='a') or (id = 2 and someColumn = 'test')")
	// Single table IN vindex route hiding behind OR
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where (id = 1 and name ='a') or (id = 3 and name = 'c')")
	// two predicates that mean the same thing
	mcmp.ExecWithColumnCompareAndNotEmpty("select textcol1 from t_user where foo = 200 and t_user.foo = 200")
	mcmp.ExecWithColumnCompareAndNotEmpty("select textcol1 from t_user where f_tinyint = 2 and t_user.f_tinyint = 2")
	// TableRoute Equal
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col = 'a'")
	// TableRoute Equal Swap left and right positions
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where 1 = col")
	// TableRoute In to Equal
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col in (1)")
	// TableRoute In
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col in ('a', 'b', 'c')")
	// TableRoute In & Equal PickBestAvailableTindex
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col in ('a', 'b', 'c') and col = 'a'")
	// TableRoute is null
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.c is null")
	// TableRoute = null
	mcmp.ExecWithColumnCompare("select id from t_user where t_user.c = null")
	// TableRoute simplifyExpression
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where not (not col = '3')")
	// TableRoute or to IN
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where (col in ('1', '5') and B or C and col in ('5', '7'))")
	// Single table unique tindex route, but complex expr
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where t_user.col = 1+0")
	// Single table unique tindex route, but complex expr
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_user where id = 10 and t_user.col = 1+1")
	// Multi-route unique vindex constraint
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user_extra.id from t_user join t_user_extra on t_user.col = t_user_extra.col where t_user.id = 6")
	// Multi-route with cross-route constraint
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user_extra.id from t_user join t_user_extra on t_user.col = t_user_extra.col where t_user_extra.user_id = t_user.col")
	// Multi-route with non-route constraint, should use first route.
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user_extra.id from t_user join t_user_extra on t_user.col = t_user_extra.col where 1 = 1")
	// not supported transitive closures with equality inside of an OR
	mcmp.AssertContainsError("select id from t_user, t_user_extra where t_user.id = t_user_extra.col and (t_user_extra.col = t_user_extra.user_id or t_user_extra.col2 = t_user_extra.name)", "Column 'id' in field list is ambiguous")
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.id from t_user, t_user_extra where t_user.id = t_user_extra.col and (t_user_extra.col = t_user_extra.user_id or t_user_extra.bar = t_user_extra.baz)")
	// left join where clauses where we can optimize into an inner join
	mcmp.ExecWithColumnCompareAndNotEmpty("select t_user.id from t_user left join t_user_extra on t_user.col = t_user_extra.col where t_user_extra.foo = 5")
	// push filter under aggregation
	mcmp.ExecWithColumnCompareAndNotEmpty("select count(*) from t_user left join t_user_extra on t_user.id = t_user_extra.bar where IFNULL(t_user_extra.col, 'NOTSET') != 'collections_lock'")
	// Single table multiple unique vindex match
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_music where id = 2 and user_id = 10")
	// Select with equals null
	mcmp.ExecWithColumnCompare("select id from t_music where id = null")
	mcmp.ExecWithColumnCompare("select id from t_music where id is null")
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_music where id is not null order by id")
	// Single table with unique vindex match and null match
	mcmp.ExecWithColumnCompare("select id from t_music where user_id = 4 and id = null")
	mcmp.ExecWithColumnCompare("select id from t_music where user_id = 4 and id IN (null)")
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_music where user_id = 13 and id IN (null, 10, 12) order by id")
	// not in (null) -> is always false
	mcmp.ExecWithColumnCompare("select id from t_music where user_id = 13 and id NOT IN (null, 1, 12) order by id")
	mcmp.ExecWithColumnCompare("select id from t_music where id NOT IN (null, 1, 2) and user_id = 13 order by id")
	mcmp.ExecWithColumnCompare("select id from t_music where id is null and user_id in (1,2) order by user_id")
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_music where user_id = 13 and id NOT IN (1, 12) order by id")
	mcmp.ExecWithColumnCompareAndNotEmpty("select id from t_music where id NOT IN (1, 2) and user_id = 13 order by id")
	// HAVING predicates that use table columns are safe to rewrite if we can move them to the WHERE clause
	mcmp.ExecWithColumnCompareAndNotEmpty("select bar+2 as a from t_music where user_id=10 having a=4")
	// one shard using distinct
	mcmp.ExecWithColumnCompareAndNotEmpty("select distinct col from t_user where id= 13")
	// Like clause evaluated on the vtgate
	mcmp.ExecWithColumnCompareAndNotEmpty("select a.textcol1 from t_user a join t_user b where a.textcol1 = b.textcol2 group by a.textcol1 having repeat(a.textcol1,sum(a.id)) like 'And%res'")
}
