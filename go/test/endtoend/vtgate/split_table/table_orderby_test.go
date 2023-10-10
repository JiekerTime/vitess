/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package split_table

import (
	"testing"
)

func TestSimpleOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key) values (1, 'a', 'aaa'),(2, 'b', 'bbb'),(3, 'c', 'ccc'),(4, 'd', 'ddd')")

	mcmp.AssertMatches(`select f_key from t_user   order by f_key asc`, `[[CHAR("aaa")] [CHAR("bbb")] [CHAR("ccc")] [CHAR("ddd")]]`)
	mcmp.AssertMatches(`select col from t_user   order by col asc`, `[[CHAR("a")] [CHAR("b")] [CHAR("c")] [CHAR("d")]]`)
	mcmp.AssertMatches(`select col from t_user where id =1   order by col asc`, `[[CHAR("a")]]`)
	mcmp.AssertMatches(`select col from t_user where id in(1,2)   order by col asc`, `[[CHAR("a")] [CHAR("b")]]`)
	mcmp.AssertMatches(`select col from t_user where id in(1,2) and col in('a','b')  order by col asc`, `[[CHAR("a")] [CHAR("b")]]`)
}

func TestOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("use user")
	mcmp.Exec("insert into t_user(id,col,f_key) values (1, 'a', 'aaa'),(2, 'b', 'bbb')")

	mcmp.AssertMatches(`select id,col,f_key from t_user   order by f_key asc`, `[[INT64(1) CHAR("a") CHAR("aaa")] [INT64(2) CHAR("b") CHAR("bbb")]]`)
	mcmp.AssertMatches(`select id,col,f_key from t_user   order by col asc`, `[[INT64(1) CHAR("a") CHAR("aaa")] [INT64(2) CHAR("b") CHAR("bbb")]]`)

	mcmp.AssertMatches(`select id,col,f_key from t_user   order by f_key desc`, `[[INT64(2) CHAR("b") CHAR("bbb")] [INT64(1) CHAR("a") CHAR("aaa")]]`)
	mcmp.AssertMatches(`select id,col,f_key from t_user   order by col desc`, `[[INT64(2) CHAR("b") CHAR("bbb")] [INT64(1) CHAR("a") CHAR("aaa")]]`)

	//VT12001: unsupported: '*' expression in cross-shard query (errno 1235)
	//mcmp.Exec(`select * from t_user  order by col asc`)

	//VT12001: unsupported: query cannot be fully operator planned (errno 1235)
	//mcmp.Exec(`select * from t_user where id =1 order by col asc`)

	//VT12001: unsupported: '*' expression in cross-shard query (errno 1235)
	//mcmp.Exec(`select * from t_user where col ='a' order by col asc`)

	mcmp.Exec(`select * from t_user where col ='a' and id=1  order by col asc`)

	mcmp.Exec("select col from t_user where id =1  order by reverse(col) asc")

	// table_postprocess_cases.json
	mcmp.ExecWithColumnCompare("select predef1 from t_user where id = 5 order by predef2")
	mcmp.ExecWithColumnCompare("select predef1 from t_user where id = 5 and col = 6 order by col")
	mcmp.ExecWithColumnCompare("select col from t_user order by col")
	mcmp.ExecWithColumnCompare("select id from t_user order by id")
	mcmp.ExecWithColumnCompare("select col from t_user order by id")
	mcmp.ExecWithColumnCompare("select col from t_user order by 1")
	mcmp.ExecWithColumnCompare("select id as foo from t_user order by 1")
	mcmp.ExecAllowAndCompareError("select col from t_user order by 2")
	mcmp.ExecWithColumnCompare("select col from t_user order by null")
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by a")
	mcmp.ExecWithColumnCompare("select t_user.* from t_user where id = 5 and col = 6 order by t_user.a")
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by t_user.a")
	//VT12001: unsupported: '*' expression in cross-shard query
	//	mcmp.ExecWithColumnCompare("select t.*, t.col from t_user t order by t.col")
	//	mcmp.ExecWithColumnCompare("select *, col from t_user order by col")
	//  mcmp.ExecWithColumnCompare("select t.*, t.name, t.*, t.col from t_user t order by t.col")
	//  mcmp.ExecWithColumnCompare("select *, name, *, col from t_user order by col")
	mcmp.ExecWithColumnCompare("select col from t_user order by a,b")
	mcmp.ExecWithColumnCompare("select id,col,a,b from t_user order by 2,3")
	mcmp.ExecWithColumnCompare("select col from t_user order by RAND()")
	mcmp.ExecAllowAndCompareError("select t_user.* from t_user where id = 5 order by e.col")
	//error parsing column number: 18446744073709551616
	//mcmp.ExecWithColumnCompare("select col from t_user where id = 5 order by 18446744073709551616")
	mcmp.ExecAllowAndCompareError("select col from t_user where id = 5 order by 2")
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by -col1")
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by concat(col,col1) collate utf8_general_ci desc")
	mcmp.ExecWithColumnCompare("select * from t_user where id = 5 and col = 6 order by id+col collate utf8_general_ci desc")
	mcmp.ExecWithColumnCompare("select col from t_user where id = 1  and col = 6 order by col")
	mcmp.ExecWithColumnCompare("select col as foo from t_user order by foo")
	mcmp.ExecWithColumnCompare("select col as foo,col2 as col from t_user order by col")
	mcmp.ExecWithColumnCompare("select col, col from t_user order by col")
	mcmp.ExecWithColumnCompare("select col from t_user order by col+1")
	mcmp.ExecWithColumnCompare("select t_user.col1 as a from t_user order by 1 collate utf8_general_ci")

}
