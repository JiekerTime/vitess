/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	regexps map[string]*regexp.Regexp
)

// Handles initializing the regular expression map.
func init() {
	expressions := [][2]string{
		{"select_autocommit", "@@(session.|global.)?['\"`]?autocommit['\"`]?"},
		{"show_variables", "show +(session( )+)?variables"},
		{"show_databases", "show +databases"},
		{"select_database", "database\\( *\\)"},
		{"select_database_user", "database\\( *\\), +user\\( *\\)"},
		{"last_insert_id", `(?i)^select(\s|\t|\n){1,}(last_insert_id\(\))(\s|\t|\n){1,}as(\s|\t|\n){1,}`},
		{"found_rows", `(?i)^select(\s|\t|\n){1,}(found_rows\(\))(\s|\t|\n){1,}as(\s|\t|\n){1,}`},
		{"set_query_tablet_type", `(?i)^set\s+query_tablet_type.*`},
	}

	regexps = make(map[string]*regexp.Regexp)

	for _, exp := range expressions {
		regexps[exp[0]] = regexp.MustCompile(exp[1])
	}
}

// CrossInterceptor ...
type CrossInterceptor struct {
}

// NewAttachInterceptor ,judge cmd cross tablet or not
func NewAttachInterceptor() *CrossInterceptor {
	return &CrossInterceptor{}
}

// passthrough-show databases cross tablet
func (itr *CrossInterceptor) passthrough(query string) bool {
	switch sqlparser.Preview(query) {
	case sqlparser.StmtShow:
		if regexps["show_databases"].MatchString(strings.ToLower(query)) {
			return false
		}
	case sqlparser.StmtPlan:
		return false
	}
	return true
}
