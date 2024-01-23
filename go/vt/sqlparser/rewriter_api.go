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

package sqlparser

import (
	"strings"
)

// The rewriter was heavily inspired by https://github.com/golang/tools/blob/master/go/ast/astutil/rewrite.go

// Rewrite traverses a syntax tree recursively, starting with root,
// and calling pre and post for each node as described below.
// Rewrite returns the syntax tree, possibly modified.
//
// If pre is not nil, it is called for each node before the node's
// children are traversed (pre-order). If pre returns false, no
// children are traversed, and post is not called for that node.
//
// If post is not nil, and a prior call of pre didn't return false,
// post is called for each node after its children are traversed
// (post-order). If post returns false, traversal is terminated and
// Apply returns immediately.
//
// Only fields that refer to AST nodes are considered children;
// i.e., fields of basic types (strings, []byte, etc.) are ignored.
func Rewrite(node SQLNode, pre, post ApplyFunc) (result SQLNode) {
	parent := &RootNode{node}

	// this is the root-replacer, used when the user replaces the root of the ast
	replacer := func(newNode SQLNode, _ SQLNode) {
		parent.SQLNode = newNode
	}

	a := &application{
		pre:  pre,
		post: post,
	}

	a.rewriteSQLNode(parent, node, replacer)

	return parent.SQLNode
}

// SafeRewrite does not allow replacing nodes on the down walk of the tree walking
// Long term this is the only Rewrite functionality we want
func SafeRewrite(
	node SQLNode,
	shouldVisitChildren func(node SQLNode, parent SQLNode) bool,
	up ApplyFunc,
) SQLNode {
	var pre func(cursor *Cursor) bool
	if shouldVisitChildren != nil {
		pre = func(cursor *Cursor) bool {
			visitChildren := shouldVisitChildren(cursor.Node(), cursor.Parent())
			if !visitChildren && up != nil {
				// this gives the up-function a chance to do work on this node even if we are not visiting the children
				// unfortunately, if the `up` function also returns false for this node, we won't abort the rest of the
				// tree walking. This is a temporary limitation, and will be fixed when we generated the correct code
				up(cursor)
			}
			return visitChildren
		}
	}
	return Rewrite(node, pre, up)
}

// RootNode is the root node of the AST when rewriting. It is the first element of the tree.
type RootNode struct {
	SQLNode
}

// An ApplyFunc is invoked by Rewrite for each node n, even if n is nil,
// before and/or after the node's children, using a Cursor describing
// the current node and providing operations on it.
//
// The return value of ApplyFunc controls the syntax tree traversal.
// See Rewrite for details.
type ApplyFunc func(*Cursor) bool

// A Cursor describes a node encountered during Apply.
// Information about the node and its parent is available
// from the Node and Parent methods.
type Cursor struct {
	parent   SQLNode
	replacer replacerFunc
	node     SQLNode

	// marks that the node has been replaced, and the new node should be visited
	revisit bool
}

// Node returns the current Node.
func (c *Cursor) Node() SQLNode { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() SQLNode { return c.parent }

// Replace replaces the current node in the parent field with this new object. The use needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode SQLNode) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

// ReplacerF returns a replace func that will work even when the cursor has moved to a different node.
func (c *Cursor) ReplacerF() func(newNode SQLNode) {
	replacer := c.replacer
	parent := c.parent
	return func(newNode SQLNode) {
		replacer(newNode, parent)
	}
}

// ReplaceAndRevisit replaces the current node in the parent field with this new object.
// When used, this will abort the visitation of the current node - no post or children visited,
// and the new node visited.
func (c *Cursor) ReplaceAndRevisit(newNode SQLNode) {
	switch newNode.(type) {
	case SelectExprs, Expr:
	default:
		// We need to add support to the generated code for when to look at the revisit flag. At the moment it is only
		// there for slices of SQLNode implementations
		panic("no support added for this type yet")
	}

	c.replacer(newNode, c.parent)
	c.node = newNode
	c.revisit = true
}

type replacerFunc func(newNode, parent SQLNode)

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post ApplyFunc
	cur       Cursor
}

// ReplaceTbName is used to replace the name of a table by a token given by a map.
// @param in SQLNode to be replaced stmt tree.
// @param replacements map[string]string the replaced tokens.
// @param isCopy bool whether copy the replaced tree nodes.
func ReplaceTbName(in SQLNode, replacements map[string]string, isCopy bool) (result SQLNode) {
	aliasedTableNames := make(map[string]bool)
	switch stmt := in.(type) {
	case *Select:
		getAliasedTableNames(replacements, aliasedTableNames, stmt.From)
	case *Delete:
		getAliasedTableNames(replacements, aliasedTableNames, stmt.TableExprs)
	case *Update:
		getAliasedTableNames(replacements, aliasedTableNames, stmt.TableExprs)
	}
	preFunc := func(node SQLNode, parent SQLNode) bool {
		_, ok := node.(TableName)
		return !ok
	}
	if isCopy {
		return CopyOnRewrite(in, preFunc, func(cursor *CopyOnWriteCursor) {
			switch cNode := cursor.Node().(type) {
			case TableName:
				switch cursor.parent.(type) {
				case *AliasedTableExpr:
					if replacement, ok := replacements[cNode.Name.String()]; ok {
						cursor.Replace(TableName{
							Name:      NewIdentifierCS(replacement),
							Qualifier: cNode.Qualifier,
						})
					}
				}
				if _, isAsName := aliasedTableNames[cNode.Name.String()]; !isAsName {
					if replacement, ok := replacements[cNode.Name.String()]; ok {
						cursor.Replace(TableName{
							Name:      NewIdentifierCS(replacement),
							Qualifier: cNode.Qualifier,
						})
					}
				}
			}
		}, nil)
	}
	return SafeRewrite(in, preFunc, func(cursor *Cursor) bool {
		switch cNode := cursor.Node().(type) {
		case TableName:
			switch cursor.parent.(type) {
			case *AliasedTableExpr:
				if replacement, ok := replacements[cNode.Name.String()]; ok {
					cursor.Replace(TableName{
						Name:      NewIdentifierCS(replacement),
						Qualifier: cNode.Qualifier,
					})
				}
			}
			if _, isAsName := aliasedTableNames[cNode.Name.String()]; !isAsName {
				if replacement, ok := replacements[cNode.Name.String()]; ok {
					cursor.Replace(TableName{
						Name:      NewIdentifierCS(replacement),
						Qualifier: cNode.Qualifier,
					})
				}
			}
		}
		return true
	})
}

func getAliasedTableNames(tableMap map[string]string, asNameList map[string]bool, tableExprs []TableExpr) {
	for _, expr := range tableExprs {
		switch node := expr.(type) {
		case *AliasedTableExpr:
			asNameList[node.As.String()] = true
		case *JoinTableExpr:
			getAliasedTableNames(tableMap, asNameList, []TableExpr{node.LeftExpr})
			getAliasedTableNames(tableMap, asNameList, []TableExpr{node.RightExpr})
		}
	}
}

func ReplaceToken(query string, token string, value string) string {
	token = FormateToken(token)
	var buf strings.Builder
	buf.Grow(len(query))

	n, l, r, t := len(query), 0, -1, 0

	for c, s := range query {
		if r == -1 {
			if n-c+1 < len(token) {
				break
			}
			if byte(s) == token[0] {
				r = c
				t++
			}
		} else {
			if byte(s) != token[t] {
				t = 0
				buf.WriteString(query[l:r])
				l = r
				if byte(s) == token[t] {
					r = c
					t++
				} else {
					r = -1
				}
			} else {
				if c-r+1 == len(token) {
					t = 0
					buf.WriteString(query[l:r] + value)
					r = -1
					l = c + 1
				} else {
					t++
				}
			}
		}
	}
	if l < n {
		buf.WriteString(query[l:])
	}
	return buf.String()
}

func AcqTokenIndex(query string, token string) []int {
	token = FormateToken(token)
	result := make([]int, 0)

	i := 0
	for i <= len(query)-len(token) {
		if query[i:i+len(token)] == token {
			result = append(result, i, i+len(token)-1)
			i += len(token)
		} else {
			i++
		}
	}

	return result
}

// FormateToken is used to format tokens when the token has key word or letter.
func FormateToken(token string) string {
	if containEscapableChars(token, 0) {
		return "`" + token + "`"
	} else {
		return token
	}
}
