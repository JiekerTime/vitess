package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TableDML contains the common elements between TableUpdate and TableDelete plans
type TableDML struct {
	// Queries are the split query statements
	Queries []*querypb.BoundQuery

	// AST is the sql statement
	AST sqlparser.Statement

	// KsidVindex is primary Vindex
	KsidVindex vindexes.Vindex

	// KsidLength is number of columns that represents KsidVindex
	KsidLength int

	// TableNames are the name of the tables involved in the query.
	TableNames []string

	// Vindexes are the column vindexes modified by this DML.
	Vindexes []*vindexes.ColumnVindex

	// Option to override the standard behavior and allow a multi-shard update
	// to use single round trip autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int
	// RoutingParameters parameters required for query routing.
	ShardRouteParam *RoutingParameters

	TableRouteParam *TableRoutingParameters

	txNeeded
}

func (dml *TableDML) execUnsharded(ctx context.Context, primitive Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard,
	actualTableNameMap map[string][]vindexes.ActualTable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("unsupported opcode: %v", Unsharded)
}

func (dml *TableDML) execMultiDestination(ctx context.Context, primitive Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard,
	dmlSpecialFunc func(context.Context, VCursor, map[string]*querypb.BindVariable, []*srvtopo.ResolvedShard) error,
	actualTableNameMap map[string][]vindexes.ActualTable) (*sqltypes.Result, error) {
	if len(rss) == 0 {
		return &sqltypes.Result{}, nil
	}

	var err error
	if dml.Queries, err = dml.TableRouteParam.getTableQueries(dml.AST, bindVars, actualTableNameMap); err != nil {
		return nil, err
	}

	if dmlSpecialFunc != nil {
		if err = dmlSpecialFunc(ctx, vcursor, bindVars, rss); err != nil {
			return nil, err
		}
	}

	queries := make([][]*querypb.BoundQuery, len(rss))

	for i := range rss {
		queries[i] = dml.Queries
	}

	isSingleShardSingleSql := false
	if len(rss) == 1 {
		isSingleShardSingleSql = len(queries[0]) == 1
	}
	autocommit := isSingleShardSingleSql && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteBatchMultiShard(ctx, primitive, rss, queries, true /* rollbackOnError */, autocommit)

	if errs != nil {
		return nil, vterrors.Aggregate(errs)
	}
	return result, nil
}

func (dml *TableDML) RouteType() string {
	return dml.TableRouteParam.TableOpcode.String()
}

func (dml *TableDML) GetKeyspaceName() string {
	return dml.ShardRouteParam.Keyspace.Name
}

func (dml *TableDML) GetTableName() string {
	sort.Strings(dml.TableNames)
	var tableNames []string
	var previousTbl string
	for _, name := range dml.TableNames {
		if name != previousTbl {
			tableNames = append(tableNames, name)
			previousTbl = name
		}
	}
	return strings.Join(tableNames, ", ")
}
