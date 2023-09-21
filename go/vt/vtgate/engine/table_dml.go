package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

	// Table specifies the table for the update.
	Table []*vindexes.Table

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
	return nil, fmt.Errorf("implement me")
}

func (dml *TableDML) execMultiDestination(ctx context.Context, primitive Primitive, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard,
	dmlSpecialFunc func(context.Context, VCursor, map[string]*querypb.BindVariable, []*srvtopo.ResolvedShard) error,
	actualTableNameMap map[string][]vindexes.ActualTable) (*sqltypes.Result, error) {
	if len(rss) == 0 {
		return &sqltypes.Result{}, nil
	}

	if err := dml.getSplitQueries(bindVars, actualTableNameMap); err != nil {
		return nil, err

	}

	if err := dmlSpecialFunc(ctx, vcursor, bindVars, rss); err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}
	for _, query := range dml.Queries {
		rssQueries := make([]*querypb.BoundQuery, 0, len(rss))
		for range rss {
			rssQueries = append(rssQueries, query)
		}
		innerResult, err := execMultiShard(ctx, primitive, vcursor, rss, rssQueries, dml.MultiShardAutocommit)
		if err != nil {
			return nil, err
		}
		result.AppendResult(innerResult)
	}
	for _, field := range result.Fields {
		field.Table = dml.TableRouteParam.LogicTable[dml.Table[0].Name.String()].LogicTableName
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
	if dml.Table != nil {
		tableNameMap := map[string]any{}
		for _, table := range dml.Table {
			tableNameMap[table.Name.String()] = nil
		}

		var tableNames []string
		for name := range tableNameMap {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)

		return strings.Join(tableNames, ", ")
	}
	return ""
}

func (dml *TableDML) GetSingleTable() (*vindexes.Table, error) {
	if len(dml.Table) > 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported dml on complex table expression")
	}
	return dml.Table[0], nil
}

func (dml *TableDML) getSplitQueries(bindVars map[string]*querypb.BindVariable, actualTableNameMap map[string][]vindexes.ActualTable) error {
	splitTableConfig, found := dml.TableRouteParam.LogicTable[dml.Table[0].Name.String()]
	if !found {
		return vterrors.VT13001("not found %s splitTableConfig", dml.Table[0].Name.String())
	}

	queries, err := getTableQueries(dml.AST, splitTableConfig, bindVars, actualTableNameMap)
	if err != nil {
		return err
	}
	dml.Queries = queries
	return nil
}
