/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*SplitTableSend)(nil)

// SplitTableSend is an operator to send query to the specific keyspace, tabletType and destination
type SplitTableSend struct {

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	// IsDML specifies how to deal with autocommit behaviour
	IsDML bool

	// SingleShardOnly specifies that the query must be send to only single shard
	SingleShardOnly bool

	// ShardNameNeeded specified that the shard name is added to the bind variables
	ShardNameNeeded bool

	// MultishardAutocommit specifies that a multishard transaction query can autocommit
	MultishardAutocommit bool

	noInputs

	// config of splitTable
	SplitTableConfig vindexes.SplitTableMap
}

// NeedsTransaction implements the Primitive interface
func (s *SplitTableSend) NeedsTransaction() bool {
	return s.IsDML
}

// RouteType implements Primitive interface
func (s *SplitTableSend) RouteType() string {
	if s.IsDML {
		return "SendDML"
	}

	return "Send"
}

// GetKeyspaceName implements Primitive interface
func (s *SplitTableSend) GetKeyspaceName() string {
	return s.Keyspace.Name
}

// GetTableName implements Primitive interface
func (s *SplitTableSend) GetTableName() string {
	return ""
}

// TryExecute implements Primitive interface
func (s *SplitTableSend) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, 0)
	defer cancelFunc()
	rss, _, err := vcursor.ResolveDestinations(ctx, s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return nil, err
	}

	if !s.Keyspace.Sharded && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	querieses, errBuildQueries := buildSplitTableQueries(rss, s, bindVars)
	if errBuildQueries != nil {
		return nil, errBuildQueries
	}

	qrs := new(sqltypes.Result)
	rollbackOnError := s.IsDML // for non-dml queries, there's no need to do a rollback
	query0 := querieses[0]
	for indexForTable := range query0 {
		executeQuery := make([]*querypb.BoundQuery, 0, len(rss))
		for i := range rss {
			executeQuery = append(executeQuery, querieses[i][indexForTable])
		}
		qr, errQr := vcursor.ExecuteMultiShard(ctx, s, rss, executeQuery, rollbackOnError, s.canAutoCommit(vcursor, rss))
		if errQr != nil {
			return nil, vterrors.Aggregate(errQr)
		}
		qrs.AppendResult(qr)
	}
	return qrs, nil
}

func buildSplitTableQueries(rss []*srvtopo.ResolvedShard, s *SplitTableSend, bindVars map[string]*querypb.BindVariable) ([][]*querypb.BoundQuery, error) {
	querieses := make([][]*querypb.BoundQuery, len(rss))
	logicalTableNames := make([]string, 0, len(s.SplitTableConfig))
	for logicTableName := range s.SplitTableConfig {
		logicalTableNames = append(logicalTableNames, logicTableName)
	}
	actualTableFirst := s.SplitTableConfig[logicalTableNames[0]]
	stmt, _, err := sqlparser.Parse2(s.Query)
	if err != nil {
		return nil, err
	}
	for i := range rss {
		var queries []*querypb.BoundQuery
		for indexForTable := range actualTableFirst.ActualTableList {
			actualTableNames := make(map[string]string, len(actualTableFirst.ActualTableList))
			for _, logicalTableName := range logicalTableNames {
				actualTableNames[logicalTableName] = s.SplitTableConfig[logicalTableName].ActualTableList[indexForTable].ActualTableName
			}
			cloneStmt := sqlparser.DeepCloneStatement(stmt)
			sqlparser.SafeRewrite(cloneStmt, nil, func(cursor *sqlparser.Cursor) bool {
				switch node := cursor.Node().(type) {
				case sqlparser.TableName:
					if value, ok := actualTableNames[node.Name.String()]; ok {
						cursor.Replace(sqlparser.TableName{
							Name: sqlparser.NewIdentifierCS(value),
						})
					}
				}
				return true
			})

			queries = append(queries, &querypb.BoundQuery{
				Sql:           sqlparser.String(cloneStmt),
				BindVariables: bindVars,
			})
		}
		querieses[i] = queries
	}
	return querieses, nil
}

func (s *SplitTableSend) canAutoCommit(vcursor VCursor, rss []*srvtopo.ResolvedShard) bool {
	if s.IsDML {
		return (len(rss) == 1 || s.MultishardAutocommit) && vcursor.AutocommitApproval()
	}
	return false
}

// TryStreamExecute implements Primitive interface
func (s *SplitTableSend) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	rss, _, err := vcursor.ResolveDestinations(ctx, s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return err
	}

	if !s.Keyspace.Sharded && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i, rs := range rss {
		bv := bindVars
		if s.ShardNameNeeded {
			bv = copyBindVars(bindVars)
			bv[ShardName] = sqltypes.StringBindVariable(rs.Target.Shard)
		}
		multiBindVars[i] = bv
	}
	errors := vcursor.StreamExecuteMulti(ctx, s, s.Query, rss, multiBindVars, s.IsDML, s.canAutoCommit(vcursor, rss), callback)
	return vterrors.Aggregate(errors)
}

// GetFields implements Primitive interface
func (s *SplitTableSend) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(ctx, s, bindVars, false)
	if err != nil {
		return nil, err
	}
	qr.Rows = nil
	return qr, nil
}

func (s *SplitTableSend) description() PrimitiveDescription {
	other := map[string]any{
		"Query": s.Query,
		"Table": s.GetTableName(),
	}
	if s.IsDML {
		other["IsDML"] = true
	}
	if s.SingleShardOnly {
		other["SingleShardOnly"] = true
	}
	if s.ShardNameNeeded {
		other["ShardNameNeeded"] = true
	}
	if s.MultishardAutocommit {
		other["MultishardAutocommit"] = true
	}
	return PrimitiveDescription{
		OperatorType:      "Send",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,
		Other:             other,
	}
}
