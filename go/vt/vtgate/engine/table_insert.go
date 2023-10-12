package engine

/*
方便后续合代码 把分表的逻辑单独写到了一个文件里
后续可以把getInsertShardedRoute的公共函数抽出来简化代码
*/
import (
	"context"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func (ins *Insert) execInsertTableSharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.processGenerateFromValues(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	rss, queries, err := ins.getInsertTableShardedRoute(ctx, vcursor, bindVars)

	if err != nil {
		return nil, err
	}

	return ins.executeInsertQueriesForSplitTable(ctx, vcursor, rss, queries, insertID)
}

func (ins *Insert) getInsertTableShardedRoute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) ([]*srvtopo.ResolvedShard, [][]*querypb.BoundQuery, error) {
	// vindexRowsValues builds the values of all vindex columns.
	// the 3-d structure indexes are colVindex, row, col. Note that
	// ins.Values indexes are colVindex, col, row. So, the conversion
	// involves a transpose.
	// The reason we need to transpose is that all the Vindex APIs
	// require inputs in that format.
	vindexRowsValues := make([][]sqltypes.Row, len(ins.VindexValues))
	rowCount := 0
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	colVindexes := ins.ColVindexes
	if colVindexes == nil {
		colVindexes = ins.Table.ColumnVindexes
	}
	for vIdx, vColValues := range ins.VindexValues {
		if len(vColValues) != len(colVindexes[vIdx].Columns) {
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] supplied vindex column values don't match vschema: %v %v", vColValues, colVindexes[vIdx].Columns)
		}
		for colIdx, colValues := range vColValues {
			rowsResolvedValues := make(sqltypes.Row, 0, len(colValues))
			for _, colValue := range colValues {
				result, err := env.Evaluate(colValue)
				if err != nil {
					return nil, nil, err
				}
				rowsResolvedValues = append(rowsResolvedValues, result.Value(vcursor.ConnCollation()))
			}
			// This is the first iteration: allocate for transpose.
			if colIdx == 0 {
				if len(rowsResolvedValues) == 0 {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] rowcount is zero for inserts: %v", rowsResolvedValues)
				}
				if rowCount == 0 {
					rowCount = len(rowsResolvedValues)
				}
				if rowCount != len(rowsResolvedValues) {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] uneven row values for inserts: %d %d", rowCount, len(rowsResolvedValues))
				}
				vindexRowsValues[vIdx] = make([]sqltypes.Row, rowCount)
			}
			// Perform the transpose.
			for rowNum, colVal := range rowsResolvedValues {
				vindexRowsValues[vIdx][rowNum] = append(vindexRowsValues[vIdx][rowNum], colVal)
			}
		}
	}

	// The output from the following 'process' functions is a list of
	// keyspace ids. For regular inserts, a failure to find a route
	// results in an error. For 'ignore' type inserts, the keyspace
	// id is returned as nil, which is used later to drop the corresponding rows.
	if len(vindexRowsValues) == 0 || len(colVindexes) == 0 {
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.RequiresPrimaryKey, vterrors.PrimaryVindexNotSet, ins.Table.Name)
	}
	keyspaceIDs, err := ins.processPrimary(ctx, vcursor, vindexRowsValues[0], colVindexes[0])
	if err != nil {
		return nil, nil, err
	}

	for vIdx := 1; vIdx < len(colVindexes); vIdx++ {
		colVindex := colVindexes[vIdx]
		var err error
		if colVindex.Owned {
			err = ins.processOwned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		} else {
			err = ins.processUnowned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		}
		if err != nil {
			return nil, nil, err
		}
	}

	// Build 3-d bindvars. Skip rows with nil keyspace ids in case
	// we're executing an insert ignore.
	for vIdx, colVindex := range colVindexes {
		for rowNum, rowColumnKeys := range vindexRowsValues[vIdx] {
			if keyspaceIDs[rowNum] == nil {
				// InsertIgnore: skip the row.
				continue
			}
			for colIdx, vindexKey := range rowColumnKeys {
				col := colVindex.Columns[colIdx]
				name := InsertVarName(col, rowNum)
				bindVars[name] = sqltypes.ValueBindVariable(vindexKey)
			}
		}
	}

	// We need to know the keyspace ids and the Mids associated with
	// each RSS.  So we pass the ksid indexes in as ids, and get them back
	// as values. We also skip nil KeyspaceIds, no need to resolve them.
	var indexes []*querypb.Value
	var destinations []key.Destination
	for i, ksid := range keyspaceIDs {
		if ksid != nil {
			indexes = append(indexes, &querypb.Value{
				Value: strconv.AppendInt(nil, int64(i), 10),
			})
			destinations = append(destinations, key.DestinationKeyspaceID(ksid))
		}
	}
	if len(destinations) == 0 {
		// In this case, all we have is nil KeyspaceIds, we don't do
		// anything at all.
		return nil, nil, nil
	}

	rss, indexesPerRss, err := vcursor.ResolveDestinations(ctx, ins.Keyspace.Name, indexes, destinations)
	if err != nil {
		return nil, nil, err
	}

	//上面全是分片的逻辑代码和getInsertShardedRoute一样
	//分表和分片的逻辑一样根据plan层传递的TableVindexValues，去计算表名与rows对应关系
	tableVindexRowsValues, err := ins.getTableVindexRowsValues(ctx, vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}
	tableColumns := ins.TableColVindexes.TableIndexColumn
	for rowNum, rowColumnKeys := range tableVindexRowsValues {
		for colIdx, vindexKey := range rowColumnKeys {
			name := InsertVarName(tableColumns[colIdx].Column, rowNum)
			bindVars[name] = sqltypes.ValueBindVariable(vindexKey)
		}
	}
	actualTables, err := ins.resolveTables(ctx, vcursor, tableVindexRowsValues)
	if err != nil {
		return nil, nil, err
	}
	queries := make([][]*querypb.BoundQuery, len(rss))
	for i := range rss {
		mids := make(map[string][]string)
		for _, indexValue := range indexesPerRss[i] {
			index, _ := strconv.ParseInt(string(indexValue.Value), 0, 64)
			if keyspaceIDs[index] != nil {
				mids[actualTables[index].ActualTableName] = append(mids[actualTables[index].ActualTableName], ins.Mid[index])
			}
		}
		for tableName, mid := range mids {
			queries[i] = append(queries[i], &querypb.BoundQuery{
				Sql:           strings.Replace(ins.Prefix, ins.TableColVindexes.LogicTableName, tableName, 1) + strings.Join(mid, ",") + ins.Suffix,
				BindVariables: bindVars,
			})
		}
	}

	return rss, queries, nil
}

func (ins *Insert) getTableVindexRowsValues(ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable) (tableVindexRowsValues []sqltypes.Row, err error) {

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	rowCount := 0
	for colIdx, colValues := range ins.TableVindexValues {
		rowsResolvedValues := make(sqltypes.Row, 0, len(colValues))
		for _, colValue := range colValues {
			result, err := env.Evaluate(colValue)
			if err != nil {
				return nil, err
			}
			rowsResolvedValues = append(rowsResolvedValues, result.Value(vcursor.ConnCollation()))
		}

		if colIdx == 0 {
			if len(rowsResolvedValues) == 0 {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] rowcount is zero for inserts: %v", rowsResolvedValues)
			}
			if rowCount == 0 {
				rowCount = len(rowsResolvedValues)
			}
			if rowCount != len(rowsResolvedValues) {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] uneven row values for inserts: %d %d", rowCount, len(rowsResolvedValues))
			}
			tableVindexRowsValues = make([]sqltypes.Row, rowCount)

		}
		for rowNum, colVal := range rowsResolvedValues {
			tableVindexRowsValues[rowNum] = append(tableVindexRowsValues[rowNum], colVal)
		}

	}
	return tableVindexRowsValues, nil

}

func (ins *Insert) resolveTables(ctx context.Context, vcursor VCursor, vindexKeys []sqltypes.Row) (tables []vindexes.ActualTable, err error) {

	// Map using the Vindex
	destinations, err := vindexes.TableMap(ctx, ins.TableColVindexes.TableVindex, vcursor, vindexKeys)

	if err != nil {
		return nil, err
	}
	var logicTableConfig = ins.TableColVindexes
	for _, destination := range destinations {
		if err = destination.Resolve(logicTableConfig, func(actualTableIndex int) error {
			tables = append(tables, logicTableConfig.ActualTableList[actualTableIndex])
			return nil
		}); err != nil {
			return tables, err
		}
	}
	return tables, nil
}

func (ins *Insert) executeInsertQueriesForSplitTable(
	ctx context.Context,
	vcursor VCursor,
	rss []*srvtopo.ResolvedShard,
	queries [][]*querypb.BoundQuery,
	insertID int64,
) (*sqltypes.Result, error) {
	isSingleShardSingleSql := false
	if len(rss) == 1 {
		isSingleShardSingleSql = len(queries[0]) == 1
	}
	autocommit := isSingleShardSingleSql && vcursor.AutocommitApproval()
	err := allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}
	result, errs := vcursor.ExecuteBatchMultiShard(ctx, ins, rss, queries, true /* rollbackOnError */, autocommit)
	if errs != nil {
		return nil, vterrors.Aggregate(errs)
	}

	if insertID != 0 {
		result.InsertID = uint64(insertID)
	}
	return result, nil
}
