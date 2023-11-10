package vtgate

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/vtgate/logstats"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var (
	loadMaxRowsInBatch = flag.Int("load_max_rows_in_batch", 2000, "max insert rows in batch of load data")
	//loadMaxRetryTimes  = flag.Int("load_max_retry_times", 2, "load data retry count for executing batch insert ")
)

// InsertFunc insert callback
type InsertFunc func(insert string) error

// LoadDataFunc ...
type LoadDataFunc func() error

// NewLoadData create LoadData return pointer
func NewLoadData(e *Executor) *LoadData {
	return &LoadData{
		LoadDataInfo: &LoadDataInfo{
			e:              e,
			maxRowsInBatch: *loadMaxRowsInBatch,
		},
	}
}

// LoadData include load_data status info and package info
type LoadData struct {
	LoadDataInfo *LoadDataInfo
}

// LoadDataInfo params
type LoadDataInfo struct {
	e              *Executor
	maxRowsInBatch int
	LinesInfo      *sqlparser.LinesClause
	FieldsInfo     *sqlparser.FieldsClause
	Columns        sqlparser.Columns
	Table          *sqlparser.TableName

	//LoadDtaDone used for load data, set true if load data fnished(error or success)
	LoadDataDone bool
}

// SetMaxRowsInBatch sets the max number of rows to insert in a batch.
func (l *LoadDataInfo) SetMaxRowsInBatch(limit int) {
	l.maxRowsInBatch = limit
}

// ParseLoadDataPram parse the load data statement
func (l *LoadDataInfo) ParseLoadDataPram(loadStmt *sqlparser.LoadDataStmt) {
	l.Columns = loadStmt.Columns
	l.Table = &loadStmt.Table
	l.FieldsInfo = loadStmt.FieldsInfo
	l.LinesInfo = loadStmt.LinesInfo

}

// getValidData returns prevData and curData that starts from starting symbol.
// If the data doesn't have starting symbol, prevData is nil and curData is curData[len(curData)-startingLen+1:].
// If curData size less than startingLen, curData is returned directly.
func (l *LoadDataInfo) getValidData(prevData, curData []byte) ([]byte, []byte) {
	startingLen := len(l.LinesInfo.Starting)
	if startingLen == 0 {
		return prevData, curData
	}

	prevLen := len(prevData)
	if prevLen > 0 {
		// starting symbol in the prevData
		idx := strings.Index(string(prevData), l.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:], curData
		}

		// starting symbol in the middle of prevData and curData
		restStart := curData
		if len(curData) >= startingLen {
			restStart = curData[:startingLen-1]
		}
		prevData = append(prevData, restStart...)
		idx = strings.Index(string(prevData), l.LinesInfo.Starting)
		if idx != -1 {
			return prevData[idx:prevLen], curData
		}
	}

	// starting symbol in the curData
	idx := strings.Index(string(curData), l.LinesInfo.Starting)
	if idx != -1 {
		return nil, curData[idx:]
	}

	// no starting symbol
	if len(curData) >= startingLen {
		curData = curData[len(curData)-startingLen+1:]
	}
	return nil, curData
}

// getLine returns a line, curData, the next data start index and a bool value.
// If it has starting symbol the bool is true, otherwise is false.
func (l *LoadDataInfo) getLine(prevData, curData []byte) ([]byte, []byte, bool) {
	startingLen := len(l.LinesInfo.Starting)
	prevData, curData = l.getValidData(prevData, curData)
	if prevData == nil && len(curData) < startingLen {
		return nil, curData, false
	}
	prevLen := len(prevData)
	terminatedLen := len(l.LinesInfo.Terminated)
	curStartIdx := 0
	if prevLen < startingLen {
		curStartIdx = startingLen - prevLen
	}
	endIdx := -1
	if len(curData) >= curStartIdx {
		endIdx = strings.Index(string(hack.String(curData[curStartIdx:])), l.LinesInfo.Terminated)
	}
	if endIdx == -1 {
		// no terminated symbol
		if len(prevData) == 0 {
			return nil, curData, true
		}

		// terminated symbol in the middle of prevData and curData
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(hack.String(curData[startingLen:])), l.LinesInfo.Terminated)
		if endIdx != -1 {
			nextDataIdx := startingLen + endIdx + terminatedLen
			return curData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
		}
		// no terminated symbol
		return nil, curData, true
	}

	// terminated symbol in the curData
	nextDataIdx := curStartIdx + endIdx + terminatedLen
	if len(prevData) == 0 {
		return curData[curStartIdx : curStartIdx+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the curData
	prevData = append(prevData, curData[:nextDataIdx]...)
	endIdx = strings.Index(string(hack.String(prevData[startingLen:])), l.LinesInfo.Terminated)
	if endIdx >= prevLen {
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}

// getLine returns a line, curData, the next data start index and a bool value.
// If it has starting symbol the bool is true, otherwise is false.
/*func (l *LoadDataInfo) getLineNew(curData []byte) ([]byte, []byte, bool) {
	startingLen := len(l.LinesInfo.Starting)
	prevData, curData = l.getValidData(prevData, curData)
	if prevData == nil && len(curData) < startingLen {
		return nil, curData, false
	}
	prevLen := len(prevData)
	terminatedLen := len(l.LinesInfo.Terminated)
	curStartIdx := 0
	if prevLen < startingLen {
		curStartIdx = startingLen - prevLen
	}
	endIdx := -1
	if len(curData) >= curStartIdx {
		endIdx = strings.Index(string(curData[curStartIdx:]), l.LinesInfo.Terminated)
	}
	if endIdx == -1 {
		// no terminated symbol
		if len(prevData) == 0 {
			return nil, curData, true
		}

		// terminated symbol in the middle of prevData and curData
		curData = append(prevData, curData...)
		endIdx = strings.Index(string(curData[startingLen:]), l.LinesInfo.Terminated)
		if endIdx != -1 {
			nextDataIdx := startingLen + endIdx + terminatedLen
			return curData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
		}
		// no terminated symbol
		return nil, curData, true
	}

	// terminated symbol in the curData
	nextDataIdx := curStartIdx + endIdx + terminatedLen
	if len(prevData) == 0 {
		return curData[curStartIdx : curStartIdx+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the curData
	prevData = append(prevData, curData[:nextDataIdx]...)
	endIdx = strings.Index(string(prevData[startingLen:]), l.LinesInfo.Terminated)
	if endIdx >= prevLen {
		return prevData[startingLen : startingLen+endIdx], curData[nextDataIdx:], true
	}

	// terminated symbol in the middle of prevData and curData
	lineLen := startingLen + endIdx + terminatedLen
	return prevData[startingLen : startingLen+endIdx], curData[lineLen-prevLen:], true
}
*/

// MysqlEscap is used to escape the source.
func (l *LoadDataInfo) MysqlEscap(source string) (string, error) {
	var j = 0
	if len(source) == 0 {
		return "", errors.New("source is null")
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
		case '\n':
			flag = true
			escape = '\n'
		case '\\':
			flag = true
			escape = '\\'
		case '\'':
			flag = true
			escape = '\''
		case '"':
			flag = true
			escape = '"'
		case '\032':
			flag = true
			escape = 'Z'
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j]), nil
}

// MakeInsert  batch "insert ignore into" to implement the  load data
func (l *LoadDataInfo) MakeInsert(rows [][]string, tb *vindexes.Table, fields []*querypb.Field) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	var insertVasSQLBuf bytes.Buffer
	insertVasSQLBuf.WriteString("INSERT  IGNORE  INTO ")
	insertVasSQLBuf.WriteString(l.Table.Name.String())

	//get fields type and make the  insert ignore into values(...)
	columns := l.Columns
	columnsSize := len(columns)
	var specificFields = make([]*querypb.Field, 0, len(columns))
	insertVasSQLBuf.WriteString("(")

	// identify columns is not necessary
	if columns != nil && columnsSize > 0 {
		for key, value := range columns {
			for _, field := range fields {
				if strings.EqualFold(field.Name, value.String()) {
					specificFields = append(specificFields, field)
				}
			}
			insertVasSQLBuf.WriteString(value.String())
			if key != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}
		insertVasSQLBuf.WriteString(") ")
	} else {
		specificFields = fields
		columnsSize = len(specificFields)
		for key, e := range specificFields {
			insertVasSQLBuf.WriteString(e.Name)
			if key != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}
		insertVasSQLBuf.WriteString(") ")
	}
	insertVasSQLBuf.WriteString("values")
	for r, record := range rows {
		insertVasSQLBuf.WriteString("(")

		for k, specificField := range specificFields {
			var column string
			if k >= len(record) {
				column = "NULL"
			} else {
				column = record[k]
			}

			// Distinguish the type of  field. number or char.
			// if number eg:insert values(100);
			// if string eg:insert values('100')
			if isNumericType(specificField.Type, column) {
				if strings.TrimSpace(column) == "" {
					column = "0"
				}
				insertVasSQLBuf.WriteString(column)
			} else {
				insertVasSQLBuf.WriteString("'" + column + "'")
			}
			if k != columnsSize-1 {
				insertVasSQLBuf.WriteString(",")
			}
		}

		if r == len(rows)-1 {
			insertVasSQLBuf.WriteString(")")
		} else {
			insertVasSQLBuf.WriteString("),")
		}
	}
	return insertVasSQLBuf.String(), nil
}

// GetRowFromLine splits line according to fieldsInfo, this function is exported for testing.
func (l *LoadDataInfo) GetRowFromLine(line []byte) ([]string, error) {
	var sep []byte
	if l.FieldsInfo.Enclosed != 0 {
		if line[0] != l.FieldsInfo.Enclosed || line[len(line)-1] != l.FieldsInfo.Enclosed {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "line %s should begin and end with %v", string(line), l.FieldsInfo.Enclosed)
		}
		line = line[1 : len(line)-1]
		sep = make([]byte, 0, len(l.FieldsInfo.Terminated)+2)
		sep = append(sep, l.FieldsInfo.Enclosed)
		sep = append(sep, l.FieldsInfo.Terminated...)
		sep = append(sep, l.FieldsInfo.Enclosed)
	} else {
		sep = []byte(l.FieldsInfo.Terminated)
	}
	rawCols := bytes.Split(line, sep)
	cols := escapeCols(rawCols)
	return cols, nil
}

// InsertData inserts data into specified table according to the specified format.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true
func (l *LoadDataInfo) InsertData(prevData, curData []byte, rows *[][]string, tb *vindexes.Table, fields []*querypb.Field, callback InsertFunc) ([]byte, bool, error) {
	// TODO: support enclosed and escape.
	if len(prevData) == 0 && len(curData) == 0 {
		return nil, false, nil
	}
	var line []byte
	var isEOF, hasStarting, reachLimit bool
	if len(prevData) > 0 && len(curData) == 0 {
		isEOF = true
		prevData, curData = curData, prevData
	}
	for len(curData) > 0 {
		line, curData, hasStarting = l.getLine(prevData, curData)
		prevData = nil
		// If it doesn't find the terminated symbol and this data isn't the last data,
		// the data can't be inserted.
		if line == nil && !isEOF {
			break
		}
		// If doesn't find starting symbol, this data can't be inserted.
		if !hasStarting {
			if isEOF {
				curData = nil
			}
			break
		}
		if line == nil && isEOF {
			line = curData[len(l.LinesInfo.Starting):]
			curData = nil
		}

		cols, err := l.GetRowFromLine(line)
		if err != nil {
			return nil, false, err
		}
		*rows = append(*rows, cols)
		if l.maxRowsInBatch != 0 && len(*rows) == l.maxRowsInBatch {
			reachLimit = true
			inserts, err := l.MakeInsert(*rows, tb, fields)
			if err != nil {
				return nil, false, err
			}

			*rows = make([][]string, 0)
			if err := callback(inserts); err != nil {
				return nil, false, err
			}
			break
		}
	}
	return curData, reachLimit, nil
}

// GetRowsFromPacket inserts data into specified table according to the specified format.
// If prevData isn't nil and curData is nil, there are no other data to deal with and the isEOF is true
func (l *LoadDataInfo) GetRowsFromPacket(prevData, curData []byte, rows *[]interface{}, tb *vindexes.Table, wantField bool, callback LoadDataFunc) ([]byte, bool, error) {
	// TODO: support enclosed and escape.
	if len(prevData) == 0 && len(curData) == 0 {
		return nil, false, nil
	}
	var line []byte
	var isEOF, hasStarting, reachLimit bool
	if len(prevData) > 0 && len(curData) == 0 {
		isEOF = true
		prevData, curData = curData, prevData
	}
	for len(curData) > 0 {
		line, curData, hasStarting = l.getLine(prevData, curData)
		prevData = nil
		// If it doesn't find the terminated symbol and this data isn't the last data,
		// the data can't be inserted.
		if line == nil && !isEOF {
			break
		}
		// If doesn't find starting symbol, this data can't be inserted.
		if !hasStarting {
			if isEOF {
				curData = nil
			}
			break
		}
		if line == nil && isEOF {
			line = curData[len(l.LinesInfo.Starting):]
			curData = nil
		}

		if wantField {
			cols, err := l.GetRowFromLine(line)
			if err != nil {
				return nil, false, err
			}
			*rows = append(*rows, cols)
		} else {
			*rows = append(*rows, line)
		}
		if l.maxRowsInBatch != 0 && len(*rows) == l.maxRowsInBatch {
			reachLimit = true
			if err := callback(); err != nil {
				return nil, false, err
			}
			*rows = make([]interface{}, 0)
			break
		}
	}
	return curData, reachLimit, nil
}

// GetRowsToLoad get rows from c.ReadPacket()
func (l *LoadDataInfo) GetRowsToLoad(prevData, curData []byte, c *mysql.Conn, rows *[]interface{}, tb *vindexes.Table, wantField bool, callback LoadDataFunc) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = l.GetRowsFromPacket(prevData, curData, rows, tb, wantField, callback)
		if err != nil {
			return nil, err
		}
		if !reachLimit {
			break
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}

// GetAutoColumnIndexForLine ...从数据流中获取自增列所在的序号,如果使用了自增
func (l *LoadDataInfo) GetAutoColumnIndexForLine(tb *vindexes.Table) (int, error) {
	for i, col := range l.Columns {
		if col.String() == tb.AutoIncrement.Column.String() {
			return i, nil
		}
	}

	return -1, nil
}

// GetGeneratedID batch get global auto_incrment for load
func (l *LoadDataInfo) GetGeneratedID(ctx context.Context, vcursor engine.VCursor, keyspace string, generateTable string, dst key.Destination, count int64) (minID int64, maxID int64, err error) {

	rss, _, err := vcursor.ResolveDestinations(ctx, keyspace, nil, []key.Destination{dst})
	if err != nil {
		return 0, 0, vterrors.Wrap(err, "processGenerate")
	}
	if len(rss) != 1 {
		return 0, 0, vterrors.Wrapf(err, "processGenerate len(rss)=%v", len(rss))
	}
	generateSQL := fmt.Sprintf("select next :n values from %s", generateTable)
	bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
	qr, err := vcursor.ExecuteStandalone(ctx, nil, generateSQL, bindVars, rss[0])
	if err != nil {
		return 0, 0, err
	}
	// If no rows are returned, it's an internal error, and the code
	// must panic, which will be caught and reported.
	minID, err = qr.Rows[0][0].ToInt64()
	if err != nil {
		return 0, 0, err
	}
	return minID, minID + count, nil
}

// processPrimary maps the primary vindex values to the keyspace id
func (l *LoadDataInfo) processPrimary(ctx context.Context, vcursor engine.VCursor, vindexKeys [][]sqltypes.Value, colVindex *vindexes.ColumnVindex, logStats *logstats.LogStats) ([][]byte, error) {

	destinations, err := vindexes.Map(ctx, colVindex.Vindex, vcursor, vindexKeys)
	if err != nil {
		return nil, err
	}

	keyspaceIDs := make([][]byte, len(destinations))
	for i, destination := range destinations {
		switch d := destination.(type) {
		case key.DestinationKeyspaceID:
			// This is a single keyspace id, we're good.
			keyspaceIDs[i] = d
			/*		case key.DestinationNone:
					// No valid keyspace id, we may return an error.
					if ins.Opcode != InsertShardedIgnore {
						return nil, fmt.Errorf("could not map %v to a keyspace id", flattenedVindexKeys[i])
					}*/
		default:
			return nil, fmt.Errorf("could not map %v to a unique keyspace id: %v", vindexKeys[i], destination)
		}
	}
	return keyspaceIDs, nil
}

func (l *LoadDataInfo) getLoadShardedRoute(ctx context.Context, vcursor engine.VCursor, table *vindexes.Table, rows [][]string, vindexColumnIndex []int) (map[*srvtopo.ResolvedShard][][]string, error) {

	vindexRowsValues := make([][]sqltypes.Value, len(rows))

	rowCount := 0
	rowMap := make(map[int][]string)
	for vIdx, row := range rows {
		rowMap[vIdx] = row
		for colIdx, colValues := range vindexColumnIndex {
			// This is the first iteration: allocate for transpose.
			if colIdx == 0 {
				if len(vindexColumnIndex) == 0 {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "no vincex found for %v ", vindexColumnIndex)
				}
				if rowCount == 0 {
					rowCount = len(vindexColumnIndex)
				}
				if rowCount != len(vindexColumnIndex) {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: uneven row values for inserts: %d %d", rowCount, len(vindexColumnIndex))
				}
				vindexRowsValues[vIdx] = make([]sqltypes.Value, rowCount)
				if colValues > len(row)-1 {
					return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "load fields termined err ,please check")
				}
				v, err := sqltypes.NewValue(sqltypes.Char, []byte(row[colValues]))
				if err != nil {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: uneven row values for inserts: %d %d", rowCount, len(vindexColumnIndex))
				}
				vindexRowsValues[vIdx][0] = v
				continue
			}
			// Perform the transpose.
			//[]sqltypes.Value
			//sqltypes.TypeToMySQL()
			v, err := sqltypes.NewValue(sqltypes.Char, []byte(row[colValues]))
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: uneven row values for inserts: %d %d", rowCount, len(vindexColumnIndex))
			}
			vindexRowsValues[vIdx] = append(vindexRowsValues[vIdx], v)
		}
	}

	keyspaceIDs, err := l.processPrimary(ctx, vcursor, vindexRowsValues, table.ColumnVindexes[0], nil)
	if err != nil {
		return nil, err
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
		return nil, nil
	}

	rss, indexesPerRss, err := vcursor.ResolveDestinations(ctx, table.Keyspace.Name, indexes, destinations)
	if err != nil {
		return nil, err
	}
	rssLinesMap := make(map[*srvtopo.ResolvedShard][][]string)
	for index, rs := range rss {
		for _, rowIndex := range indexesPerRss[index] {
			index, _ := strconv.ParseInt(string(rowIndex.Value), 0, 32)
			if _, ok := rssLinesMap[rs]; ok {
				rssLinesMap[rs] = append(rssLinesMap[rs], rowMap[int(index)])
			} else {
				rsList := [][]string{rowMap[int(index)]}
				rssLinesMap[rs] = rsList
			}
		}

	}

	return rssLinesMap, nil
}

// LoadDataInfileDataStream for single shard or pinned table
func (l *LoadDataInfo) LoadDataInfileDataStream(ctx context.Context, vCursor engine.VCursor, c *mysql.Conn, table *vindexes.Table,
	safeSession *SafeSession, tabletType topodata.TabletType, logStats *logstats.LogStats, lines chan string, wantField bool) error {
	// Add some kind of timeout too.
	var shouldBreak bool
	var prevData, curData []byte
	var err error
	var rows = make([]interface{}, 0)
	var idmin int64
	var idmax int64

	var autIndex int
	if table.AutoIncrement != nil {
		idmin, idmax, err = l.GetGeneratedID(ctx, vCursor, table.AutoIncrement.Sequence.Keyspace.Name, table.AutoIncrement.Sequence.Name.String(), key.DestinationKeyspaceID(table.AutoIncrement.Sequence.Pinned), 1000)
		if err != nil {
			log.Errorf("get seq for %v err:%s", table, err.Error())
			return err
		}
		autIndex, err = l.GetAutoColumnIndexForLine(table)
		if err != nil {
			log.Errorf("get autoindex for %v err:%s", table, err.Error())
			return err
		}
	}
	for {
		curData, err = c.ReadPacket()
		if err != nil {
			if err == io.EOF {
				l.LoadDataDone = true
				break
			}
		}
		if len(curData) == 0 {
			shouldBreak = true
			if len(prevData) == 0 {
				l.LoadDataDone = true
				break
			}
		}
		if prevData, err = l.GetRowsToLoad(prevData, curData, c, &rows, table, wantField, func() error {
			// load data retry ExecuteMerge
			if len(rows) == 0 {
				return nil
			}

			//no global AutoIncrement
			if table.AutoIncrement == nil {
				for _, row := range rows {
					if wantField {
						columns, _ := row.([]string)
						lines <- strings.Join(columns, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
					} else {
						lines <- string(row.([]byte)) + l.LinesInfo.Terminated
					}
				}
			} else if autIndex == -1 && l.Columns != nil {
				log.Errorf("auto column must be in values for %v err:%s", table)
				return fmt.Errorf("auto column must be in values")
			} else {
				for _, row := range rows {
					columns, _ := row.([]string)
					if strings.ToLower(columns[autIndex]) == string("null") || strings.ToLower(columns[autIndex]) == "" || columns[autIndex] == "\\N" {
						if idmin == idmax {
							idmin, idmax, err = l.GetGeneratedID(ctx, vCursor, table.AutoIncrement.Sequence.Keyspace.Name, table.AutoIncrement.Sequence.Name.String(), key.DestinationKeyspaceID(table.AutoIncrement.Sequence.Pinned), 1000)
							if err != nil {
								log.Errorf("get seq for %v err:%s", table, err.Error())
								return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "GetGeneratedID for %s:%s", table.Name, err.Error())
							}
						}
						columns[autIndex] = strconv.FormatInt(idmin, 10)
						idmin = idmin + 1
					}
					//global auto_incrment not used in single shard
					if wantField {
						lines <- strings.Join(columns, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
					} else {
						lines <- string(row.([]byte)) + l.LinesInfo.Terminated
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if shouldBreak {
			l.LoadDataDone = true
			break
		}
	}
	if len(rows) > 0 {
		if table.AutoIncrement == nil {
			for _, row := range rows {
				if wantField {
					columns, _ := row.([]string)
					lines <- strings.Join(columns, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
				} else {
					lines <- string(row.([]byte)) + l.LinesInfo.Terminated
				}
			}
		} else if autIndex == -1 {
			log.Errorf("auto column must be in values for %v err:%s", table)
			return fmt.Errorf("auto column must be in values")
		} else {
			for _, row := range rows {
				columns, _ := row.([]string)
				if strings.ToLower(columns[autIndex]) == string("null") || strings.ToLower(columns[autIndex]) == "" || columns[autIndex] == "\\N" {
					if idmin == idmax {
						idmin, idmax, err = l.GetGeneratedID(ctx, vCursor, table.AutoIncrement.Sequence.Keyspace.Name, table.AutoIncrement.Sequence.Name.String(), key.DestinationKeyspaceID(table.AutoIncrement.Sequence.Pinned), 1000)
						if err != nil {
							return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "GetGeneratedID for %s:%s", table.Name, err.Error())
						}
					}
					columns[autIndex] = strconv.FormatInt(idmin, 10)
					idmin = idmin + 1
				}
				if wantField {
					lines <- strings.Join(columns, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
				} else {
					lines <- string(row.([]byte)) + l.LinesInfo.Terminated
				}
			}
		}
	}
	return nil
}

// ExecLoadDataUpstream  LoadDataInfileDataStream for single shard or pinned table
func (l *LoadDataInfo) ExecLoadDataUpstream(ctx context.Context, vCursor engine.VCursor, c *mysql.Conn, table *vindexes.Table, safeSession *SafeSession,
	tabletType topodata.TabletType, logStats *logstats.LogStats, lines chan string, errs chan error, wantField bool, sql string) (*sqltypes.Result, error) {
	count := 0
	linesTdo := make([][]string, 0)
	shardLinesMap := make(map[string]*chan string)
	shardResult := make(chan *sqltypes.Result)
	shardErr := make(chan error)
	endResult := &sqltypes.Result{}

	//get column index for  vindex fro per row
	var vindexColumnIndex []int
	for _, VindexColumnName := range table.ColumnVindexes[0].Columns {
		for index, column := range l.Columns {
			if column.String() == VindexColumnName.String() {
				vindexColumnIndex = append(vindexColumnIndex, index)
			}
		}
	}

	select {
	case err := <-errs:
		return endResult, err
	default:
		ticker := time.NewTicker(time.Second * 1)
		<-ticker.C
	}

	for line := range lines {
		select {
		case err := <-errs:
			if err != nil {
				return endResult, err
			}
		default:
		}
		count = count + 1
		linesTdo = append(linesTdo, strings.Split(strings.Trim(line, l.LinesInfo.Terminated), l.FieldsInfo.Terminated))
		if count == l.maxRowsInBatch {
			linesPerShards, err := l.getLoadShardedRoute(ctx, vCursor, table, linesTdo, vindexColumnIndex)
			if err != nil {
				return endResult, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "getLoadShardedRoute failed  for %v", vindexColumnIndex)
			}
			if linesPerShards == nil {
				return endResult, err
			}
			for shard, lines := range linesPerShards {
				if _, ok := shardLinesMap[shard.Target.Shard]; !ok {
					shardChan := make(chan string, 10000)
					shardLinesMap[shard.Target.Shard] = &shardChan
					go func(ctx context.Context, safeSession *SafeSession, rs *srvtopo.ResolvedShard, lines *chan string, sql string, resultChan chan *sqltypes.Result, errChan chan error) {
						var result *sqltypes.Result
						var err error
						defer func() {
							errRecvoer := recover()
							if errRecvoer != nil {
								shardErr <- fmt.Errorf("%v", errRecvoer)
							} else {
								shardErr <- err
							}
							shardResult <- result
						}()
						result, err = l.e.scatterConn.streamUpload(ctx, safeSession, rs, *lines, sql)
					}(ctx, safeSession, shard, &shardChan, sql, shardResult, shardErr)
				}
				shardChan := shardLinesMap[shard.Target.Shard]
				for _, line := range lines {
					if line != nil {
						*shardChan <- strings.Join(line, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
					}
				}
				//close(*shardChan)
			}
			count = 0
			linesTdo = linesTdo[:0]
		}
	}

	if len(linesTdo) >= 1 {
		linesPerShards, err := l.getLoadShardedRoute(ctx, vCursor, table, linesTdo, vindexColumnIndex)
		if err != nil {
			return endResult, err
		}
		if linesPerShards == nil {
			return endResult, err
		}
		for shard, lines := range linesPerShards {
			if _, ok := shardLinesMap[shard.Target.Shard]; !ok {
				shardChan := make(chan string, 10000)
				shardLinesMap[shard.Target.Shard] = &shardChan
				go func(ctx context.Context, safeSession *SafeSession, rs *srvtopo.ResolvedShard, lines *chan string, sql string, resultChan chan *sqltypes.Result, errChan chan error) {
					var result *sqltypes.Result
					var err error
					defer func() {
						errRecvoer := recover()
						if errRecvoer != nil {
							shardErr <- fmt.Errorf("%v", errRecvoer)
						} else {
							shardErr <- err
						}
						shardResult <- result
						//close(lines)
					}()
					result, err = l.e.scatterConn.streamUpload(ctx, safeSession, rs, *lines, sql)
				}(ctx, safeSession, shard, &shardChan, sql, shardResult, shardErr)
			}
			shardChan := shardLinesMap[shard.Target.Shard]
			for _, line := range lines {
				if line != nil {
					*shardChan <- strings.Join(line, l.FieldsInfo.Terminated) + l.LinesInfo.Terminated
				}
			}
		}
	}

	allShard, _, err := l.e.resolver.resolver.GetAllShards(ctx, table.Keyspace.Name, defaultTabletType)
	if err != nil {
		return endResult, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "GetAllShards for %s, err%s", table.Keyspace.Name, err.Error())
	}

	//shard not having data to load
	if len(shardLinesMap) < len(allShard) {
		for _, shard := range allShard {
			if _, ok := shardLinesMap[shard.Target.Shard]; !ok {
				shardChan := make(chan string, 10000)
				shardLinesMap[shard.Target.Shard] = &shardChan
				go func(ctx context.Context, safeSession *SafeSession, rs *srvtopo.ResolvedShard, lines *chan string, sql string, resultChan chan *sqltypes.Result, errChan chan error) {
					var result *sqltypes.Result
					var err error
					defer func() {
						errRecvoer := recover()
						if errRecvoer != nil {
							shardErr <- fmt.Errorf("%v", errRecvoer)
						} else {
							shardErr <- err
						}
						shardResult <- result
						//close(lines)
					}()
					result, err = l.e.scatterConn.streamUpload(ctx, safeSession, rs, *lines, sql)
				}(ctx, safeSession, shard, &shardChan, sql, shardResult, shardErr)
			}
		}

	}

	//close input chan
	for _, linesChan := range shardLinesMap {
		close(*linesChan)
	}
	allErr := make([]string, 0)
	for i := 0; i < len(shardLinesMap); i++ {
		err := <-shardErr
		if err != nil {
			allErr = append(allErr, err.Error())
		}
		result := <-shardResult
		if result != nil {
			endResult.RowsAffected = endResult.RowsAffected + result.RowsAffected
		}
	}

	if len(allErr) >= 1 {
		return endResult, fmt.Errorf("%s", strings.Join(allErr, ""))
	}
	return endResult, nil
}

func escapeCols(strs [][]byte) []string {
	ret := make([]string, len(strs))
	for i, v := range strs {
		//	[]byte{92,78} represent \\N   []byte{92,78,13} represent　\\N\r　[]byte{78, 85, 76, 76} represent NULL
		if bytes.Equal(v, []byte{92, 78}) || bytes.Equal(v, []byte{92, 78, 13}) || bytes.Equal(v, []byte{78, 85, 76, 76}) {
			v = []byte{92, 78}
		}
		//output := escape(v)
		ret[i] = string(v)
	}
	return ret
}

// escape handles escape characters when running load data statement.
// TODO: escape need to be improved, it should support ESCAPED BY to specify
// the escape character and handle \N escape.
// See http://dev.mysql.com/doc/refman/5.7/en/load-data.html
func escape(str []byte) []byte {
	desc := make([]byte, len(str)*2)
	pos := 0
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c == '\\' && i+1 < len(str) {
			c = escapeChar(str[i+1])
			desc[pos] = c
			i++
			pos++
		} else if c == '"' || c == '\'' {
			desc[pos] = '\\'
			desc[pos+1] = c
			pos += 2
		} else {
			desc[pos] = c
			pos++
		}
	}
	return desc[:pos]
}

func escapeChar(c byte) byte {
	switch c {
	case '0':
		return 0
	case 'b':
		return '\b'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'Z':
		return 26
	case '\\':
		return '\\'
	}
	return c
}

func isNumericType(t querypb.Type, column string) bool {
	if column == "NULL" {
		return true
	}
	if t == querypb.Type_INT8 ||
		t == querypb.Type_INT16 ||
		t == querypb.Type_INT24 ||
		t == querypb.Type_INT32 ||
		t == querypb.Type_INT64 ||
		t == querypb.Type_UINT8 ||
		t == querypb.Type_UINT16 ||
		t == querypb.Type_UINT24 ||
		t == querypb.Type_UINT32 ||
		t == querypb.Type_UINT64 ||
		t == querypb.Type_FLOAT32 ||
		t == querypb.Type_FLOAT64 ||
		t == querypb.Type_DECIMAL {
		return true
	}
	return false
}
