// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tidb

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var extraHandleTableColumn = &table.Column{
	ColumnInfo:    kv.ExtraHandleColumnInfo,
	GeneratedExpr: nil,
	DefaultExpr:   nil,
}

const (
	writeRowsMaxRetryTimes = 3
	// To limit memory usage for prepared statements.
	prepStmtCacheSize uint = 100
)

type tidbRow struct {
	insertStmt         string
	preparedInsertStmt string
	values             []any
	path               string
	offset             int64
}

var emptyTiDBRow = tidbRow{
	insertStmt: "",
	path:       "",
	offset:     0,
}

type tidbRows []tidbRow

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (rows tidbRows) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range rows {
		encoder.AppendString(redact.Value(r.insertStmt))
	}
	return nil
}

type tidbEncoder struct {
	mode mysql.SQLMode
	tbl  table.Table
	// the index of table columns for each data field.
	// index == len(table.columns) means this field is `_tidb_rowid`
	columnIdx []int
	// the max index used in this chunk, due to the ignore-columns config, we can't
	// directly check the total column count, so we fall back to only check that
	// the there are enough columns.
	columnCnt int
	// data file path
	path     string
	logger   log.Logger
	prepStmt bool
}

type encodingBuilder struct{}

// NewEncodingBuilder creates an EncodingBuilder with TiDB backend implementation.
func NewEncodingBuilder() encode.EncodingBuilder {
	return new(encodingBuilder)
}

// NewEncoder creates a KV encoder.
// It implements the `backend.EncodingBuilder` interface.
func (*encodingBuilder) NewEncoder(_ context.Context, config *encode.EncodingConfig) (encode.Encoder, error) {
	return &tidbEncoder{
		mode:     config.SQLMode,
		tbl:      config.Table,
		path:     config.Path,
		logger:   config.Logger,
		prepStmt: config.LogicalImportPrepStmt,
	}, nil
}

// MakeEmptyRows creates an empty KV rows.
// It implements the `backend.EncodingBuilder` interface.
func (*encodingBuilder) MakeEmptyRows() encode.Rows {
	return tidbRows(nil)
}

type targetInfoGetter struct {
	db *sql.DB
}

// NewTargetInfoGetter creates an TargetInfoGetter with TiDB backend implementation.
func NewTargetInfoGetter(db *sql.DB) backend.TargetInfoGetter {
	return &targetInfoGetter{
		db: db,
	}
}

// FetchRemoteDBModels implements the `backend.TargetInfoGetter` interface.
func (b *targetInfoGetter) FetchRemoteDBModels(ctx context.Context) ([]*model.DBInfo, error) {
	results := []*model.DBInfo{}
	logger := log.Wrap(logutil.Logger(ctx))
	s := common.SQLWithRetry{
		DB:     b.db,
		Logger: logger,
	}
	err := s.Transact(ctx, "fetch db models", func(_ context.Context, tx *sql.Tx) error {
		results = results[:0]

		rows, e := tx.Query("SHOW DATABASES")
		if e != nil {
			return e
		}
		defer rows.Close()

		for rows.Next() {
			var dbName string
			if e := rows.Scan(&dbName); e != nil {
				return e
			}
			dbInfo := &model.DBInfo{
				Name: ast.NewCIStr(dbName),
			}
			results = append(results, dbInfo)
		}
		return rows.Err()
	})
	return results, err
}

// exported for test.
var (
	FetchRemoteTableModelsConcurrency = 8
	FetchRemoteTableModelsBatchSize   = 32
)

// FetchRemoteTableModels implements the `backend.TargetInfoGetter` interface.
func (b *targetInfoGetter) FetchRemoteTableModels(
	ctx context.Context,
	schemaName string,
	tableNames []string,
) (map[string]*model.TableInfo, error) {
	tableInfos := make([]*model.TableInfo, len(tableNames))
	logger := log.Wrap(logutil.Logger(ctx))
	s := common.SQLWithRetry{
		DB:     b.db,
		Logger: logger,
	}

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	eg.SetLimit(FetchRemoteTableModelsConcurrency)
	for i := 0; i < len(tableNames); i += FetchRemoteTableModelsBatchSize {
		start := i
		end := min(i+FetchRemoteTableModelsBatchSize, len(tableNames))
		eg.Go(func() error {
			return s.Transact(
				egCtx, "fetch table columns",
				func(_ context.Context, tx *sql.Tx) error {
					args := make([]any, 0, 1+end-start)
					args = append(args, schemaName)
					for _, tableName := range tableNames[start:end] {
						args = append(args, tableName)
					}
					//nolint:gosec
					rows, err := tx.Query(`
						SELECT table_name, column_name, column_type, generation_expression, extra
						FROM information_schema.columns
						WHERE table_schema = ? AND table_name IN (?`+strings.Repeat(",?", end-start-1)+`)
						ORDER BY table_name, ordinal_position;
					`, args...)
					if err != nil {
						return err
					}
					defer rows.Close()

					var (
						curTableName string
						curColOffset int
						curTable     *model.TableInfo
						tableIdx     = start - 1
					)
					for rows.Next() {
						var tableName, columnName, columnType, generationExpr, columnExtra string
						if err2 := rows.Scan(&tableName, &columnName, &columnType, &generationExpr, &columnExtra); err2 != nil {
							return err2
						}
						if tableName != curTableName {
							tableIdx++
							curTable = &model.TableInfo{
								Name:       ast.NewCIStr(tableName),
								State:      model.StatePublic,
								PKIsHandle: true,
							}
							tableInfos[tableIdx] = curTable
							curTableName = tableName
							curColOffset = 0
						}

						// see: https://github.com/pingcap/parser/blob/3b2fb4b41d73710bc6c4e1f4e8679d8be6a4863e/types/field_type.go#L185-L191
						var flag uint
						if strings.HasSuffix(columnType, "unsigned") {
							flag |= mysql.UnsignedFlag
						}
						if strings.Contains(columnExtra, "auto_increment") {
							flag |= mysql.AutoIncrementFlag
						}

						ft := types.FieldType{}
						ft.SetFlag(flag)
						curTable.Columns = append(curTable.Columns, &model.ColumnInfo{
							Name:                ast.NewCIStr(columnName),
							Offset:              curColOffset,
							State:               model.StatePublic,
							FieldType:           ft,
							GeneratedExprString: generationExpr,
						})
						curColOffset++
					}
					if err := rows.Err(); err != nil {
						return err
					}

					failpoint.Inject(
						"FetchRemoteTableModels_BeforeFetchTableAutoIDInfos",
						func() {
							fmt.Println("failpoint: FetchRemoteTableModels_BeforeFetchTableAutoIDInfos")
						},
					)

					// init auto id column for each table
					for idx := start; idx <= tableIdx; idx++ {
						tbl := tableInfos[idx]
						tblName := common.UniqueTable(schemaName, tbl.Name.O)
						autoIDInfos, err := FetchTableAutoIDInfos(ctx, tx, tblName)
						if err != nil {
							logger.Warn(
								"fetch table auto ID infos error. Ignore this table and continue.",
								zap.String("table_name", tblName),
								zap.Error(err),
							)
							tableInfos[idx] = nil
							continue
						}
						for _, info := range autoIDInfos {
							for _, col := range tbl.Columns {
								if col.Name.O == info.Column {
									switch info.Type {
									case "AUTO_INCREMENT":
										col.AddFlag(mysql.AutoIncrementFlag)
									case "AUTO_RANDOM":
										col.AddFlag(mysql.PriKeyFlag)
										tbl.PKIsHandle = true
										// set a stub here, since we don't really need the real value
										tbl.AutoRandomBits = 1
									}
								}
							}
						}
					}
					return nil
				})
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	ret := make(map[string]*model.TableInfo, len(tableInfos))
	for _, tbl := range tableInfos {
		if tbl != nil {
			ret[tbl.Name.L] = tbl
		}
	}

	return ret, nil
}

// CheckRequirements performs the check whether the backend satisfies the version requirements.
// It implements the `backend.TargetInfoGetter` interface.
func (*targetInfoGetter) CheckRequirements(ctx context.Context, _ *backend.CheckCtx) error {
	logutil.Logger(ctx).Info("skipping check requirements for tidb backend")
	return nil
}

// stmtKey defines key for stmtCache.
type stmtKey struct {
	query string
}

// Hash implements SimpleLRUCache.Key.
func (k *stmtKey) Hash() []byte {
	return hack.Slice(k.query)
}

type tidbBackend struct {
	db          *sql.DB
	conflictCfg config.Conflict
	// onDuplicate is the type of INSERT SQL. It may be different with
	// conflictCfg.Strategy to implement other feature, but the behaviour in caller's
	// view should be the same.
	onDuplicate config.DuplicateResolutionAlgorithm
	errorMgr    *errormanager.ErrorManager
	// maxChunkSize and maxChunkRows are the target size and number of rows of each INSERT SQL
	// statement to be sent to downstream. Sometimes we want to reduce the txn size to avoid
	// affecting the cluster too much.
	maxChunkSize uint64
	maxChunkRows int
	// implement stmtCache to improve performance
	stmtCache      *kvcache.SimpleLRUCache
	stmtCacheMutex sync.RWMutex
}

var _ backend.Backend = (*tidbBackend)(nil)

// NewTiDBBackend creates a new TiDB backend using the given database.
//
// The backend does not take ownership of `db`. Caller should close `db`
// manually after the backend expired.
func NewTiDBBackend(
	ctx context.Context,
	db *sql.DB,
	cfg *config.Config,
	errorMgr *errormanager.ErrorManager,
) backend.Backend {
	conflict := cfg.Conflict
	var onDuplicate config.DuplicateResolutionAlgorithm
	switch conflict.Strategy {
	case config.ErrorOnDup:
		onDuplicate = config.ErrorOnDup
	case config.ReplaceOnDup:
		onDuplicate = config.ReplaceOnDup
	case config.IgnoreOnDup:
		if conflict.MaxRecordRows == 0 {
			onDuplicate = config.IgnoreOnDup
		} else {
			// need to stop batch insert on error and fall back to row by row insert
			// to record the row
			onDuplicate = config.ErrorOnDup
		}
	default:
		logutil.Logger(ctx).Warn("unsupported conflict strategy for TiDB backend, overwrite with `error`")
		onDuplicate = config.ErrorOnDup
	}
	var stmtCache *kvcache.SimpleLRUCache
	if cfg.TikvImporter.LogicalImportPrepStmt {
		stmtCache = kvcache.NewSimpleLRUCache(prepStmtCacheSize, 0, 0)
		stmtCache.SetOnEvict(func(_ kvcache.Key, value kvcache.Value) {
			stmt := value.(*sql.Stmt)
			stmt.Close()
		})
	}
	return &tidbBackend{
		db:             db,
		conflictCfg:    conflict,
		onDuplicate:    onDuplicate,
		errorMgr:       errorMgr,
		maxChunkSize:   uint64(cfg.TikvImporter.LogicalImportBatchSize),
		maxChunkRows:   cfg.TikvImporter.LogicalImportBatchRows,
		stmtCache:      stmtCache,
		stmtCacheMutex: sync.RWMutex{},
	}
}

func (row tidbRow) Size() uint64 {
	return uint64(len(row.insertStmt))
}

func (row tidbRow) String() string {
	return row.insertStmt
}

func (row tidbRow) ClassifyAndAppend(data *encode.Rows, checksum *verification.KVChecksum, _ *encode.Rows, _ *verification.KVChecksum) {
	rows := (*data).(tidbRows)
	// Cannot do `rows := data.(*tidbRows); *rows = append(*rows, row)`.
	//nolint:gocritic
	*data = append(rows, row)
	cs := verification.MakeKVChecksum(row.Size(), 1, 0)
	checksum.Add(&cs)
}

func (rows tidbRows) splitIntoChunks(splitSize uint64, splitRows int) []tidbRows {
	if len(rows) == 0 {
		return nil
	}

	res := make([]tidbRows, 0, 1)
	i := 0
	cumSize := uint64(0)

	for j, row := range rows {
		if i < j && (cumSize+row.Size() > splitSize || j-i >= splitRows) {
			res = append(res, rows[i:j])
			i = j
			cumSize = 0
		}
		cumSize += row.Size()
	}

	return append(res, rows[i:])
}

func (rows tidbRows) Clear() encode.Rows {
	return rows[:0]
}

func (enc *tidbEncoder) appendSQLBytes(sb *strings.Builder, value []byte) {
	sb.Grow(2 + len(value))
	sb.WriteByte('\'')
	if enc.mode.HasNoBackslashEscapesMode() {
		for _, b := range value {
			if b == '\'' {
				sb.WriteString(`''`)
			} else {
				sb.WriteByte(b)
			}
		}
	} else {
		for _, b := range value {
			switch b {
			case 0:
				sb.WriteString(`\0`)
			case '\b':
				sb.WriteString(`\b`)
			case '\n':
				sb.WriteString(`\n`)
			case '\r':
				sb.WriteString(`\r`)
			case '\t':
				sb.WriteString(`\t`)
			case 26:
				sb.WriteString(`\Z`)
			case '\'':
				sb.WriteString(`''`)
			case '\\':
				sb.WriteString(`\\`)
			default:
				sb.WriteByte(b)
			}
		}
	}
	sb.WriteByte('\'')
}

// appendSQL appends the SQL representation of the Datum into the string builder.
// Note that we cannot use Datum.ToString since it doesn't perform SQL escaping.
func (enc *tidbEncoder) appendSQL(sb *strings.Builder, datum *types.Datum, _ *table.Column) error {
	switch datum.Kind() {
	case types.KindNull:
		sb.WriteString("NULL")

	case types.KindMinNotNull:
		sb.WriteString("MINVALUE")

	case types.KindMaxValue:
		sb.WriteString("MAXVALUE")

	case types.KindInt64:
		// longest int64 = -9223372036854775808 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendInt(buffer[:0], datum.GetInt64(), 10)
		sb.Write(value)

	case types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet:
		// longest uint64 = 18446744073709551615 which has 20 characters
		var buffer [20]byte
		value := strconv.AppendUint(buffer[:0], datum.GetUint64(), 10)
		sb.Write(value)

	case types.KindFloat32, types.KindFloat64:
		// float64 has 16 digits of precision, so a buffer size of 32 is more than enough...
		var buffer [32]byte
		value := strconv.AppendFloat(buffer[:0], datum.GetFloat64(), 'g', -1, 64)
		sb.Write(value)
	case types.KindString:
		// See: https://github.com/pingcap/tidb-lightning/issues/550
		// if enc.mode.HasStrictMode() {
		//	d, err := table.CastValue(enc.se, *datum, col.ToInfo(), false, false)
		//	if err != nil {
		//		return errors.Trace(err)
		//	}
		//	datum = &d
		// }

		enc.appendSQLBytes(sb, datum.GetBytes())
	case types.KindBytes:
		enc.appendSQLBytes(sb, datum.GetBytes())

	case types.KindMysqlJSON:
		value, err := datum.GetMysqlJSON().MarshalJSON()
		if err != nil {
			return err
		}
		enc.appendSQLBytes(sb, value)

	case types.KindBinaryLiteral:
		value := datum.GetBinaryLiteral()
		sb.Grow(3 + 2*len(value))
		sb.WriteString("x'")
		if _, err := hex.NewEncoder(sb).Write(value); err != nil {
			return errors.Trace(err)
		}
		sb.WriteByte('\'')

	case types.KindMysqlBit:
		var buffer [20]byte
		intValue, err := datum.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return err
		}
		value := strconv.AppendUint(buffer[:0], intValue, 10)
		sb.Write(value)

		// time, duration, decimal
	default:
		value, err := datum.ToString()
		if err != nil {
			return err
		}
		sb.WriteByte('\'')
		sb.WriteString(value)
		sb.WriteByte('\'')
	}

	return nil
}

func (*tidbEncoder) Close() {}

func getColumnByIndex(cols []*table.Column, index int) *table.Column {
	if index == len(cols) {
		return extraHandleTableColumn
	}
	return cols[index]
}

func (enc *tidbEncoder) Encode(row []types.Datum, _ int64, columnPermutation []int, offset int64) (encode.Row, error) {
	cols := enc.tbl.Cols()

	if len(enc.columnIdx) == 0 {
		columnMaxIdx := -1
		columnIdx := make([]int, len(columnPermutation))
		for i := range columnPermutation {
			columnIdx[i] = -1
		}
		for i, idx := range columnPermutation {
			if idx >= 0 {
				columnIdx[idx] = i
				if idx > columnMaxIdx {
					columnMaxIdx = idx
				}
			}
		}
		enc.columnIdx = columnIdx
		enc.columnCnt = columnMaxIdx + 1
	}

	// TODO: since the column count doesn't exactly reflect the real column names, we only check the upper bound currently.
	// See: tests/generated_columns/data/gencol.various_types.0.sql this sql has no columns, so encodeLoop will fill the
	// column permutation with default, thus enc.columnCnt > len(row).
	if len(row) < enc.columnCnt {
		// 1. if len(row) < enc.columnCnt: data in row cannot populate the insert statement, because
		// there are enc.columnCnt elements to insert but fewer columns in row
		enc.logger.Error("column count mismatch", zap.Ints("column_permutation", columnPermutation),
			zap.Array("data", kv.RowArrayMarshaller(row)))
		return emptyTiDBRow, errors.Errorf("column count mismatch, expected %d, got %d", enc.columnCnt, len(row))
	}

	if len(row) > len(enc.columnIdx) {
		// 2. if len(row) > len(columnIdx): raw row data has more columns than those
		// in the table
		enc.logger.Error("column count mismatch", zap.Ints("column_count", enc.columnIdx),
			zap.Array("data", kv.RowArrayMarshaller(row)))
		return emptyTiDBRow, errors.Errorf("column count mismatch, at most %d but got %d", len(enc.columnIdx), len(row))
	}

	var encoded, preparedInsertStmt strings.Builder
	var values []any
	encoded.Grow(8 * len(row))
	encoded.WriteByte('(')
	if enc.prepStmt {
		preparedInsertStmt.Grow(2 * len(row))
		preparedInsertStmt.WriteByte('(')
		values = make([]any, 0, len(row))
	}
	cnt := 0
	for i, field := range row {
		if enc.columnIdx[i] < 0 {
			continue
		}
		if cnt > 0 {
			encoded.WriteByte(',')
			if enc.prepStmt {
				preparedInsertStmt.WriteByte(',')
			}
		}
		datum := field
		if err := enc.appendSQL(&encoded, &datum, getColumnByIndex(cols, enc.columnIdx[i])); err != nil {
			enc.logger.Error("tidb encode failed",
				zap.Array("original", kv.RowArrayMarshaller(row)),
				zap.Int("originalCol", i),
				log.ShortError(err),
			)
			return nil, err
		}
		if enc.prepStmt {
			preparedInsertStmt.WriteByte('?')
			values = append(values, datum.GetValue())
		}
		cnt++
	}
	encoded.WriteByte(')')
	if enc.prepStmt {
		preparedInsertStmt.WriteByte(')')
	}

	return tidbRow{
		insertStmt:         encoded.String(),
		preparedInsertStmt: preparedInsertStmt.String(),
		values:             values,
		path:               enc.path,
		offset:             offset,
	}, nil
}

// EncodeRowForRecord encodes a row to a string compatible with INSERT statements.
func EncodeRowForRecord(ctx context.Context, encTable table.Table, sqlMode mysql.SQLMode, row []types.Datum, columnPermutation []int) string {
	enc := tidbEncoder{
		tbl:    encTable,
		mode:   sqlMode,
		logger: log.Wrap(logutil.Logger(ctx)),
	}
	resRow, err := enc.Encode(row, 0, columnPermutation, 0)
	if err != nil {
		// if encode can't succeed, fallback to record the raw input strings
		// ignore the error since it can only happen if the datum type is unknown, this can't happen here.
		datumStr, _ := types.DatumsToString(row, true)
		return datumStr
	}
	return resRow.(tidbRow).insertStmt
}

func (*tidbBackend) Close() {
	// *Not* going to close `be.db`. The db object is normally borrowed from a
	// TidbManager, so we let the manager to close it.
}

func (*tidbBackend) RetryImportDelay() time.Duration {
	return 0
}

func (*tidbBackend) ShouldPostProcess() bool {
	return true
}

func (*tidbBackend) OpenEngine(context.Context, *backend.EngineConfig, uuid.UUID) error {
	return nil
}

func (*tidbBackend) CloseEngine(context.Context, *backend.EngineConfig, uuid.UUID) error {
	return nil
}

func (*tidbBackend) CleanupEngine(context.Context, uuid.UUID) error {
	return nil
}

func (*tidbBackend) ImportEngine(context.Context, uuid.UUID, int64, int64) error {
	return nil
}

func (be *tidbBackend) WriteRows(ctx context.Context, tableName string, columnNames []string, rows encode.Rows) error {
	var err error
rowLoop:
	for _, r := range rows.(tidbRows).splitIntoChunks(be.maxChunkSize, be.maxChunkRows) {
		for range writeRowsMaxRetryTimes {
			// Write in the batch mode first.
			err = be.WriteBatchRowsToDB(ctx, tableName, columnNames, r)
			switch {
			case err == nil:
				continue rowLoop
			case common.IsRetryableError(err):
				// retry next loop
			case be.errorMgr.TypeErrorsRemain() > 0 ||
				be.errorMgr.ConflictErrorsRemain() > 0 ||
				(be.conflictCfg.Strategy == config.ErrorOnDup && !be.errorMgr.RecordErrorOnce()):
				// WriteBatchRowsToDB failed in the batch mode and can not be retried,
				// we need to redo the writing row-by-row to find where the error locates (and skip it correctly in future).
				if err = be.WriteRowsToDB(ctx, tableName, columnNames, r); err != nil {
					// If the error is not nil, it means we reach the max error count in the
					// non-batch mode or this is "error" conflict strategy.
					return errors.Annotatef(err, "[%s] write rows exceed conflict threshold", tableName)
				}
				continue rowLoop
			default:
				return err
			}
		}
		return errors.Annotatef(err, "[%s] batch write rows reach max retry %d and still failed", tableName, writeRowsMaxRetryTimes)
	}
	return nil
}

type stmtTask struct {
	rows   tidbRows
	stmt   string
	values []any
}

// WriteBatchRowsToDB write rows in batch mode, which will insert multiple rows like this:
//
//	insert into t1 values (111), (222), (333), (444);
func (be *tidbBackend) WriteBatchRowsToDB(ctx context.Context, tableName string, columnNames []string, rows tidbRows) error {
	insertStmt := be.checkAndBuildStmt(rows, tableName, columnNames)
	if insertStmt == nil {
		return nil
	}
	// Note: we are not going to do interpolation (prepared statements) to avoid
	// complication arise from data length overflow of BIT and BINARY columns
	var values []any
	if be.stmtCache != nil && len(rows) > 0 {
		values = make([]any, 0, len(rows[0].values)*len(rows))
	}
	stmtTasks := make([]stmtTask, 1)
	for i, row := range rows {
		if i != 0 {
			insertStmt.WriteByte(',')
		}
		if be.stmtCache != nil {
			insertStmt.WriteString(row.preparedInsertStmt)
			values = append(values, row.values...)
		} else {
			insertStmt.WriteString(row.insertStmt)
		}
	}
	stmtTasks[0] = stmtTask{rows, insertStmt.String(), values}
	return be.execStmts(ctx, stmtTasks, tableName, true)
}

func (be *tidbBackend) checkAndBuildStmt(rows tidbRows, tableName string, columnNames []string) *strings.Builder {
	if len(rows) == 0 {
		return nil
	}
	return be.buildStmt(tableName, columnNames)
}

// WriteRowsToDB write rows in row-by-row mode, which will insert multiple rows like this:
//
//	insert into t1 values (111);
//	insert into t1 values (222);
//	insert into t1 values (333);
//	insert into t1 values (444);
//
// See more details in br#1366: https://github.com/pingcap/br/issues/1366
func (be *tidbBackend) WriteRowsToDB(ctx context.Context, tableName string, columnNames []string, rows tidbRows) error {
	insertStmt := be.checkAndBuildStmt(rows, tableName, columnNames)
	if insertStmt == nil {
		return nil
	}
	is := insertStmt.String()
	stmtTasks := make([]stmtTask, 0, len(rows))
	for _, row := range rows {
		var finalInsertStmt strings.Builder
		finalInsertStmt.WriteString(is)
		if be.stmtCache != nil {
			finalInsertStmt.WriteString(row.preparedInsertStmt)
		} else {
			finalInsertStmt.WriteString(row.insertStmt)
		}
		stmtTasks = append(stmtTasks, stmtTask{[]tidbRow{row}, finalInsertStmt.String(), row.values})
	}
	return be.execStmts(ctx, stmtTasks, tableName, false)
}

func (be *tidbBackend) buildStmt(tableName string, columnNames []string) *strings.Builder {
	var insertStmt strings.Builder
	switch be.onDuplicate {
	case config.ReplaceOnDup:
		insertStmt.WriteString("REPLACE INTO ")
	case config.IgnoreOnDup:
		insertStmt.WriteString("INSERT IGNORE INTO ")
	case config.ErrorOnDup:
		insertStmt.WriteString("INSERT INTO ")
	}
	insertStmt.WriteString(tableName)
	if len(columnNames) > 0 {
		insertStmt.WriteByte('(')
		for i, colName := range columnNames {
			if i != 0 {
				insertStmt.WriteByte(',')
			}
			common.WriteMySQLIdentifier(&insertStmt, colName)
		}
		insertStmt.WriteByte(')')
	}
	insertStmt.WriteString(" VALUES")
	return &insertStmt
}

func (be *tidbBackend) execStmts(ctx context.Context, stmtTasks []stmtTask, tableName string, batch bool) error {
stmtLoop:
	for _, stmtTask := range stmtTasks {
		var (
			result sql.Result
			err    error
		)
		for range writeRowsMaxRetryTimes {
			query := stmtTask.stmt
			if be.stmtCache != nil {
				var prepStmt *sql.Stmt
				key := &stmtKey{query: query}
				be.stmtCacheMutex.RLock()
				stmt, ok := be.stmtCache.Get(key)
				be.stmtCacheMutex.RUnlock()
				if ok {
					prepStmt = stmt.(*sql.Stmt)
				} else if stmt, err := be.db.PrepareContext(ctx, query); err == nil {
					be.stmtCacheMutex.Lock()
					// check again if the key is already in the cache
					// to avoid override existing stmt without closing it
					if cachedStmt, ok := be.stmtCache.Get(key); !ok {
						prepStmt = stmt
						be.stmtCache.Put(key, stmt)
					} else {
						prepStmt = cachedStmt.(*sql.Stmt)
						stmt.Close()
					}
					be.stmtCacheMutex.Unlock()
				} else {
					return errors.Trace(err)
				}
				result, err = prepStmt.ExecContext(ctx, stmtTask.values...)
			} else {
				result, err = be.db.ExecContext(ctx, query)
			}
			if err == nil {
				affected, err2 := result.RowsAffected()
				if err2 != nil {
					// should not happen
					return errors.Trace(err2)
				}
				diff := int64(len(stmtTask.rows)) - affected
				if diff < 0 {
					diff = -diff
				}
				if diff > 0 {
					if err2 = be.errorMgr.RecordDuplicateCount(diff); err2 != nil {
						return err2
					}
				}
				continue stmtLoop
			}

			if !common.IsContextCanceledError(err) {
				logutil.Logger(ctx).Error("execute statement failed",
					zap.Array("rows", stmtTask.rows), zap.String("stmt", redact.Value(query)), zap.Error(err))
			}
			// It's batch mode, just return the error. Caller will fall back to row-by-row mode.
			if batch {
				return errors.Trace(err)
			}
			if !common.IsRetryableError(err) {
				break
			}
		}

		firstRow := stmtTask.rows[0]

		if isDupEntryError(err) {
			// rowID is ignored in tidb backend
			if be.conflictCfg.Strategy == config.ErrorOnDup {
				be.errorMgr.RecordDuplicateOnce(
					ctx,
					log.Wrap(logutil.Logger(ctx)),
					tableName,
					firstRow.path,
					firstRow.offset,
					err.Error(),
					0,
					firstRow.insertStmt,
				)
				return err
			}
			err = be.errorMgr.RecordDuplicate(
				ctx,
				log.Wrap(logutil.Logger(ctx)),
				tableName,
				firstRow.path,
				firstRow.offset,
				err.Error(),
				0,
				firstRow.insertStmt,
			)
		} else {
			err = be.errorMgr.RecordTypeError(
				ctx,
				log.Wrap(logutil.Logger(ctx)),
				tableName,
				firstRow.path,
				firstRow.offset,
				firstRow.insertStmt,
				err,
			)
		}
		if err != nil {
			return errors.Trace(err)
		}
		// max-error not yet reached (error consumed by errorMgr), proceed to next stmtTask.
	}
	failpoint.Inject("FailIfImportedSomeRows", func() {
		panic("forcing failure due to FailIfImportedSomeRows, before saving checkpoint")
	})
	return nil
}

func isDupEntryError(err error) bool {
	merr, ok := errors.Cause(err).(*gmysql.MySQLError)
	if !ok {
		return false
	}
	return merr.Number == errno.ErrDupEntry
}

// FlushEngine flushes the data in the engine to the underlying storage.
func (*tidbBackend) FlushEngine(context.Context, uuid.UUID) error {
	return nil
}

// FlushAllEngines flushes all the data in the engines to the underlying storage.
func (*tidbBackend) FlushAllEngines(context.Context) error {
	return nil
}

// LocalWriter returns a writer that writes data to local storage.
func (be *tidbBackend) LocalWriter(
	_ context.Context,
	cfg *backend.LocalWriterConfig,
	_ uuid.UUID,
) (backend.EngineWriter, error) {
	return &Writer{be: be, tableName: cfg.TiDB.TableName}, nil
}

// Writer is a writer that writes data to local storage.
type Writer struct {
	be        *tidbBackend
	tableName string
}

// Close implements the EngineWriter interface.
func (*Writer) Close(_ context.Context) (backend.ChunkFlushStatus, error) {
	return nil, nil
}

// AppendRows implements the EngineWriter interface.
func (w *Writer) AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error {
	return w.be.WriteRows(ctx, w.tableName, columnNames, rows)
}

// IsSynced implements the EngineWriter interface.
func (*Writer) IsSynced() bool {
	return true
}

// TableAutoIDInfo is the auto id information of a table.
type TableAutoIDInfo struct {
	Column string
	NextID uint64
	Type   string
}

// FetchTableAutoIDInfos fetches the auto id information of a table.
func FetchTableAutoIDInfos(ctx context.Context, exec dbutil.QueryExecutor, tableName string) ([]*TableAutoIDInfo, error) {
	rows, e := exec.QueryContext(ctx, fmt.Sprintf("SHOW TABLE %s NEXT_ROW_ID", tableName))
	if e != nil {
		return nil, errors.Trace(e)
	}
	var autoIDInfos []*TableAutoIDInfo
	for rows.Next() {
		var (
			dbName, tblName, columnName, idType string
			nextID                              uint64
		)
		columns, err := rows.Columns()
		if err != nil {
			return nil, errors.Trace(err)
		}

		//+--------------+------------+-------------+--------------------+----------------+
		//| DB_NAME      | TABLE_NAME | COLUMN_NAME | NEXT_GLOBAL_ROW_ID | ID_TYPE        |
		//+--------------+------------+-------------+--------------------+----------------+
		//| testsysbench | t          | _tidb_rowid |                  1 | AUTO_INCREMENT |
		//+--------------+------------+-------------+--------------------+----------------+

		// if columns length is 4, it doesn't contain the last column `ID_TYPE`, and it will always be 'AUTO_INCREMENT'
		// for v4.0.0~v4.0.2 show table t next_row_id only returns 4 columns.
		if len(columns) == 4 {
			err = rows.Scan(&dbName, &tblName, &columnName, &nextID)
			idType = "AUTO_INCREMENT"
		} else {
			err = rows.Scan(&dbName, &tblName, &columnName, &nextID, &idType)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		autoIDInfos = append(autoIDInfos, &TableAutoIDInfo{
			Column: columnName,
			NextID: nextID,
			Type:   idType,
		})
	}
	// Defer in for-loop would be costly, anyway, we don't need those rows after this turn of iteration.
	//nolint:sqlclosecheck
	if err := rows.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return autoIDInfos, nil
}
