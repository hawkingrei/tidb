// Copyright 2015 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRenameTable(
	t *testing.T,
	ctx sessionctx.Context,
	d ddl.ExecutorForTest,
	newSchemaID, oldSchemaID int64,
	oldSchemaName ast.CIStr,
	newSchemaName ast.CIStr,
	tblInfo *model.TableInfo,
) *model.Job {
	job := &model.Job{
		SchemaID: newSchemaID,
		TableID:  tblInfo.ID,
		Type:     model.ActionRenameTable,
		Version:  model.GetJobVerInUse(),

		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: oldSchemaName.L, Table: tblInfo.Name.L},
			{Database: newSchemaName.L, Table: tblInfo.Name.L},
		},
	}
	args := &model.RenameTableArgs{
		OldSchemaID:   oldSchemaID,
		OldSchemaName: oldSchemaName,
		NewTableName:  tblInfo.Name,
	}

	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true)))

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testRenameTables(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest,
	oldSchemaIDs, newSchemaIDs []int64,
	newTableNames []ast.CIStr, oldTableIDs []int64,
	oldSchemaNames, oldTableNames []ast.CIStr) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		Type:       model.ActionRenameTables,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: oldSchemaNames[0].L, Table: oldTableNames[0].L},
			{Database: oldSchemaNames[0].L, Table: newTableNames[0].L},
		},
	}

	args := &model.RenameTablesArgs{
		RenameTableInfos: model.GetRenameTablesArgsFromV1(
			oldSchemaIDs, oldSchemaNames, oldTableNames,
			newSchemaIDs, newTableNames, oldTableIDs,
		),
	}

	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true)))

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: nil})
	return job
}

func testLockTable(
	t *testing.T,
	ctx sessionctx.Context,
	d ddl.ExecutorForTest,
	uuid string,
	newSchemaID int64,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
	lockTp ast.TableLockType,
) *model.Job {
	args := &model.LockTablesArgs{
		LockTables: []model.TableLockTpInfo{{SchemaID: newSchemaID, TableID: tblInfo.ID, Tp: lockTp}},
		SessionInfo: model.SessionInfo{
			ServerID:  uuid,
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionLockTable,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: schemaName.L, Table: tblInfo.Name.L},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func checkTableLockedTest(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, serverID string, sessionID uint64, lockTp ast.TableLockType) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		require.NotNil(t, info)
		require.NotNil(t, info.Lock)
		require.Len(t, info.Lock.Sessions, 1)
		require.Equal(t, serverID, info.Lock.Sessions[0].ServerID)
		require.Equal(t, sessionID, info.Lock.Sessions[0].SessionID)
		require.Equal(t, lockTp, info.Lock.Tp)
		require.Equal(t, lockTp, info.Lock.Tp)
		require.Equal(t, model.TableLockStatePublic, info.Lock.State)
		return nil
	})
	require.NoError(t, err)
}

func testTruncateTable(t *testing.T, ctx sessionctx.Context, store kv.Storage, d ddl.ExecutorForTest, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	newTableID := genIDs[0]
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	args := &model.TruncateTableArgs{NewTableID: newTableID}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.ID = newTableID
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testGetTableWithError(r autoid.Requirement, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableInfo
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, r.Store(), false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		var err1 error
		tblInfo, err1 = t.GetTable(schemaID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		return nil, errors.New("table not found")
	}
	alloc := autoid.NewAllocator(r, schemaID, tblInfo.ID, false, autoid.RowIDAllocType)
	tbl, err := table.TableFromMeta(autoid.NewAllocators(false, alloc), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, nil
}

func TestTable(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	d := domain.DDL()
	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)

	ctx := testkit.NewTestKit(t, store).Session()

	tblInfo, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)
	job := testCreateTable(t, ctx, de, dbInfo, tblInfo)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// Create an existing table.
	newTblInfo, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)
	doDDLJobErr(t, dbInfo.ID, newTblInfo.ID, dbInfo.Name.L, newTblInfo.Name.L, model.ActionCreateTable,
		ctx, de, store, func(job *model.Job) model.JobArgs {
			return &model.CreateTableArgs{TableInfo: newTblInfo}
		})

	ctx = testkit.NewTestKit(t, store).Session()
	txn, err := newTxn(ctx)
	require.NoError(t, err)
	count := 2000
	tbl := testGetTable(t, domain, tblInfo.ID)
	for i := 1; i <= count; i++ {
		_, err := tbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	require.NoError(t, ctx.CommitTxn(context.Background()))

	jobID := testDropTable(testkit.NewTestKit(t, store), t, dbInfo.Name.L, tblInfo.Name.L, domain)
	testCheckJobDone(t, store, jobID, false)

	// for truncate table
	tblInfo, err = testTableInfo(store, "tt", 3)
	require.NoError(t, err)
	job = testCreateTable(t, ctx, de, dbInfo, tblInfo)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	job = testTruncateTable(t, ctx, store, de, dbInfo, tblInfo)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// for rename table
	dbInfo1, err := testSchemaInfo(store, "test_rename_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo1)
	job = testRenameTable(t, ctx, de, dbInfo1.ID, dbInfo.ID, dbInfo.Name, dbInfo1.Name, tblInfo)
	testCheckTableState(t, store, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	job = testLockTable(t, ctx, de, d.GetID(), dbInfo1.ID, dbInfo1.Name, tblInfo, ast.TableLockWrite)
	testCheckTableState(t, store, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	checkTableLockedTest(t, store, dbInfo1, tblInfo, d.GetID(), ctx.GetSessionVars().ConnectionID, ast.TableLockWrite)
	// for alter cache table
	job = testAlterCacheTable(t, ctx, de, dbInfo1.ID, dbInfo1.Name, tblInfo)
	testCheckTableState(t, store, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	checkTableCacheTest(t, store, dbInfo1, tblInfo)
	// for alter no cache table
	job = testAlterNoCacheTable(t, ctx, de, dbInfo1.ID, dbInfo1.Name, tblInfo)
	testCheckTableState(t, store, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	checkTableNoCacheTest(t, store, dbInfo1, tblInfo)

	testDropSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)
}

func TestCreateView(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)

	ctx := testkit.NewTestKit(t, store).Session()

	tblInfo, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)
	job := testCreateTable(t, ctx, de, dbInfo, tblInfo)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// Create a view
	newTblInfo0, err := testTableInfo(store, "v", 3)
	require.NoError(t, err)
	job = &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
	}
	args := &model.CreateTableArgs{TableInfo: newTblInfo0}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: newTblInfo0})
	tblInfo.State = model.StateNone
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// Replace a view
	newTblInfo1, err := testTableInfo(store, "v", 3)
	require.NoError(t, err)
	job = &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
	}
	args = &model.CreateTableArgs{TableInfo: newTblInfo1, OnExistReplace: true, OldViewTblID: newTblInfo0.ID}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	v = getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: newTblInfo1})
	tblInfo.State = model.StateNone
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// Replace a view with a non-existing table id
	newTblInfo2, err := testTableInfo(store, "v", 3)
	require.NoError(t, err)
	job = &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
	}
	args = &model.CreateTableArgs{TableInfo: newTblInfo2, OnExistReplace: true, OldViewTblID: newTblInfo0.ID}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	// The non-existing table id in job args will not be considered anymore.
	require.NoError(t, err)
}

func checkTableCacheTest(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.NotNil(t, info.TableCacheStatusType)
		require.Equal(t, model.TableCacheStatusEnable, info.TableCacheStatusType)
		return nil
	}))
}

func checkTableNoCacheTest(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, model.TableCacheStatusDisable, info.TableCacheStatusType)
		return nil
	}))
}

func testAlterCacheTable(
	t *testing.T,
	ctx sessionctx.Context,
	d ddl.ExecutorForTest,
	newSchemaID int64,
	newSchemaName ast.CIStr,
	tblInfo *model.TableInfo,
) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterCacheTable,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: newSchemaName.L, Table: tblInfo.Name.L},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.EmptyArgs{}, true))
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func testAlterNoCacheTable(
	t *testing.T,
	ctx sessionctx.Context,
	d ddl.ExecutorForTest,
	newSchemaID int64,
	newSchemaName ast.CIStr,
	tblInfo *model.TableInfo,
) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterNoCacheTable,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: newSchemaName.L, Table: tblInfo.Name.L},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.EmptyArgs{}, true)))

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func TestRenameTables(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)

	ctx := testkit.NewTestKit(t, store).Session()
	var tblInfos = make([]*model.TableInfo, 0, 2)
	var newTblInfos = make([]*model.TableInfo, 0, 2)
	for i := 1; i < 3; i++ {
		tableName := fmt.Sprintf("t%d", i)
		tblInfo, err := testTableInfo(store, tableName, 3)
		require.NoError(t, err)
		job := testCreateTable(t, ctx, de, dbInfo, tblInfo)
		testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
		testCheckJobDone(t, store, job.ID, true)
		tblInfos = append(tblInfos, tblInfo)

		newTableName := fmt.Sprintf("tt%d", i)
		tblInfo, err = testTableInfo(store, newTableName, 3)
		require.NoError(t, err)
		newTblInfos = append(newTblInfos, tblInfo)
	}
	job := testRenameTables(t, ctx, de,
		[]int64{dbInfo.ID, dbInfo.ID},
		[]int64{dbInfo.ID, dbInfo.ID},
		[]ast.CIStr{newTblInfos[0].Name, newTblInfos[1].Name},
		[]int64{tblInfos[0].ID, tblInfos[1].ID},
		[]ast.CIStr{dbInfo.Name, dbInfo.Name},
		[]ast.CIStr{tblInfos[0].Name, tblInfos[1].Name})

	historyJob, err := ddl.GetHistoryJobByID(testkit.NewTestKit(t, store).Session(), job.ID)
	require.NoError(t, err)
	wantTblInfos := historyJob.BinlogInfo.MultipleTableInfos
	require.Equal(t, wantTblInfos[0].Name.L, "tt1")
	require.Equal(t, wantTblInfos[1].Name.L, "tt2")
}

func TestCreateTables(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)

	ctx := testkit.NewTestKit(t, store).Session()

	genIDs, err := genGlobalIDs(store, 3)
	require.NoError(t, err)

	args := &model.BatchCreateTableArgs{
		Tables: make([]*model.CreateTableArgs, 0, 3),
	}
	for i := range 3 {
		args.Tables = append(args.Tables, &model.CreateTableArgs{
			TableInfo: &model.TableInfo{
				ID:   genIDs[i],
				Name: ast.NewCIStr(fmt.Sprintf("s%d", i+1)),
			},
		})
	}

	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateTables,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: "test_table", Table: "s1"},
			{Database: "test_table", Table: "s2"},
			{Database: "test_table", Table: "s3"},
		},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")

	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockGetJobByIDFail", func(errP *error) {
		once.Do(func() {
			*errP = errors.New("mock get job by ID failed")
		})
	})
	err = de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	testGetTable(t, domain, genIDs[0])
	testGetTable(t, domain, genIDs[1])
	testGetTable(t, domain, genIDs[2])
}

func TestAlterTTL(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	de := domain.DDLExecutor().(ddl.ExecutorForTest)

	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), de, dbInfo)

	ctx := testkit.NewTestKit(t, store).Session()

	// initialize a table with ttlInfo
	tableName := "t"
	tblInfo, err := testTableInfo(store, tableName, 2)
	require.NoError(t, err)
	tblInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeDatetime)
	tblInfo.Columns[1].FieldType = *types.NewFieldType(mysql.TypeDatetime)
	tblInfo.TTLInfo = &model.TTLInfo{
		ColumnName:       tblInfo.Columns[0].Name,
		IntervalExprStr:  "5",
		IntervalTimeUnit: int(ast.TimeUnitDay),
	}

	// create table
	job := testCreateTable(t, ctx, de, dbInfo, tblInfo)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	// submit ddl job to modify ttlInfo
	tableInfoAfterAlterTTLInfo := tblInfo.Clone()
	require.NoError(t, err)
	tableInfoAfterAlterTTLInfo.TTLInfo = &model.TTLInfo{
		ColumnName:       tblInfo.Columns[1].Name,
		IntervalExprStr:  "1",
		IntervalTimeUnit: int(ast.TimeUnitYear),
	}

	job = &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionAlterTTLInfo,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	args := &model.AlterTTLInfoArgs{
		TTLInfo: &model.TTLInfo{
			ColumnName:       tblInfo.Columns[1].Name,
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitYear),
		},
	}
	require.NoError(t, de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true)))

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: nil})

	// assert the ddlInfo as expected
	historyJob, err := ddl.GetHistoryJobByID(testkit.NewTestKit(t, store).Session(), job.ID)
	require.NoError(t, err)
	require.Equal(t, tableInfoAfterAlterTTLInfo.TTLInfo, historyJob.BinlogInfo.TableInfo.TTLInfo)

	// submit a ddl job to modify ttlEnabled
	job = &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionAlterTTLRemove,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.EmptyArgs{}, true)))

	v = getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: nil})

	// assert the ddlInfo as expected
	historyJob, err = ddl.GetHistoryJobByID(testkit.NewTestKit(t, store).Session(), job.ID)
	require.NoError(t, err)
	require.Empty(t, historyJob.BinlogInfo.TableInfo.TTLInfo)
}

func TestRenameTableIntermediateState(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("create database db1;")
	tk.MustExec("create database db2;")
	tk.MustExec("create table db1.t(a int);")

	testCases := []struct {
		renameSQL string
		insertSQL string
		errMsg    string
		finalDB   string
	}{
		{"rename table db1.t to db1.t1;", "insert into db1.t values(1);", "[schema:1146]Table 'db1.t' doesn't exist", "db1.t1"},
		{"rename table db1.t1 to db1.t;", "insert into db1.t values(1);", "", "db1.t"},
		{"rename table db1.t to db2.t;", "insert into db1.t values(1);", "[schema:1146]Table 'db1.t' doesn't exist", "db2.t"},
		{"rename table db2.t to db1.t;", "insert into db1.t values(1);", "", "db1.t"},
	}

	var finishedJobID int64
	for _, tc := range testCases {
		runInsert := false
		var jobID int64 = 0
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
			if job.ID <= finishedJobID {
				// The job has been done, OnJobUpdated may be invoked later asynchronously.
				// We should skip the done job.
				return
			}
			if job.Type == model.ActionRenameTable &&
				job.SchemaState == model.StatePublic && !runInsert && !t.Failed() {
				_, err := tk2.Exec(tc.insertSQL)
				// In rename table intermediate state, new table is public.
				if len(tc.errMsg) > 0 {
					// Old table should not be visible to DML.
					assert.NotNil(t, err)
					assert.Equal(t, tc.errMsg, err.Error())
				} else {
					// New table should be visible to DML.
					assert.NoError(t, err)
				}
				runInsert = true
				jobID = job.ID
			}
		})
		tk.MustExec(tc.renameSQL)
		result := tk.MustQuery(fmt.Sprintf("select * from %s;", tc.finalDB))
		if len(tc.errMsg) > 0 {
			result.Check(testkit.Rows())
		} else {
			result.Check(testkit.Rows("1"))
		}
		tk.MustExec(fmt.Sprintf("delete from %s;", tc.finalDB))
		finishedJobID = jobID
	}
}

func TestCreateSameTableOrDBOnOwnerChange(t *testing.T) {
	tc := testkit.NewDistExecutionContext(t, 2)
	defer tc.Close()

	// keep trigger owner change every 50ms.
	var ownerWg util.WaitGroupWrapper
	var finished atomic.Bool
	ownerWg.Run(func() {
		for !finished.Load() {
			tc.TriggerOwnerChange()
			time.Sleep(50 * time.Millisecond)
		}
	})

	var pauseSchedule atomic.Bool
	var waitSchCh = make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeLoadAndDeliverJobs", func() {
		if pauseSchedule.Load() {
			<-waitSchCh
		}
	})
	pauseSchedule.Store(true)

	var enableWaitSubmit atomic.Bool
	waitSubmitCh := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/waitJobSubmitted",
		func() {
			if enableWaitSubmit.Load() {
				<-waitSubmitCh
			}
		},
	)
	enableWaitSubmit.Store(true)

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStepAndWaitSync", func() { time.Sleep(300 * time.Millisecond) })
	// create and wait all jobs are submitted to tidb_ddl_job before they are run.
	// we are creating same table/database, only the first will success.
	var wg util.WaitGroupWrapper
	for gi, g := range [][]string{
		{
			"create table test.t(a int)",
			"create table test.t(a int)",
			"create table test.t(a int)",
		},
		{
			"create database aaa",
			"create database aaa",
			"create database aaa",
		},
	} {
		expectedErr := infoschema.ErrTableExists
		if gi == 1 {
			expectedErr = infoschema.ErrDatabaseExists
		}
		for i, s := range g {
			idx, sql := i, s
			wg.Run(func() {
				tk2 := testkit.NewTestKit(t, tc.Store)
				if idx == 0 {
					tk2.MustExec(sql)
				} else {
					err := tk2.ExecToErr(sql)
					require.ErrorIs(t, err, expectedErr)
				}
			})
			waitSubmitCh <- struct{}{}
		}
	}
	enableWaitSubmit.Store(false)

	// start schedule jobs when all jobs are submitted to tidb_ddl_job
	pauseSchedule.Store(false)
	close(waitSchCh)

	wg.Wait()
	finished.Store(true)
	ownerWg.Wait()
}

func TestDropTableAccessibleInInfoSchema(t *testing.T) {
	// The dropped table should always be accessible until the state reaches `StateNone`.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tkDDL := testkit.NewTestKit(t, store)
	tkDDL.MustExec("use test")
	tkDDL.MustExec("create table t (id int key)")

	var errs []error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionDropTable && job.TableName == "t" {
			if job.SchemaState == model.StateDeleteOnly || job.SchemaState == model.StateWriteOnly {
				_, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
				errs = append(errs, err)
			}
		}
	})
	tkDDL.MustExec("drop table t")

	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.True(t, len(errs) > 0)
}

func TestCreateViewTwice(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_raw (id int)")
	tk2.MustExec("use test")

	var wg sync.WaitGroup
	wg.Add(1)
	first := true
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeDeliveryJob", func(job *model.Job) {
		if first {
			first = false
			go func() {
				defer wg.Done()
				tk2.MustExecToErr("create view v as select * from t_raw where id > 666")
			}()
		}
	})
	tk.MustExec("create view v as select * from t_raw")
	wg.Wait()
}

func TestIssue59238(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t ( a INT, b INT, INDEX idx(b))" +
		" PARTITION BY RANGE(a) (" +
		" PARTITION p1 VALUES LESS THAN (10000)," +
		" PARTITION p2 VALUES LESS THAN (20000)," +
		" PARTITION p3 VALUES LESS THAN (MAXVALUE))")

	rs := tk.MustQuery("select distinct create_time from information_schema.partitions where table_name = 't'").String()

	tk.MustExec("alter table t truncate partition p1")
	require.True(t, tk.MustQuery("select distinct create_time from information_schema.partitions where table_name = 't'").Equal(testkit.Rows(rs)))

	tk.MustExec("create table t1 (a int, b int, index idx(b))")
	tk.MustExec("alter table t exchange partition p1 with table t1")
	require.True(t, tk.MustQuery("select distinct create_time from information_schema.partitions where table_name = 't'").Equal(testkit.Rows(rs)))
}

// TestRefreshMetaBasic tests few scenarios of meta kv inconsistent with infoschema.
func TestRefreshMetaBasic(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)
	sctx := testkit.NewTestKit(t, store).Session()

	// get t1 table info
	tk.MustExec("create placement policy p1 followers=1")
	tk.MustExec("create placement policy p2 followers=2")
	tk.MustExec("create database test1 placement policy p1")
	tk.MustExec("use test1")
	tk.MustExec("create table t1(id int)")
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test1"))
	require.True(t, ok)
	clonedTableInfo := getClonedTableInfoFromDomain(t, "test1", "t1", domain)
	// update t1 table name to t2 by txn
	clonedTableInfo.Name = ast.NewCIStr("t2")
	updateTableMeta(t, store, dbInfo.ID, clonedTableInfo)
	t2TableInfo := testutil.GetTableInfoByTxn(t, store, dbInfo.ID, clonedTableInfo.ID)
	require.Equal(t, clonedTableInfo, t2TableInfo)
	// validate infoschema doesn't conatain t2 table info
	_, err := domain.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.ErrorContains(t, err, "Table 'test1.t2' doesn't exist")
	// refresh meta, validate infoschema store table t2 and schema version increase 1
	oldSchemaVer := getSchemaVer(t, sctx)
	testutil.RefreshMeta(sctx, t, de, dbInfo.ID, clonedTableInfo.ID, dbInfo.Name.O, clonedTableInfo.Name.O)
	newSchemaVer := getSchemaVer(t, sctx)
	require.Equal(t, oldSchemaVer+1, newSchemaVer)
	_, err = domain.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test1"), ast.NewCIStr("t2"))
	require.NoError(t, err)

	// table not exists in kv, exists in infoschema
	tk.MustExec("create table t3(id int) placement policy p2")
	txn, err := store.Begin()
	require.NoError(t, err)
	clonedTableInfo = getClonedTableInfoFromDomain(t, "test1", "t3", domain)
	// drop table t3 by txn
	err = meta.NewMutator(txn).DropTableOrView(dbInfo.ID, clonedTableInfo.ID)
	require.NoError(t, err)
	txn.Commit(context.Background())
	txn, err = store.Begin()
	require.NoError(t, err)
	kvTableInfo, err := meta.NewMutator(txn).GetTable(dbInfo.ID, clonedTableInfo.ID)
	require.NoError(t, err)
	require.Nil(t, kvTableInfo)
	// t3 table info exists in infoschema
	_, ok = domain.InfoSchema().TableByID(context.Background(), clonedTableInfo.ID)
	require.True(t, ok)
	// after refresh meta, t3 table info should be not exists in infoschema
	testutil.RefreshMeta(sctx, t, de, dbInfo.ID, clonedTableInfo.ID, dbInfo.Name.O, clonedTableInfo.Name.O)
	_, ok = domain.InfoSchema().TableByID(context.Background(), clonedTableInfo.ID)
	require.False(t, ok)
	_, ok = domain.InfoSchema().PlacementBundleByPhysicalTableID(clonedTableInfo.ID)
	require.False(t, ok)

	// table exists in kv, not exists in infoschema
	clonedTableInfo.Name = ast.NewCIStr("t4")
	clonedTableInfo.ID = 40000
	txn, err = store.Begin()
	require.NoError(t, err)
	// create table t4 by txn
	err = meta.NewMutator(txn).CreateTableOrView(dbInfo.ID, clonedTableInfo)
	require.NoError(t, err)
	txn.Commit(context.Background())
	txn, err = store.Begin()
	require.NoError(t, err)
	kvTableInfo, err = meta.NewMutator(txn).GetTable(dbInfo.ID, clonedTableInfo.ID)
	require.NoError(t, err)
	require.Equal(t, clonedTableInfo, kvTableInfo)
	// t4 table info not exists in infoschema
	_, ok = domain.InfoSchema().TableByID(context.Background(), clonedTableInfo.ID)
	require.False(t, ok)
	// refresh meta, t4 table info should be equal with kv table info
	testutil.RefreshMeta(sctx, t, de, dbInfo.ID, clonedTableInfo.ID, dbInfo.Name.O, clonedTableInfo.Name.O)
	infoschemaTableInfo, ok := domain.InfoSchema().TableByID(context.Background(), clonedTableInfo.ID)
	require.True(t, ok)
	require.Equal(t, kvTableInfo.ID, infoschemaTableInfo.Meta().ID)
	require.Equal(t, kvTableInfo.Name, infoschemaTableInfo.Meta().Name)
	require.Equal(t, kvTableInfo.PlacementPolicyRef, infoschemaTableInfo.Meta().PlacementPolicyRef)
	_, ok = domain.InfoSchema().PlacementBundleByPhysicalTableID(clonedTableInfo.ID)
	require.True(t, ok)

	// schema not exists in kv, exists in infoschema
	clonedDBInfo, ok := getClonedDatabase(domain, "test1")
	require.True(t, ok)
	txn, err = store.Begin()
	require.NoError(t, err)
	// create table t4 by txn
	err = meta.NewMutator(txn).DropDatabase(clonedDBInfo.ID)
	require.NoError(t, err)
	txn.Commit(context.Background())
	txn, err = store.Begin()
	require.NoError(t, err)
	kvDBInfo, err := meta.NewMutator(txn).GetDatabase(clonedDBInfo.ID)
	require.NoError(t, err)
	require.Nil(t, kvDBInfo)
	// test db info exists in infoschema
	_, ok = domain.InfoSchema().SchemaByID(clonedDBInfo.ID)
	require.True(t, ok)
	// refresh meta, t4 table info should be equal with kv table info
	testutil.RefreshMeta(sctx, t, de, clonedDBInfo.ID, 0, clonedDBInfo.Name.O, model.InvolvingAll)
	_, ok = domain.InfoSchema().SchemaByID(clonedDBInfo.ID)
	require.False(t, ok)

	// schema exists in kv, not exists in infoschema
	clonedDBInfo.Name = ast.NewCIStr("test2")
	clonedDBInfo.ID = 20000
	txn, err = store.Begin()
	require.NoError(t, err)
	// create database test2 by txn
	err = meta.NewMutator(txn).CreateDatabase(clonedDBInfo)
	require.NoError(t, err)
	txn.Commit(context.Background())
	txn, err = store.Begin()
	require.NoError(t, err)
	kvDBInfo, err = meta.NewMutator(txn).GetDatabase(clonedDBInfo.ID)
	require.NoError(t, err)
	// test2 db info not exists in infoschema
	_, ok = domain.InfoSchema().SchemaByID(clonedDBInfo.ID)
	require.False(t, ok)
	// refresh meta, test2 db info should exists in infoschema
	testutil.RefreshMeta(sctx, t, de, clonedDBInfo.ID, 0, clonedDBInfo.Name.O, model.InvolvingAll)
	infoschemaDBInfo, ok := domain.InfoSchema().SchemaByID(clonedDBInfo.ID)
	require.True(t, ok)
	require.Equal(t, kvDBInfo, infoschemaDBInfo)
}
