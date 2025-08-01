// Copyright 2023 PingCAP, Inc.
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

package executor_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func Test_fillOneImportJobInfo(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, len(plannercore.ImportIntoSchemaFTypes))
	for _, tp := range plannercore.ImportIntoSchemaFTypes {
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		fieldTypes = append(fieldTypes, fieldType)
	}
	c := chunk.New(fieldTypes, 10, 10)
	jobInfo := &importer.JobInfo{
		Parameters: importer.ImportParameters{},
	}

	fmap := plannercore.ImportIntoFieldMap
	rowCntIdx := fmap["ImportedRows"]
	startIdx := fmap["StartTime"]
	endIdx := fmap["EndTime"]

	executor.FillOneImportJobInfo(c, jobInfo, nil)
	require.True(t, c.GetRow(0).IsNull(rowCntIdx))
	require.True(t, c.GetRow(0).IsNull(startIdx))
	require.True(t, c.GetRow(0).IsNull(endIdx))

	executor.FillOneImportJobInfo(c, jobInfo, &importinto.RuntimeInfo{ImportRows: 0})
	require.False(t, c.GetRow(1).IsNull(rowCntIdx))
	require.Equal(t, uint64(0), c.GetRow(1).GetUint64(rowCntIdx))
	require.True(t, c.GetRow(1).IsNull(startIdx))
	require.True(t, c.GetRow(1).IsNull(endIdx))

	jobInfo.Status = importer.JobStatusFinished
	jobInfo.Summary = &importer.Summary{ImportedRows: 123}
	jobInfo.StartTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	jobInfo.EndTime = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	executor.FillOneImportJobInfo(c, jobInfo, nil)
	require.False(t, c.GetRow(2).IsNull(rowCntIdx))
	require.Equal(t, uint64(123), c.GetRow(2).GetUint64(rowCntIdx))
	require.False(t, c.GetRow(2).IsNull(startIdx))
	require.False(t, c.GetRow(2).IsNull(endIdx))

	ti := time.Now()
	ri := &importinto.RuntimeInfo{
		Processed:  10,
		Total:      100000,
		StartTime:  types.NewTime(types.FromGoTime(ti), mysql.TypeTimestamp, 0),
		UpdateTime: types.NewTime(types.FromGoTime(ti.Add(time.Second*5)), mysql.TypeTimestamp, 0),
	}
	jobInfo.Summary = &importer.Summary{ImportedRows: 0}
	executor.FillOneImportJobInfo(c, jobInfo, ri)
	require.Equal(t, "10B", c.GetRow(3).GetString(fmap["CurStepProcessedSize"]))
	require.Equal(t, "100kB", c.GetRow(3).GetString(fmap["CurStepTotalSize"]))
	require.Equal(t, "0", c.GetRow(3).GetString(fmap["CurStepProgressPct"]))
	require.Equal(t, "2B/s", c.GetRow(3).GetString(fmap["CurStepSpeed"]))
	require.Equal(t, "13:53:15", c.GetRow(3).GetString(fmap["CurStepETA"]))
}

func TestShow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int, abclmn int);")
	tk.MustExec("create table abclmn(a int);")

	tk.MustGetErrCode("show columns from t like id", errno.ErrBadField)
	tk.MustGetErrCode("show columns from t like `id`", errno.ErrBadField)

	tk.MustQuery("show tables").Check(testkit.Rows("abclmn", "t"))
	tk.MustQuery("show full tables").Check(testkit.Rows("abclmn BASE TABLE", "t BASE TABLE"))
	tk.MustQuery("show tables like 't'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'T'").Check(testkit.Rows("t"))
	tk.MustQuery("show tables like 'ABCLMN'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like 'ABC%'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show tables like '%lmn'").Check(testkit.Rows("abclmn"))
	tk.MustQuery("show full tables like '%lmn'").Check(testkit.Rows("abclmn BASE TABLE"))
	tk.MustGetErrCode("show tables like T", errno.ErrBadField)
	tk.MustGetErrCode("show tables like `T`", errno.ErrBadField)

	tk.MustExec("drop database test;")
	tk.MustExec("create database test;")
	tk.MustExec("create temporary table test.t1(id int);")
	tk.MustQuery("show tables from test like 't1';").Check(testkit.Rows( /* empty */ ))
	tk.MustExec("create global temporary table test.t2(id int) ON COMMIT DELETE ROWS;")
	tk.MustQuery("show tables from test like 't2';").Check(testkit.Rows("t2"))
}

func TestShowIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(id int, abclmn int);")

	tk.MustExec("create index idx on t(abclmn);")
	tk.MustQuery("show index from t").Check(testkit.Rows("t 1 idx 1 abclmn A 0 <nil> <nil> YES BTREE   YES <nil> NO NO"))
}

func TestShowIndexWithGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true;")

	defer tk.MustExec("set tidb_enable_global_index=false;")

	tk.MustExec("create table test_t1 (a int, b int) partition by range (b) (partition p0 values less than (10),  partition p1 values less than (maxvalue));")

	tk.MustExec("insert test_t1 values (1, 1);")
	tk.MustExec("alter table test_t1 add unique index p_a (a) GLOBAL;")
	tk.MustQuery("show index from test_t1").Check(testkit.Rows("test_t1 0 p_a 1 a A 0 <nil> <nil> YES BTREE   YES <nil> NO YES"))
}

func TestShowSessionStates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show session_states").CheckAt([]int{1}, testkit.Rows("<nil>"))

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk1.MustQuery("show session_states").CheckAt([]int{1}, testkit.Rows("<nil>"))
}
