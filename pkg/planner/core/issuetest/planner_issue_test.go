// Copyright 2022 PingCAP, Inc.
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

package issuetest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(core.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}

func Test53726(t *testing.T) {
	// test for RemoveUnnecessaryFirstRow
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t7(c int); ")
	tk.MustExec("insert into t7 values (575932053), (-258025139);")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_8 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_9 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_7 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#13, Column#14, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12, cast(test.t7.c, decimal(10,0) BINARY)->Column#13, cast(test.t7.c, bigint(22) BINARY)->Column#14",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue58476(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("CREATE TABLE t3 (id int PRIMARY KEY,c1 varchar(256),c2 varchar(256) GENERATED ALWAYS AS (concat(c1, c1)) VIRTUAL,KEY (id));")
	tk.MustExec("insert into t3(id, c1) values (50, 'c');")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").Check(testkit.Rows())
	tk.MustQuery("explain format='brief' SELECT /*+ USE_INDEX_MERGE(`t3`)*/ id FROM `t3` WHERE c2 BETWEEN 'a' AND 'b' GROUP BY id HAVING id < 100 or id > 0;").
		Check(testkit.Rows(
			`Projection 249.75 root  test.t3.id`,
			`└─Selection 249.75 root  ge(test.t3.c2, "a"), le(test.t3.c2, "b")`,
			`  └─Projection 9990.00 root  test.t3.id, test.t3.c2`,
			`    └─IndexMerge 9990.00 root  type: union`,
			`      ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t3, index:id(id) range:[-inf,100), keep order:false, stats:pseudo`,
			`      ├─TableRangeScan(Build) 3333.33 cop[tikv] table:t3 range:(0,+inf], keep order:false, stats:pseudo`,
			`      └─TableRowIDScan(Probe) 9990.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
}

func TestIssue53175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set @@sql_mode = default`)
	tk.MustQuery(`select @@sql_mode REGEXP 'ONLY_FULL_GROUP_BY'`).Check(testkit.Rows("1"))
	tk.MustContainErrMsg(`select * from t group by null`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`create view v as select * from t group by null`)
	tk.MustContainErrMsg(`select * from v`, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustExec(`set @@sql_mode = ""`)
	tk.MustQuery(`select * from t group by null`)
	tk.MustQuery(`select * from v`)
}

func loadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := os.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &util.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(context.Background(), dom.InfoSchema(), statsTbl, 0)
	if err != nil {
		return err
	}
	return nil
}

func TestABC(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database if not exists dh_app_7226`)
	tk.MustExec("use dh_app_7226")
	tk.MustExec(`CREATE TABLE dh_login_device_record (
  id bigint(20) unsigned NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */ COMMENT 'id',
  site_code varchar(64) NOT NULL DEFAULT '' COMMENT '品牌id',
  create_time int(10) NOT NULL DEFAULT '0' COMMENT '创建时间',
  update_time int(10) NOT NULL DEFAULT '0' COMMENT '更新时间',
  useridx bigint(20) NOT NULL DEFAULT '0' COMMENT '用户ID',
  record_type tinyint(1) NOT NULL DEFAULT '1' COMMENT '当前设备:1当前 2上次',
  os_type int(10) NOT NULL DEFAULT '0' COMMENT '应用程序',
  app_version varchar(50) NOT NULL DEFAULT '' COMMENT '版本号',
  device_model varchar(64) NOT NULL DEFAULT '' COMMENT '设备类型',
  app_system varchar(50) NOT NULL DEFAULT '' COMMENT '系统',
  network_type varchar(50) NOT NULL DEFAULT '' COMMENT '网络类型',
  ip varchar(255) NOT NULL DEFAULT '' COMMENT 'ip地址',
  area varchar(255) NOT NULL DEFAULT '' COMMENT '地区',
  logintime bigint(20) NOT NULL DEFAULT '0' COMMENT '登陆时间',
  is_delete tinyint(1) NOT NULL DEFAULT '1' COMMENT '1 正常 2 删除',
  idle_time int(10) NOT NULL DEFAULT '0' COMMENT '闲置时长',
  device_id varchar(64) NOT NULL COMMENT '设备ID',
  user_token varchar(64) NOT NULL DEFAULT '' COMMENT 'token',
  os_kind varchar(16) NOT NULL DEFAULT '' COMMENT '设备分类',
  operating_system varchar(100) NOT NULL DEFAULT '' COMMENT '操作系统',
  device_brand varchar(100) NOT NULL DEFAULT '' COMMENT '设备品牌',
  login_count int(10) NOT NULL DEFAULT '0' COMMENT '登录次数',
  browser_type varchar(100) NOT NULL DEFAULT '' COMMENT '浏览器类型',
  browser_finger_id varchar(255) NOT NULL DEFAULT '' COMMENT '浏览器指纹',
  user_agent varchar(1000) NOT NULL DEFAULT '' COMMENT 'UA',
  device_hash varchar(64) NOT NULL DEFAULT '' COMMENT '设备hash, 用于卸载重装自动登录',
  trust_type int(8) NOT NULL DEFAULT '0' COMMENT '信任设备类型 1注册2充值3手动4取消',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY idx_udx_useridx_token_isdelete (useridx,user_token,is_delete),
  KEY idx_udx_useridx_token_sitecode (useridx,user_token,site_code),
  KEY idx_useridx_isDelete_deviceId_sitecode (useridx,is_delete,device_id,site_code),
  KEY idx_login_device_record_device_hash (site_code,device_hash,is_delete)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=5640001 */ COMMENT='会员登录设备列表'`)
	err := loadTableStats("dh_app_7226.dh_login_device_record.json", dom)
	require.NoError(t, err)
	tk.MustQuery(`explain SELECT /*+ FORCE_INDEX(dh_login_device_record, idx_useridx_isDelete_deviceId_sitecode )*/
  id,
  create_time,
  login_count,
  trust_type
FROM
  dh_login_device_record
WHERE
  (
    useridx = 339375754
    AND device_id = "672c5c1e-5ad0-4245-a038-2f451d2e4270"
    AND os_type = 6
  )
  AND site_code = "8268"
ORDER BY
  update_time DESC
LIMIT
  1`).Check(testkit.Rows())
}
