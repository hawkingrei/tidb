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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
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

	nodeW := resolve.NewNodeW(stmt)
	p, _, err := planner.Optimize(context.TODO(), tk.Session(), nodeW, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
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

	tk.MustExec("analyze table t7 all columns")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#13, Column#14, funcs:firstrow(Column#13)->Column#3, funcs:firstrow(Column#14)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#13, cast(test.t7.c, bigint(22) BINARY)->Column#14",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}

func TestIssue54535(t *testing.T) {
	// test for tidb_enable_inl_join_inner_multi_pattern system variable
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set session tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustExec("create table ta(a1 int, a2 int, a3 int, index idx_a(a1))")
	tk.MustExec("create table tb(b1 int, b2 int, b3 int, index idx_b(b1))")
	tk.MustExec("analyze table ta")
	tk.MustExec("analyze table tb")

	tk.MustQuery("explain SELECT /*+ inl_join(tmp) */ * FROM ta, (SELECT b1, COUNT(b3) AS cnt FROM tb GROUP BY b1, b2) as tmp where ta.a1 = tmp.b1").
		Check(testkit.Rows(
			"Projection_9 9990.00 root  test.ta.a1, test.ta.a2, test.ta.a3, test.tb.b1, Column#9",
			"└─IndexJoin_16 9990.00 root  inner join, inner:HashAgg_14, outer key:test.ta.a1, inner key:test.tb.b1, equal cond:eq(test.ta.a1, test.tb.b1)",
			"  ├─TableReader_43(Build) 9990.00 root  data:Selection_42",
			"  │ └─Selection_42 9990.00 cop[tikv]  not(isnull(test.ta.a1))",
			"  │   └─TableFullScan_41 10000.00 cop[tikv] table:ta keep order:false, stats:pseudo",
			"  └─HashAgg_14(Probe) 9990.00 root  group by:test.tb.b1, test.tb.b2, funcs:count(Column#11)->Column#9, funcs:firstrow(test.tb.b1)->test.tb.b1",
			"    └─IndexLookUp_15 9990.00 root  ",
			"      ├─Selection_12(Build) 9990.00 cop[tikv]  not(isnull(test.tb.b1))",
			"      │ └─IndexRangeScan_10 10000.00 cop[tikv] table:tb, index:idx_b(b1) range: decided by [eq(test.tb.b1, test.ta.a1)], keep order:false, stats:pseudo",
			"      └─HashAgg_13(Probe) 9990.00 cop[tikv]  group by:test.tb.b1, test.tb.b2, funcs:count(test.tb.b3)->Column#11",
			"        └─TableRowIDScan_11 9990.00 cop[tikv] table:tb keep order:false, stats:pseudo"))
	// test for issues/55169
	tk.MustExec("create table t1(col_1 int, index idx_1(col_1));")
	tk.MustExec("create table t2(col_1 int, col_2 int, index idx_2(col_1));")
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
	tk.MustQuery("select /*+ inl_join(tmp) */ * from t1 inner join (select col_1, group_concat(distinct col_2 order by col_2) from t2 group by col_1) tmp on t1.col_1 = tmp.col_1;").Check(testkit.Rows())
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
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustQuery(`select * from t group by null`)
	tk.MustQuery(`select * from v`)
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

func TestIssue59643(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustQuery(`explain format='brief' SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;
`).Check(testkit.Rows(
		"HashJoin 2.00 root  inner join, equal:[nulleq(Column#5, Column#11)]",
		"├─HashAgg(Build) 2.00 root  group by:Column#5, Column#6, funcs:firstrow(Column#5)->Column#5, funcs:firstrow(Column#6)->Column#6",
		"│ └─Union 2.00 root  ",
		"│   ├─HashAgg 1.00 root  group by:1, funcs:firstrow(1)->Column#1, funcs:firstrow(\"Alice\")->Column#2",
		"│   │ └─TableDual 1.00 root  rows:1",
		"│   └─Projection 1.00 root  <nil>->Column#5, Bob->Column#6",
		"│     └─TableDual 1.00 root  rows:1",
		"└─HashAgg(Probe) 2.00 root  group by:Column#11, Column#12, funcs:firstrow(Column#11)->Column#11, funcs:firstrow(Column#12)->Column#12",
		"  └─Union 2.00 root  ",
		"    ├─Projection 1.00 root  1->Column#11, 100->Column#12",
		"    │ └─TableDual 1.00 root  rows:1",
		"    └─Projection 1.00 root  <nil>->Column#11, <nil>->Column#12",
		"      └─TableDual 1.00 root  rows:1"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT distinct 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
	tk.MustQuery(`SELECT
    base.c1,
    base.c2,
    base2.c1 AS base2_c1,
    base2.c3
FROM
    (SELECT 1 AS c1, 'Alice' AS c2 UNION SELECT NULL AS c1, 'Bob' AS c2) AS base
INNER JOIN
    (SELECT 1 AS c1, 100 AS c3 UNION SELECT NULL AS c1, NULL AS c3) AS base2
ON base.c1 <=> base2.c1;`).Sort().Check(testkit.Rows(
		"1 Alice 1 100",
		"<nil> Bob <nil> <nil>"))
}

func TestIssue58451(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1 (a1 int, b1 int);")
	tk.MustExec("create table t2 (a2 int, b2 int);")
	tk.MustExec("insert into t1 values(1,1);")
	tk.MustQuery(`explain format='brief'
SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("HashJoin 2.00 root  CARTESIAN left outer semi join, left side:HashAgg",
		"├─HashAgg(Build) 2.00 root  group by:Column#18, Column#19, funcs:firstrow(1)->Column#45",
		"│ └─Union 0.00 root  ",
		"│   ├─Projection 0.00 root  8->Column#18, 0->Column#19",
		"│   │ └─TableDual 0.00 root  rows:0",
		"│   └─Projection 0.00 root  8->Column#18, 8->Column#19",
		"│     └─TableDual 0.00 root  rows:0",
		"└─HashAgg(Probe) 2.00 root  group by:Column#10, funcs:firstrow(1)->Column#42",
		"  └─HashJoin 10000.00 root  CARTESIAN left outer semi join, left side:TableReader",
		"    ├─HashAgg(Build) 2.00 root  group by:Column#8, Column#9, funcs:firstrow(1)->Column#44",
		"    │ └─Union 0.00 root  ",
		"    │   ├─Projection 0.00 root  8->Column#8, 0->Column#9",
		"    │   │ └─TableDual 0.00 root  rows:0",
		"    │   └─Projection 0.00 root  8->Column#8, 8->Column#9",
		"    │     └─TableDual 0.00 root  rows:0",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:table1 keep order:false, stats:pseudo"))
	tk.MustQuery(`SELECT (4,5) IN (SELECT 8,0 UNION SELECT 8, 8) AS field1
FROM t1 AS table1
WHERE (EXISTS (SELECT SUBQUERY2_t1.a1 AS SUBQUERY2_field1 FROM t1 AS SUBQUERY2_t1)) OR table1.b1 >= 55
GROUP BY field1;`).Check(testkit.Rows("0"))
}

func TestIssue59902(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(a int primary key, b int);")
	tk.MustExec("create table t2(a int, b int, key idx(a));")
	tk.MustExec("set tidb_enable_inl_join_inner_multi_pattern=on;")
	tk.MustQuery("explain format='brief' select t1.b,(select count(*) from t2 where t2.a=t1.a) as a from t1 where t1.a=1;").
		Check(testkit.Rows(
			"Projection 1.00 root  test.t1.b, ifnull(Column#9, 0)->Column#9",
			"└─IndexJoin 1.00 root  left outer join, inner:HashAgg, left side:Point_Get, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a)",
			"  ├─Point_Get(Build) 1.00 root table:t1 handle:1",
			"  └─HashAgg(Probe) 1.00 root  group by:test.t2.a, funcs:count(Column#10)->Column#9, funcs:firstrow(test.t2.a)->test.t2.a",
			"    └─IndexReader 1.00 root  index:HashAgg",
			"      └─HashAgg 1.00 cop[tikv]  group by:test.t2.a, funcs:count(1)->Column#10",
			"        └─Selection 1.00 cop[tikv]  not(isnull(test.t2.a))",
			"          └─IndexRangeScan 1.00 cop[tikv] table:t2, index:idx(a) range: decided by [eq(test.t2.a, test.t1.a)], keep order:false, stats:pseudo"))
}

func TestABC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`CREATE TABLE mht_business_scenario_code_trade_acct (
  id int(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '逻辑主键',
  asset_acct_id varchar(32) NOT NULL COMMENT '资产账号',
  trade_acct varchar(32) NOT NULL COMMENT '交易账号',
  ta_no varchar(2) NOT NULL COMMENT 'TANO',
  business_scenario_type varchar(2) NOT NULL COMMENT '业务场景类型：01-默认场景 02-税优',
  business_scenario_code varchar(10) NOT NULL COMMENT '业务场景编码，0101：默认场景类型下第一个子业务代码 0201：税优业务场景类型下第一个子业务编码',
  create_time datetime DEFAULT NULL COMMENT '创建时间',
  modify_time datetime DEFAULT NULL COMMENT '修改时间',
  create_by varchar(20) DEFAULT NULL COMMENT '创建人',
  modify_by varchar(20) DEFAULT NULL COMMENT '修改人',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY uniq_trade_acct_business_scenario (trade_acct,ta_no,business_scenario_code),
  KEY idx_trade_acct_business_scenario (trade_acct,business_scenario_type,business_scenario_code),
  KEY idx_asset_acct_business_scenario (asset_acct_id,ta_no,business_scenario_code),
  KEY idx_tano_businessscenariotype (ta_no,business_scenario_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=2537239 COMMENT='交易账号和场景关联关系表'`)
	tk.MustExec(`CREATE TABLE mht_product_ta_info (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  product_id varchar(16) NOT NULL COMMENT '产品代码',
  fund_code varchar(20) NOT NULL COMMENT '基金代码',
  ta_no varchar(2) NOT NULL COMMENT 'TA代码',
  create_time datetime NOT NULL COMMENT '新增时间',
  asset_type varchar(4) DEFAULT NULL COMMENT '资产大类',
  fof_strategy_type varchar(6) DEFAULT NULL COMMENT '组合策略类型',
  fof_sub_strategy_type varchar(6) DEFAULT NULL COMMENT 'fof子类型',
  product_type varchar(2) DEFAULT NULL COMMENT '产品类型',
  fund_type varchar(2) DEFAULT NULL COMMENT '基金类型',
  fund_name varchar(100) DEFAULT NULL COMMENT '基金名称',
  merchant_id varchar(24) DEFAULT NULL COMMENT '商户号',
  manager_name varchar(50) DEFAULT NULL COMMENT '基金管理人',
  red_days int(3) DEFAULT '0' COMMENT '赎回划款日',
  pur_acct_no varchar(50) DEFAULT NULL COMMENT '申购账号',
  sub_acct_no varchar(45) DEFAULT NULL COMMENT '认购账号',
  branch_code varchar(10) NOT NULL DEFAULT '0001' COMMENT '销售机构代码',
  sale_channel_no varchar(6) DEFAULT NULL COMMENT '份额所属渠道号',
  trade_acct_type varchar(2) NOT NULL DEFAULT '0' COMMENT '代表这个TA下这个产品交易是资金或者资产',
  detail_fund_type varchar(4) DEFAULT NULL COMMENT '基金类型明细，互认基金',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY uniq_key (product_id,branch_code,ta_no),
  KEY idx_fundcode_tano (fund_code,ta_no),
  KEY idx_fund_type (fund_type),
  KEY idx_1 (product_id,ta_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=191233 COMMENT='产品对应的TA信息表'`)
	tk.MustExec(`CREATE TABLE mht_ta_user_account (
  asset_acct_id varchar(32) NOT NULL DEFAULT '' COMMENT '用户交易账号',
  ta_no varchar(2) NOT NULL COMMENT 'TA代码',
  trade_acct varchar(32) NOT NULL COMMENT '用户交易账户(分配后传给TA)',
  fund_acct varchar(64) DEFAULT NULL COMMENT '用户TA账号(TA分配)',
  status varchar(1) DEFAULT NULL,
  gmt_create datetime DEFAULT NULL COMMENT '创建时间',
  gmt_modified datetime DEFAULT NULL COMMENT '修改时间',
  branch_code char(4) DEFAULT '0001' COMMENT '销售渠道',
  trade_acct_type varchar(2) NOT NULL DEFAULT '0' COMMENT '交易帐号类别',
  third_trade_acct varchar(32) DEFAULT '' COMMENT '第三方交易账户',
  trans_way_type varchar(10) DEFAULT NULL COMMENT '交易方式类型，区分和ta交互的方式',
  PRIMARY KEY (asset_acct_id,ta_no,trade_acct,trade_acct_type) /*T![clustered_index] CLUSTERED */,
  KEY IDX_TRD_TRADE_ACCT (third_trade_acct),
  KEY idx_mht_create_time (gmt_create),
  KEY idx_tradeacct_tano (trade_acct,ta_no),
  KEY idx_fund_acct (fund_acct)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='用户TA账号表'`)
	tk.MustExec(`CREATE TABLE mht_trade_audit_info (
  app_no varchar(32) NOT NULL COMMENT '审批申请单号',
  info_type varchar(1) NOT NULL DEFAULT '' COMMENT '审批内容类型C-出款审批',
  audit_status varchar(1) DEFAULT NULL COMMENT '审批状态',
  operator varchar(20) DEFAULT NULL COMMENT '经办人',
  operate_time datetime DEFAULT NULL COMMENT '经办时间',
  checked_by varchar(20) DEFAULT NULL COMMENT '复核人',
  check_time datetime DEFAULT NULL COMMENT '复核时间',
  create_time datetime DEFAULT NULL COMMENT '创建时间',
  modify_time datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (app_no,info_type) /*T![clustered_index] CLUSTERED */,
  KEY idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易审核信息表'`)
	tk.MustExec(`CREATE TABLE mht_trade_cash_out_info (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  cash_out_id varchar(32) NOT NULL COMMENT '出款单号',
  request_id varchar(32) NOT NULL COMMENT '请求单号',
  step_no int(2) NOT NULL COMMENT '请求状态步骤序号',
  card_idx_no varchar(64) NOT NULL COMMENT '索引卡号',
  cash_type varchar(1) NOT NULL COMMENT '出款至V货币基金B银行',
  cash_out_src varchar(5) NOT NULL COMMENT '出款来由(赎回款,退款,分红款等)',
  cash_amt bigint(16) DEFAULT NULL COMMENT '划款金额',
  trans_id varchar(32) DEFAULT NULL COMMENT '交易单号',
  cash_out_date varchar(8) DEFAULT NULL COMMENT '出款日期',
  real_cash_out_date varchar(8) DEFAULT NULL COMMENT '在发送隔夜垫资的情况下，出款记录本来实际应该出款的日期',
  advance_natural_days smallint(2) DEFAULT '0' COMMENT '垫资自然日天数',
  cash_out_status varchar(1) DEFAULT NULL COMMENT '出款状态0-待出款1-出款中8-出款成功9-出款失败3-暂停出款',
  inst_account_no varchar(10) DEFAULT NULL COMMENT '银行内部号',
  pay_decision_id varchar(32) DEFAULT NULL COMMENT '支付决策ID',
  pay_success_date varchar(8) DEFAULT NULL COMMENT '支付成功日期',
  pay_success_time varchar(6) DEFAULT NULL COMMENT '支付成功时间',
  success_inst_account_no varchar(10) DEFAULT NULL COMMENT '支付成功的银行内部号',
  sign_inst_acct_no varchar(24) DEFAULT '' COMMENT '签约账号',
  error_msg varchar(4096) DEFAULT NULL COMMENT '错误信息',
  ack_no varchar(32) DEFAULT NULL,
  batch_no varchar(32) DEFAULT NULL COMMENT '批次号',
  advance_type varchar(2) NOT NULL DEFAULT '00' COMMENT '垫资类别',
  is_mock_ta varchar(1) NOT NULL DEFAULT '0' COMMENT '是否mock调用TA',
  ext varchar(1024) DEFAULT NULL COMMENT '扩展字段',
  create_time datetime DEFAULT NULL COMMENT '创建时间',
  modify_time datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY idx_uk_mht_trade_cash_out_info (cash_out_id,card_idx_no),
  KEY idx_mht_trade_cash_out_info_ack (ack_no),
  KEY idx_advance_type (advance_type),
  KEY idx_cash_out_info_trans_id (trans_id),
  KEY idx_pay_success_date (pay_success_date),
  KEY idx_resid_stno_cdno_chtp (request_id,step_no,card_idx_no,cash_type),
  KEY idx_batch_no (batch_no),
  KEY idx_csotdt_advctp_cashts (cash_out_date,advance_type,cash_out_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=39342863 COMMENT='交易出款信息表'`)
	tk.MustExec(`CREATE TABLE mht_trade_request_detail (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  request_id varchar(32) NOT NULL COMMENT '请求单号',
  trans_id varchar(32) DEFAULT NULL COMMENT '交易单号',
  trans_detail_id varchar(32) DEFAULT NULL COMMENT '多卡时明细交易单号',
  card_idx_no varchar(64) NOT NULL DEFAULT '' COMMENT '多卡时索引卡号',
  inst_id varchar(20) DEFAULT NULL COMMENT '银行id',
  inst_account_no varchar(10) DEFAULT NULL COMMENT '银行内部号',
  app_amt bigint(16) DEFAULT NULL COMMENT '申请金额',
  app_quty bigint(16) DEFAULT NULL COMMENT '申请份额',
  ack_amt bigint(16) DEFAULT NULL COMMENT '确认金额',
  ack_quty bigint(16) DEFAULT NULL COMMENT '确认份额',
  target_ack_quty bigint(16) DEFAULT NULL COMMENT '目标确认份额',
  cash_type varchar(1) NOT NULL DEFAULT '' COMMENT '资金类型',
  pay_success_date varchar(8) DEFAULT NULL COMMENT '支付成功日期',
  pay_success_time varchar(6) DEFAULT NULL COMMENT '支付成功时间',
  success_inst_account_no varchar(10) DEFAULT NULL,
  create_time datetime DEFAULT NULL COMMENT '创建时间',
  modify_time datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY uniq_key (request_id,card_idx_no,cash_type),
  UNIQUE KEY idx_uk_mht_trade_request_detail (trans_id,card_idx_no,cash_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=19340544 COMMENT='交易请求子表'`)
	tk.MustExec(`CREATE TABLE mht_trade_request (
  request_id varchar(32) NOT NULL COMMENT '请求单号',
  trans_id varchar(32) NOT NULL COMMENT '交易单号',
  ref_trans_id varchar(32) DEFAULT NULL COMMENT '关联交易单号-撤单或者转换',
  sub_trans_code varchar(10) DEFAULT NULL COMMENT '交易类型-具体类型',
  user_id varchar(32) DEFAULT NULL COMMENT '用户id',
  business_scenario_code varchar(10) NOT NULL DEFAULT '0101' COMMENT '业务场景类型子类型编码',
  business_scenario_type varchar(2) NOT NULL DEFAULT '01' COMMENT '业务场景类型',
  asset_acct_id varchar(32) DEFAULT NULL COMMENT '用户交易账号',
  product_id varchar(16) DEFAULT NULL COMMENT '产品代码',
  target_product_id varchar(16) DEFAULT NULL COMMENT '目标产品代码',
  melon_method varchar(1) DEFAULT NULL COMMENT '分红方式',
  trans_detail_id varchar(32) DEFAULT NULL COMMENT '单卡时明细交易单号，多卡时为空',
  card_idx_no varchar(64) DEFAULT NULL COMMENT '单卡时索引卡号，多卡时为空',
  inst_id varchar(20) DEFAULT NULL COMMENT '银行id',
  inst_account_no varchar(10) DEFAULT NULL COMMENT '银行内部号',
  branch_code char(4) DEFAULT '0001' COMMENT '销售机构代码',
  sale_channel_no varchar(6) DEFAULT '' COMMENT '所属渠道号',
  source_code varchar(17) DEFAULT '' COMMENT '交易来源',
  fof_id varchar(20) DEFAULT NULL COMMENT '组合id',
  fof_batch_id varchar(32) DEFAULT NULL COMMENT '组合批次ID',
  target_porfit_batch_id varchar(32) DEFAULT '000000000000000000000000000000' COMMENT '止盈定投冻结单号',
  app_amt bigint(16) DEFAULT NULL COMMENT '申请金额',
  app_quty bigint(16) DEFAULT NULL COMMENT '申请份额',
  ack_amt bigint(16) DEFAULT NULL COMMENT '确认金额',
  ack_quty bigint(16) DEFAULT NULL COMMENT '确认份额',
  target_ack_quty bigint(16) DEFAULT NULL COMMENT '目标确认份额',
  cash_type varchar(1) DEFAULT NULL COMMENT '资金渠道(V货币基金B银行)',
  pay_success_date varchar(8) DEFAULT NULL COMMENT '支付成功日期',
  pay_success_time varchar(6) DEFAULT NULL COMMENT '支付成功时间',
  success_inst_account_no varchar(10) DEFAULT NULL,
  work_date varchar(8) DEFAULT NULL COMMENT '工作日',
  apply_date varchar(8) DEFAULT NULL COMMENT '自然日',
  apply_time varchar(6) DEFAULT NULL COMMENT '自然日',
  discount decimal(8,4) DEFAULT NULL COMMENT '费用折扣',
  large_redemption_flag varchar(1) DEFAULT NULL COMMENT '巨额赎回标志',
  multi_step tinyint(1) DEFAULT NULL COMMENT '是否有多步骤',
  step_code varchar(4) DEFAULT NULL COMMENT '当前步骤代码',
  step_status varchar(2) DEFAULT NULL COMMENT '当前步骤状态',
  ext_app_no varchar(40) DEFAULT NULL COMMENT '外部申请流水号',
  ref_step_no int(2) DEFAULT NULL COMMENT '关联步骤代码',
  ack_date varchar(8) DEFAULT NULL COMMENT '申请确认日期',
  error_code varchar(6) DEFAULT NULL COMMENT '当前步骤返回代码',
  error_message varchar(4096) DEFAULT NULL COMMENT '当前步骤返回信息',
  fee_cnt int(2) DEFAULT NULL,
  ack_no varchar(32) DEFAULT NULL COMMENT '确认流水号',
  fee_type varchar(2) DEFAULT NULL,
  fee decimal(10,2) DEFAULT NULL,
  multi_part_ack tinyint(1) DEFAULT NULL,
  ext varchar(1024) DEFAULT NULL COMMENT '交易信息拓展字段',
  bank_order_no varchar(35) DEFAULT '' COMMENT '支付订单号',
  create_time datetime DEFAULT NULL COMMENT '创建时间',
  modify_time datetime DEFAULT NULL COMMENT '修改时间',
  target_branch_code varchar(9) NOT NULL DEFAULT '' COMMENT '对方网点号 对跨市场转托管业务，此字段必填，填写要转入的上海或深圳的席位号。',
  target_distributor_code varchar(9) NOT NULL DEFAULT '' COMMENT '对方销售人代码',
  target_transaction_account_id varchar(17) NOT NULL DEFAULT '' COMMENT '对方销售人处投资人基金交易账号',
  version smallint(3) NOT NULL DEFAULT '1' COMMENT '版本号',
  PRIMARY KEY (request_id) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY idx_uk_mht_trade_request (trans_id),
  KEY idx_ext_app_no (ext_app_no),
  KEY idx_mht_trade_request_asset_acct_id_target_pbi (asset_acct_id,target_porfit_batch_id),
  KEY idx_idx_mht_trade_request_psy_succ_date (pay_success_date),
  KEY idx_work_date (work_date),
  KEY idx_recordidx_prdid (product_id),
  KEY idx_recordidx_sbtscd_stpsts (sub_trans_code,step_status),
  KEY idx_step_status (step_status),
  KEY idx_sub_trans_code (sub_trans_code),
  KEY idx_branch_code (branch_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易请求表'`)
	tk.MustExec(`CREATE TABLE mht_user_account (
  id int(11) NOT NULL AUTO_INCREMENT,
  asset_acct_id varchar(32) NOT NULL COMMENT '用户交易账号',
  user_id varchar(32) DEFAULT NULL COMMENT '用户id',
  mobile_no varchar(32) DEFAULT NULL COMMENT '手机号',
  mobile_no_md5 varchar(32) DEFAULT NULL COMMENT '手机号码MD5',
  cert_type varchar(1) DEFAULT NULL COMMENT '证件类型',
  cert_no varchar(32) DEFAULT NULL COMMENT '证件号',
  customer_name varchar(128) DEFAULT NULL COMMENT '客户姓名',
  gmt_create datetime DEFAULT NULL COMMENT '创建时间',
  gmt_modified datetime DEFAULT NULL COMMENT '修改时间',
  branch_code char(4) DEFAULT '0001' COMMENT '销售渠道',
  acct_type char(1) NOT NULL DEFAULT '1' COMMENT '账户类型，0:机构 1:个人 2:产品',
  org_cash_type varchar(4) DEFAULT NULL COMMENT '机构资金监管模式',
  transactor_cert_type char(1) NOT NULL DEFAULT '' COMMENT '机构经办人证件类型 0-身份证，1-护照 2-军官证，3-士兵证 4-港澳居民来往内地通行证，5-户口本 6-外国护照，7-其它 8-文职证，9-警官证 A-台胞证',
  transactor_cert_no varchar(30) NOT NULL DEFAULT '' COMMENT '机构经办人证件号码',
  transactor_name varchar(20) NOT NULL DEFAULT '' COMMENT '机构经办人姓名',
  institution_type char(3) NOT NULL DEFAULT '' COMMENT '机构/产品类型\\n若为机构分类如下：\\n0-保险公司，1-基金管理公司，3-信托公司，4-证券公司，8-其他，9-银行；A:私募基金管理人；B:期货公司；C-基金管理公司子公司；D-证券公司子公司；E-期货公司子公司；F-财务公司；G:其他境内金融机构；H:机关法人；I:事业单位法人；J:社会团体法人；K:非金融机构企业法人；L:非金融类非法人机构；M:境外代理人；N:境外金融机构； P:外国战略投资者；Q:境外非金融机构。\\n若为产品分类如下：\\n6-企业年金及职业年金，8-其他，9:开放式公募基金产品；A:封闭式公募基金产品；B:银行理财产品；C:信托计划；D:基金公司专户；E:基金子公司产品；F:保险产品；G:保险公司及其子公司的资产管理计划；H:证券公司集合理财产品（含证券公司大集合）；I:证券公司专项资管计划；J:证券公司定向资管计划；K:期货公司及其子公司的资产管理计划',
  product_account_card_id varchar(30) DEFAULT NULL,
  t0_upgrade_status varchar(1) DEFAULT NULL COMMENT 'T0货币升级状态,I-升级中,S-升级完成,空-未升级',
  t0_upgrade_time datetime DEFAULT NULL COMMENT 'T0货币升级时间',
  zone varchar(4) NOT NULL DEFAULT '' COMMENT '分片区间',
  ext varchar(1024) DEFAULT '' COMMENT '扩展信息',
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY idx_mht_user_account_userid (user_id),
  KEY idx_asset_acct_id (asset_acct_id),
  KEY idx_cert_no (cert_no),
  KEY idx_bcd_zne_asscctid (branch_code,zone,asset_acct_id),
  KEY idx_gmt_create (gmt_create)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1479283 COMMENT='用户资产账号表'`)
	tk.MustQuery(`explain 
SELECT /*+ LEADING(tt,d,e) */
tt.request_id, tt.step_no, tt.cash_out_date, tt.real_cash_out_date, tt.advance_natural_days
FROM mht_trade_cash_out_info tt
left join mht_trade_audit_info d on tt.cash_out_id = d.app_no and d.info_type = 'C'
join mht_trade_request e on tt.request_id = e.request_id
join mht_user_account f on e.asset_acct_id = f.asset_acct_id
WHERE 1=1
#and d.audit_status =0
and d.audit_status is null
AND tt.cash_out_date >= '20250320'
AND tt.cash_out_date <= '20250328'
AND tt.advance_type in ('03', '02', '04', '00');`)
	tk.MustQuery("show warnings").Check(testkit.Rows())
}
