## Enhancement

Suggested labels: `type/enhancement`, `sig/planner`

TiDB already folds `LEFT/RIGHT JOIN ... ON FALSE` into an outer join whose inner child is `TableDual(rows:0)`, but it still keeps the outer join operator in the final plan.

### Problem

For queries such as:

```sql
CREATE TABLE t1(id BIGINT PRIMARY KEY, a INT, b INT);
CREATE TABLE t2(id BIGINT PRIMARY KEY, a INT, b INT);

EXPLAIN FORMAT = 'plan_tree' SELECT * FROM t1 LEFT JOIN t2 ON FALSE;
```

the current plan still contains an outer join with an empty inner child:

```text
HashJoin
+- TableDual(rows:0)
\- TableReader(t1)
```

This misses a straightforward join elimination opportunity. The result of the outer join is fully determined by the outer side, and every visible inner-side column can be replaced with `NULL`.

### Expected behavior

When the optimizer can prove that the inner side of an outer join produces zero rows, it should eliminate the outer join and rewrite the plan to:

- keep the outer child unchanged
- project outer-side columns directly
- replace visible inner-side columns with typed `NULL`

For the example above, the plan should become a projection over `t1` rather than a `HashJoin` with `TableDual`.

### Benefit

- removes an unnecessary outer join operator
- improves parity with the `LEFT JOIN + FALSE COND` join elimination behavior described by other optimizers
- produces a simpler logical and physical plan for an obvious degenerate case

### Validation

- planner unit test for `select * from t1 left join t2 on false`
- planner unit test for `select * from t1 right join t2 on false`
- update `tests/integrationtest/r/expression/explain.result`

### Environment

Current local validation was blocked by a machine-local linker failure:

```text
/opt/homebrew/Cellar/go/1.26.1/libexec/pkg/tool/darwin_arm64/link: mapping output file failed: no space left on device
```
