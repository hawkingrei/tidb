# Redundant USING/NATURAL JOIN Notes

## 2026-03-09 - Qualified redundant-column remap must preserve projection identity and predicate type semantics

Background:
- Issue #66272 originally required remapping redundant `JOIN ... USING` / `NATURAL JOIN` columns so later planner phases do not keep an unresolvable redundant-side column.
- Two follow-up review findings showed the first fix was too broad:
  - projection metadata for `SELECT t_right.col` could be mislabeled as the canonical visible side,
  - `WHERE/HAVING` remap could silently change predicate semantics when the redundant and visible columns had different types.

Key takeaways:
- Projection naming and predicate remapping have different correctness constraints.
- For projection metadata, keep the original redundant-side `FullNames` entry so `ResultField` table/original-table metadata still matches the selected column.
- For predicate remapping, only reuse the canonical visible column when the join is an inner join and both sides have identical `RetType`.

Implementation choice:
- `findColFromNaturalUsingJoin` now reads the redundant-side identity from `FullSchema`/`FullNames` instead of `ResolveRedundantColumn`.
- `LogicalJoin.ResolveRedundantColumn` now returns a mapped column only when the redundant-side and visible-side `RetType` values are equal.

Regression coverage:
- `SELECT t3.id FROM t1 JOIN t3 USING(id)` verifies result-field metadata still reports `t3`.
- Mixed-type `VARCHAR`/`INT` `USING(id)` with `WHERE t_mixed_r.id = '01a'` verifies qualified predicates keep right-side integer semantics.

Validation commands:
- `make bazel_prepare`
- `go test ./pkg/planner/core/casetest/schema -run ^TestSchemaCannotFindColumnRegression$ -tags=intest,deadlock`
- `make lint`
