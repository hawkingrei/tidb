<!--

Thank you for contributing to TiDB!

PR Title Format:
1. pkg [, pkg2, pkg3]: what's changed
2. *: what's changed

-->

### What problem does this PR solve?
<!--

Please create an issue first to describe the problem.

There MUST be one line starting with "Issue Number:  " and
linking the relevant issues via the "close" or "ref".

For more info, check https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/contribute-code.html#referring-to-an-issue.

-->

Issue Number: close #67586

Problem Summary:

`LEFT/RIGHT JOIN ... ON FALSE` is already simplified to an outer join whose inner side is `TableDual(rows:0)`, but the optimizer still keeps the outer join operator in the final plan.

### What changed and how does it work?

- add a special case in `OuterJoinEliminator` for outer joins whose inner child is `LogicalTableDual{RowCount: 0}`
- rewrite such joins into a projection on top of the outer child
- preserve outer-side columns and replace visible inner-side columns with typed `NULL`
- add planner assertions for `LEFT/RIGHT JOIN ... ON FALSE`
- update `tests/integrationtest/r/expression/explain.result`

### Check List

Tests <!-- At least one of them must be included. -->

- [x] Unit test
- [x] Integration test
- [ ] Manual test (add detailed scripts or steps below)
- [ ] No need to test
  > - [ ] I checked and no code files have been changed.
  > <!-- Or your custom  "No need to test" reasons -->

Validation notes:

- Updated planner test expectations for `select * from t1 left join t2 on false`
- Updated planner test expectations for `select * from t1 right join t2 on false`
- Local `go test ./pkg/planner/core -run TestOuterJoinElimination -count=1` was blocked by a machine-local linker failure:

```text
/opt/homebrew/Cellar/go/1.26.1/libexec/pkg/tool/darwin_arm64/link: mapping output file failed: no space left on device
```

Side effects

- [ ] Performance regression: Consumes more CPU
- [ ] Performance regression: Consumes more Memory
- [ ] Breaking backward compatibility

Documentation

- [ ] Affects user behaviors
- [ ] Contains syntax changes
- [ ] Contains variable changes
- [ ] Contains experimental features
- [ ] Changes MySQL compatibility

### Release note

<!-- compatibility change, improvement, bugfix, and new feature need a release note -->

Please refer to [Release Notes Language Style Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html) to write a quality release note.

```release-note
None
```
