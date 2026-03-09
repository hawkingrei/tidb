# Yannakakis+ Phase 1 Notes

## Scope

- Entry point: `pkg/planner/core/rule_yannakakis_plus.go`.
- Current rewrite scope is intentionally narrower than the paper:
  - top-level `DISTINCT` represented as `LogicalAggregation(first_row(...))`;
  - top-level `COUNT(*)` / `COUNT(1)` represented as one `count(constant)` plus
    optional `first_row(group_by_col)` outputs;
  - all output attribute classes must be dominated by the same leaf relation;
  - child is an acyclic inner equi-join component;
  - no outer join, non-equality join predicate, or generic aggregation yet.

## Why the Scope Is Narrow

- TiDB already rewrites `DISTINCT` into a `LogicalAggregation` whose
  `AggFuncs` are `first_row` and whose `GroupByItems` are output columns.
- TiDB already has distributed partial/final aggregation for single-relation
  `COUNT`; the Yannakakis+ rule therefore only targets join-count cases where
  subtree reduction can shrink intermediate joins.
- Restricting the `COUNT` rewrite to `count(constant)` avoids remapping
  null-sensitive arguments before we have a broader aggregate-annotation model.
- Phase 1 only permits remapping to equality-equivalent root columns with
  identical field types; it still avoids general expression remapping.

## Core Invariant

For a join tree node `v` and its parent `p`, let `I(v, p)` be the equality
attribute classes shared by the two sides.

`reduceDistinctSubtree(v, p)` returns a logical plan equivalent to:

- evaluate the original subtree rooted at `v`;
- project to `I(v, p)`;
- remove duplicates.

At the root, `reduceDistinctSubtree(root, nil)` returns the reduced join subtree
without the final root projection because the top-level `DISTINCT` aggregation
still sits above it.

For the count rewrite, let `summarizeCountSubtree(v, p)` denote the grouped
summary produced for a non-root node. Its invariant is:

- the output schema contains `I(v, p)` and one multiplicity column `m(v, p)`;
- for every interface assignment `x`, `m(v, p, x)` equals the number of tuples
  in the original subtree rooted at `v` that project to `x`.

## Proof Sketch

1. DISTINCT base case:
   - If `v` is a leaf, `reduceDistinctSubtree(v, p)` is exactly
     `DISTINCT(project leaf to I(v, p))`.
2. DISTINCT inductive step:
   - Assume each child rewrite returns the distinct interface relation promised
     by the invariant.
   - Joining the current relation with each reduced child on the shared
     equality classes preserves exactly the tuples of the original subtree that
     can participate in a full join.
   - Projecting the joined result back to the interface with the parent and
     applying `DISTINCT` re-establishes the invariant for `v`.
3. DISTINCT root:
   - Because all output attribute classes are dominated by the chosen root
     relation, and the top-level `DISTINCT` is remapped only to
     equality-equivalent root columns with identical field types, the original
     result is preserved after child subtrees are reduced to their join
     interfaces.
4. COUNT summary base case:
   - If `v` is a leaf, grouping the leaf by `I(v, p)` and aggregating
     `count(1)` yields exactly the multiplicity of each interface assignment.
5. COUNT summary inductive step:
   - Assume each child summary already exposes the exact multiplicity for its
     interface with `v`.
   - Joining the current relation with all child summaries reconstructs the
     valid subtree tuples, while the product of child multiplicities gives the
     number of descendant combinations attached to each current row.
   - Grouping by `I(v, p)` and summing that product yields the exact subtree
     multiplicity for each interface assignment.
6. COUNT root:
   - At the root, summing the product of child multiplicities over the chosen
     root relation yields the original join count.
   - Scalar count rewrites add `ifnull(..., 0)` above the rewritten `sum(...)`
     so the empty-input result stays equal to `count(*)`.

The only non-obvious step is synthetic edge selection: the implementation may
join two relations that were not direct neighbors in the original left-deep
join syntax, but only when they share the same equality attribute class after
union-find. This is sound for pure inner equi-joins because equality is
transitive inside the connected component.

## Code Checks That Back the Invariant

- `isDistinctLikeAgg` accepts only the exact `DISTINCT`-style aggregation
  shape.
- `extractCountLikeAggInfo` accepts only one `count(constant)` plus optional
  `first_row(group_by_col)` outputs.
- `collectInnerJoinGraph` rejects anything outside inner equi-joins.
- `buildYannakakisGraph` rejects duplicate equality-class columns inside a
  single relation and rejects disconnected relation sets.
- `deriveAcyclicRelationTree` uses GYO-style elimination and rejects graphs
  that cannot be reduced to one relation.

## What Phase 1 Does Not Prove

- Generic `GROUP BY` with decomposable aggregates.
- `COUNT(expr)` where `expr` may become `NULL` after remapping.
- Output remapping when the root representative would change field type.
- Cost-based selection among multiple legal join trees.
- Semijoin reduction with foreign-key or Bloom-filter style extensions.

## Suggested Validation

- Positive regression:
  - `select distinct t1.a from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select distinct t1.a, t3.b from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select count(*) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
  - `select t1.a, count(*) from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b group by t1.a`
- Negative regressions:
  - cyclic join graph;
  - single-table `count(*)`;
  - grouped count whose output attribute classes are not dominated by one relation;
  - `DISTINCT` whose output attribute classes are not dominated by one relation.
