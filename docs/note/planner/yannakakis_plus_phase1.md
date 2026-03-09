# Yannakakis+ Phase 1 Notes

## Scope

- Entry point: `pkg/planner/core/rule_yannakakis_plus.go`.
- Current rewrite scope is intentionally narrower than the paper:
  - top-level `DISTINCT` represented as `LogicalAggregation(first_row(...))`;
  - all output columns must come from the same leaf relation;
  - child is an acyclic inner equi-join component;
  - no outer join, non-equality join predicate, or generic aggregation yet.

## Why the Scope Is Narrow

- TiDB already rewrites `DISTINCT` into a `LogicalAggregation` whose
  `AggFuncs` are `first_row` and whose `GroupByItems` are output columns.
- Restricting phase 1 to this shape avoids arguing about multiplicity-sensitive
  aggregates before we have annotation propagation or aggregate decomposition.
- Requiring the final output columns to come from one relation avoids column
  remapping across equality classes in the first commit.

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

## Proof Sketch

1. Base case:
   - If `v` is a leaf, `reduceDistinctSubtree(v, p)` is exactly
     `DISTINCT(project leaf to I(v, p))`.
2. Inductive step:
   - Assume each child rewrite returns the distinct interface relation promised
     by the invariant.
   - Joining the current relation with each reduced child on the shared
     equality classes preserves exactly the tuples of the original subtree that
     can participate in a full join.
   - Projecting the joined result back to the interface with the parent and
     applying `DISTINCT` re-establishes the invariant for `v`.
3. Root:
   - Because all output columns are owned by the chosen root relation, the
     original top-level `DISTINCT` still computes the same result after child
     subtrees are reduced to their join interfaces.

The only non-obvious step is synthetic edge selection: the implementation may
join two relations that were not direct neighbors in the original left-deep
join syntax, but only when they share the same equality attribute class after
union-find. This is sound for pure inner equi-joins because equality is
transitive inside the connected component.

## Code Checks That Back the Invariant

- `isDistinctLikeAgg` accepts only the exact `DISTINCT`-style aggregation
  shape.
- `collectInnerJoinGraph` rejects anything outside inner equi-joins.
- `buildYannakakisGraph` rejects duplicate equality-class columns inside a
  single relation and rejects disconnected relation sets.
- `deriveAcyclicRelationTree` uses GYO-style elimination and rejects graphs
  that cannot be reduced to one relation.

## What Phase 1 Does Not Prove

- Generic `GROUP BY` with decomposable aggregates.
- Output remapping through equivalent columns not owned by the root relation.
- Cost-based selection among multiple legal join trees.
- Semijoin reduction with foreign-key or Bloom-filter style extensions.

## Suggested Validation

- Positive regression:
  - `select distinct t1.a from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b`
- Negative regressions:
  - cyclic join graph;
  - `count(*)` over the same join;
  - `DISTINCT` whose outputs come from multiple leaf relations.
