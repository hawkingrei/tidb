// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild base.LogicalPlan, eqConds []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType logicalop.JoinType, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan
}

type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
}

func (s *joinReorderDPSolver) solve(joinGroup []base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	eqConds := expression.ScalarFuncs2Exprs(s.eqEdges)
	for _, node := range joinGroup {
		_, _, err := node.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: cost,
		})
		tracer.appendLogicalJoinCost(node, cost)
	}
	adjacents := make([][]int, len(s.curJoinGroup))
	totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}
	// Build Graph for join group
	for _, cond := range eqConds {
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx, err := findNodeIndexInGroup(joinGroup, lCol)
		if err != nil {
			return nil, err
		}
		rIdx, err := findNodeIndexInGroup(joinGroup, rCol)
		if err != nil {
			return nil, err
		}
		addEqEdge(lIdx, rIdx, sf)
	}
	totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		cols := expression.ExtractColumns(cond)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			idx, err := findNodeIndexInGroup(joinGroup, col)
			if err != nil {
				return nil, err
			}
			ids = append(ids, idx)
			mask |= 1 << uint(idx)
		}
		totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
			nodeIDs:    ids,
			nodeIDMask: mask,
			expr:       cond,
		})
	}
	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	joins := make([]base.LogicalPlan, 0, len(joinGroup))
	// BFS the tree.
	for i := range joinGroup {
		if visited[i] {
			continue
		}
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		nodeIDMask := uint(0)
		for _, nodeID := range visitID2NodeID {
			nodeIDMask |= 1 << uint(nodeID)
		}
		var subNonEqEdges []joinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			// If this edge is not the subset of the current sub graph.
			if totalNonEqEdges[i].nodeIDMask&nodeIDMask != totalNonEqEdges[i].nodeIDMask {
				continue
			}
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].nodeIDs {
				newMask |= 1 << uint(nodeID2VisitID[nodeID])
			}
			totalNonEqEdges[i].nodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = slices.Delete(totalNonEqEdges, i, i+1)
		}
		// Do DP on each sub graph.
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEqEdges, subNonEqEdges, tracer)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		remainedOtherConds = append(remainedOtherConds, edge.expr)
	}
	// Build bushy tree for cartesian joins.
	return s.makeBushyJoin(joins, remainedOtherConds, tracer.opt), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (*joinReorderDPSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VisitID []int) []int {
	queue := []int{startNode}
	visited[startNode] = true
	var visitID2NodeID []int
	for len(queue) > 0 {
		curNodeID := queue[0]
		queue = queue[1:]
		nodeID2VisitID[curNodeID] = len(visitID2NodeID)
		visitID2NodeID = append(visitID2NodeID, curNodeID)
		for _, adjNodeID := range adjacents[curNodeID] {
			if visited[adjNodeID] {
				continue
			}
			queue = append(queue, adjNodeID)
			visited[adjNodeID] = true
		}
	}
	return visitID2NodeID
}

// dpGraph is the core part of this algorithm.
// It implements the traditional join reorder algorithm: DP by subset using the following formula:
//
//	bestPlan[S:set of node] = the best one among Join(bestPlan[S1:subset of S], bestPlan[S2: S/S1])
func (s *joinReorderDPSolver) dpGraph(visitID2NodeID, nodeID2VisitID []int, _ []base.LogicalPlan,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	nodeCnt := uint(len(visitID2NodeID))
	bestPlan := make([]*jrNode, 1<<nodeCnt)
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := range nodeCnt {
		bestPlan[1<<i] = s.curJoinGroup[visitID2NodeID[i]]
	}
	// Enumerate the nodeBitmap from small to big, make sure that S1 must be enumerated before S2 if S1 belongs to S2.
	for nodeBitmap := uint(1); nodeBitmap < (1 << nodeCnt); nodeBitmap++ {
		if bits.OnesCount(nodeBitmap) == 1 {
			continue
		}
		// This loop can iterate all its subset.
		for sub := (nodeBitmap - 1) & nodeBitmap; sub > 0; sub = (sub - 1) & nodeBitmap {
			remain := nodeBitmap ^ sub
			if sub > remain {
				continue
			}
			// If this subset is not connected skip it.
			if bestPlan[sub] == nil || bestPlan[remain] == nil {
				continue
			}
			// Get the edge connecting the two parts.
			usedEdges, otherConds := s.nodesAreConnected(sub, remain, nodeID2VisitID, totalEqEdges, totalNonEqEdges)
			// Here we only check equal condition currently.
			if len(usedEdges) == 0 {
				continue
			}
			join, err := s.newJoinWithEdge(bestPlan[sub].p, bestPlan[remain].p, usedEdges, otherConds, tracer.opt)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(join, bestPlan[sub], bestPlan[remain])
			tracer.appendLogicalJoinCost(join, curCost)
			if bestPlan[nodeBitmap] == nil {
				bestPlan[nodeBitmap] = &jrNode{
					p:       join,
					cumCost: curCost,
				}
			} else if bestPlan[nodeBitmap].cumCost > curCost {
				bestPlan[nodeBitmap].p = join
				bestPlan[nodeBitmap].cumCost = curCost
			}
		}
	}
	return bestPlan[(1<<nodeCnt)-1].p, nil
}

func (*joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) ([]joinGroupEqEdge, []expression.Expression) {
	var usedEqEdges []joinGroupEqEdge
	for _, edge := range totalEqEdges {
		lIdx := uint(oldPos2NewPos[edge.nodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.nodeIDs[1]])
		if ((leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0) || ((leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0) {
			usedEqEdges = append(usedEqEdges, edge)
		}
	}
	otherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		// If the result is false, means that the current group hasn't covered the columns involved in the expression.
		if edge.nodeIDMask&(leftMask|rightMask) != edge.nodeIDMask {
			continue
		}
		// Check whether this expression is only built from one side of the join.
		if edge.nodeIDMask&leftMask == 0 || edge.nodeIDMask&rightMask == 0 {
			continue
		}
		otherConds = append(otherConds, edge.expr)
	}
	return usedEqEdges, otherConds
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan base.LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), ast.EQ, edge.edge.GetStaticType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds, nil, nil, logicalop.InnerJoin, opt)
	_, _, err := join.RecursiveDeriveStats(nil)
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan, otherConds []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]base.LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds, nil, nil, logicalop.InnerJoin, opt))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []base.LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}
