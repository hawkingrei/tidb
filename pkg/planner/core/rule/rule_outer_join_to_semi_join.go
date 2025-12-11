// Copyright 2025 PingCAP, Inc.
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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type OuterJoinToSemiJoin struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (o *OuterJoinToSemiJoin) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	return p, false, nil
}

func (e *OuterJoinToSemiJoin) recursivePlan(p base.LogicalPlan) (base.LogicalPlan, bool) {
	var isChanged bool
	for _, child := range p.Children() {
		if sel, ok := child.(*logicalop.LogicalSelection); ok {
			join, ok := sel.Children()[0].(*logicalop.LogicalJoin)
			if ok {
				newRet, ok := join.CanConvertAntiJoin(sel.Conditions, sel.Schema())
				if ok {
					join.JoinType = base.AntiSemiJoin
					if len(newRet) == 0 {
						p.SetChildren(join)
					} else {
						sel.Conditions = newRet
					}
					isChanged = true
				}
			}
		}
		_, changed := e.recursivePlan(child)
		isChanged = isChanged || changed
	}
	return p, isChanged
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*OuterJoinToSemiJoin) Name() string {
	return "outer_join_semi_join"
}
