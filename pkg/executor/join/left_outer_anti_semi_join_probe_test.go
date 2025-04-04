// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func genLeftOuterAntiSemiJoinResult(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType) []*chunk.Chunk {
	return genLeftOuterSemiOrSemiJoinOrLeftOuterAntiSemiResultImpl(t, sessCtx, leftFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes, leftKeyTypes, rightKeyTypes, leftUsedColumns, otherConditions, resultTypes, true, true)
}

func TestLeftOuterAntiSemiJoinProbeBasic(t *testing.T) {
	testLeftOuterSemiOrSemiJoinProbeBasic(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeAllJoinKeys(t *testing.T) {
	testLeftOuterSemiJoinProbeAllJoinKeys(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeOtherCondition(t *testing.T) {
	testLeftOuterSemiJoinProbeOtherCondition(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeWithSel(t *testing.T) {
	testLeftOuterSemiJoinProbeWithSel(t, true, true)
}

func TestLeftOuterAntiSemiJoinBuildResultFastPath(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinBuildResultFastPath(t, true)
}

func TestLeftOuterAntiSemiJoinSpill(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinSpill(t, true)
}
