// Copyright 2026 PingCAP, Inc.
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

package windows

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// Build constructs the concrete executor for a window physical plan.
//
// The executor builder in pkg/executor still owns plan-tree traversal. This helper
// only encapsulates the window-specific state initialization so the executor logic
// can live under pkg/executor/windows without duplicating builder code in multiple
// call sites such as ordinary execution and IndexJoin inner-side execution.
func Build(sctx sessionctx.Context, v *physicalop.PhysicalWindow, childExec exec.Executor, forcePipelined bool) (exec.Executor, error) {
	base := exec.NewBaseExecutor(sctx, v.Schema(), v.ID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.Col)
	}
	orderByCols := make([]*expression.Column, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByCols = append(orderByCols, item.Col)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultColIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	exprCtx := sctx.GetExprCtx()
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDescForWindowFunc(exprCtx, desc, false)
		if err != nil {
			return nil, err
		}
		agg := aggfuncs.BuildWindowFunctions(exprCtx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultColIdx++
	}

	if forcePipelined || sctx.GetSessionVars().EnablePipelinedWindowExec {
		exec := &PipelinedWindowExec{
			BaseExecutor:   base,
			groupChecker:   vecgroupchecker.NewVecGroupChecker(sctx.GetExprCtx().GetEvalCtx(), sctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
			numWindowFuncs: len(v.WindowFuncDescs),
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
		exec.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(exec.windowFuncs))
		for i, windowFunc := range exec.windowFuncs {
			if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
				exec.slidingWindowFuncs[i] = slidingWindowAggFunc
			}
		}
		if v.Frame == nil {
			// Nil frame is treated as the whole partition so all window executors can
			// share the same start/end machinery with explicit frame specifications.
			exec.start = &logicalop.FrameBound{Type: ast.Preceding, UnBounded: true}
			exec.end = &logicalop.FrameBound{Type: ast.Following, UnBounded: true}
		} else {
			exec.start = v.Frame.Start
			exec.end = v.Frame.End
			if v.Frame.Type == ast.Ranges {
				cmpResult := int64(-1)
				if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
					cmpResult = 1
				}
				exec.orderByCols = orderByCols
				exec.expectedCmpResult = cmpResult
				exec.isRangeFrame = true
				// RANGE frames repeatedly compare candidate rows against the current row.
				// Precomputing compare columns here keeps the produce path cheaper.
				if err := exec.start.UpdateCompareCols(sctx, exec.orderByCols); err != nil {
					return nil, err
				}
				if err := exec.end.UpdateCompareCols(sctx, exec.orderByCols); err != nil {
					return nil, err
				}
			}
		}
		return exec, nil
	}

	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Rows {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		tmpProcessor := &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByCols:       orderByCols,
			expectedCmpResult: cmpResult,
		}
		if err := tmpProcessor.start.UpdateCompareCols(sctx, orderByCols); err != nil {
			return nil, err
		}
		if err := tmpProcessor.end.UpdateCompareCols(sctx, orderByCols); err != nil {
			return nil, err
		}
		processor = tmpProcessor
	}
	return &WindowExec{
		BaseExecutor:   base,
		processor:      processor,
		groupChecker:   vecgroupchecker.NewVecGroupChecker(sctx.GetExprCtx().GetEvalCtx(), sctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}, nil
}

// BuildStream constructs the stream-window executor used by the planner's
// ordered-input path. StreamWindow is intentionally backed by the pipelined
// executor because it has to emit rows before seeing the whole partition.
func BuildStream(sctx sessionctx.Context, v *physicalop.PhysicalWindow, childExec exec.Executor) (*StreamWindowExec, error) {
	windowExec, err := Build(sctx, v, childExec, true)
	if err != nil {
		return nil, err
	}
	pipelinedExec, ok := windowExec.(*PipelinedWindowExec)
	if !ok {
		return nil, errors.New("stream window must be built with pipelined window executor")
	}
	return &StreamWindowExec{PipelinedWindowExec: pipelinedExec}, nil
}

// NewPartitionTopNExec constructs the specialized row_number upper-bound executor.
//
// This executor is intentionally narrow: it only serves the row_number <= K style
// optimization used by StreamWindow. General window execution still goes through
// Build/BuildStream above.
func NewPartitionTopNExec(sctx sessionctx.Context, v *physicalop.PhysicalStreamWindow, childExec exec.Executor, limitCount uint64) (*PartitionTopNWindowExec, error) {
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.Col)
	}
	partitionTopNExec := &PartitionTopNWindowExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, v.Schema(), v.ID(), childExec),
		groupChecker: vecgroupchecker.NewVecGroupChecker(sctx.GetExprCtx().GetEvalCtx(), sctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
		limitCount:   limitCount,
		resultColIdx: v.Schema().Len() - 1,
	}
	if err := partitionTopNExec.OpenSelf(); err != nil {
		return nil, err
	}
	return partitionTopNExec, nil
}
