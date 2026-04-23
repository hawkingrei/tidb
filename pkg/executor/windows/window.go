// Copyright 2019 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// WindowExec is the blocking implementation for window functions.
//
// Unlike PipelinedWindowExec, this executor materializes each partition before
// appending window columns back into already-buffered output chunks. That keeps
// the implementation simple for the generic path while still reusing the same
// chunk references for pass-through columns.
type WindowExec struct {
	exec.BaseExecutor

	groupChecker         *vecgroupchecker.VecGroupChecker
	childResult          *chunk.Chunk
	executed             bool
	resultChunks         []*chunk.Chunk
	remainingRowsInChunk []int

	numWindowFuncs int
	processor      windowProcessor
}

// Close releases the executor and all child resources.
func (e *WindowExec) Close() error { return errors.Trace(e.BaseExecutor.Close()) }

// Next returns the next chunk whose window columns have already been materialized.
// The executor may fetch more child chunks internally until a complete partition
// has been processed and the front buffered chunk is ready to return.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !e.executed && !e.preparedChunkAvailable() {
		err := e.consumeOneGroup(ctx)
		if err != nil {
			e.executed = true
			return err
		}
	}
	if len(e.resultChunks) > 0 {
		chk.SwapColumns(e.resultChunks[0])
		e.resultChunks[0] = nil
		e.resultChunks = e.resultChunks[1:]
		e.remainingRowsInChunk = e.remainingRowsInChunk[1:]
	}
	return nil
}

// preparedChunkAvailable reports whether the first buffered result chunk already
// has all its window columns appended. Chunks may be fetched earlier than they
// can be returned because a partition can span multiple input chunks.
func (e *WindowExec) preparedChunkAvailable() bool {
	return len(e.resultChunks) > 0 && e.remainingRowsInChunk[0] == 0
}

func (e *WindowExec) consumeOneGroup(ctx context.Context) error {
	var groupRows []chunk.Row
	if e.groupChecker.IsExhausted() {
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupRows(groupRows)
		}
		_, err = e.groupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.GetNextGroup()
	for i := begin; i < end; i++ {
		groupRows = append(groupRows, e.childResult.GetRow(i))
	}

	// If the current group reaches the end of the chunk, the same partition may
	// continue in the next child chunk. Keep fetching until the partition boundary
	// is certain, then hand the whole partition to the processor.
	for meetLastGroup := end == e.childResult.NumRows(); meetLastGroup; {
		meetLastGroup = false
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupRows(groupRows)
		}
		isFirstGroupSameAsPrev, err := e.groupChecker.SplitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.GetNextGroup()
			for i := begin; i < end; i++ {
				groupRows = append(groupRows, e.childResult.GetRow(i))
			}
			meetLastGroup = end == e.childResult.NumRows()
		}
	}
	return e.consumeGroupRows(groupRows)
}

func (e *WindowExec) consumeGroupRows(groupRows []chunk.Row) (err error) {
	remainingRowsInGroup := len(groupRows)
	if remainingRowsInGroup == 0 {
		return nil
	}
	for i := range e.resultChunks {
		// resultChunks and remainingRowsInChunk are aligned FIFO queues. We keep
		// subtracting rows from the front until the current partition has been fully
		// written back to every buffered chunk it spans.
		remained := min(e.remainingRowsInChunk[i], remainingRowsInGroup)
		e.remainingRowsInChunk[i] -= remained
		remainingRowsInGroup -= remained
		groupRows, err = e.processor.consumeGroupRows(e.Ctx(), groupRows)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = e.processor.appendResult2Chunk(e.Ctx(), groupRows, e.resultChunks[i], remained)
		if err != nil {
			return errors.Trace(err)
		}
		if remainingRowsInGroup == 0 {
			e.processor.resetPartialResult()
			break
		}
	}
	return nil
}

func (e *WindowExec) fetchChild(ctx context.Context) (eof bool, err error) {
	childResult := exec.TryNewCacheChunk(e.Children(0))
	err = exec.Next(ctx, e.Children(0), childResult)
	if err != nil {
		return false, errors.Trace(err)
	}
	numRows := childResult.NumRows()
	if numRows == 0 {
		return true, nil
	}
	resultChk := e.AllocPool.Alloc(e.RetFieldTypes(), 0, numRows)
	err = e.copyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingRowsInChunk = append(e.remainingRowsInChunk, numRows)
	e.childResult = childResult
	return false, nil
}

func (e *WindowExec) copyChk(src, dst *chunk.Chunk) error {
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		if err := dst.MakeRefTo(i, src, col.Index); err != nil {
			return err
		}
	}
	return nil
}

type windowProcessor interface {
	consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error)
	appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error)
	resetPartialResult()
}

// aggWindowProcessor handles the whole-partition case where every row in the
// partition sees the same frame result.
type aggWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
}

func (p *aggWindowProcessor) consumeGroupRows(ctx sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	for i, windowFunc := range p.windowFuncs {
		_, err := windowFunc.UpdatePartialResult(ctx.GetExprCtx().GetEvalCtx(), rows, p.partialResults[i])
		if err != nil {
			return nil, err
		}
	}
	rows = rows[:0]
	return rows, nil
}

func (p *aggWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	for remained > 0 {
		for i, windowFunc := range p.windowFuncs {
			if err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk); err != nil {
				return nil, err
			}
		}
		remained--
	}
	return rows, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}

type rowFrameWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	start          *logicalop.FrameBound
	end            *logicalop.FrameBound
	curRowIdx      uint64
}

func (p *rowFrameWindowProcessor) getStartOffset(numRows uint64) uint64 {
	if p.start.UnBounded {
		return 0
	}
	switch p.start.Type {
	case ast.Preceding:
		if p.curRowIdx >= p.start.Num {
			return p.curRowIdx - p.start.Num
		}
		return 0
	case ast.Following:
		offset := p.curRowIdx + p.start.Num
		if offset >= numRows {
			return numRows
		}
		return offset
	case ast.CurrentRow:
		return p.curRowIdx
	}
	return 0
}

func (p *rowFrameWindowProcessor) getEndOffset(numRows uint64) uint64 {
	if p.end.UnBounded {
		return numRows
	}
	switch p.end.Type {
	case ast.Preceding:
		if p.curRowIdx >= p.end.Num {
			return p.curRowIdx - p.end.Num + 1
		}
		return 0
	case ast.Following:
		offset := p.curRowIdx + p.end.Num
		if offset >= numRows {
			return numRows
		}
		return offset + 1
	case ast.CurrentRow:
		return p.curRowIdx + 1
	}
	return 0
}

func (*rowFrameWindowProcessor) consumeGroupRows(_ sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	return rows, nil
}

func (p *rowFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	numRows := uint64(len(rows))
	var initializedSlidingWindow bool
	var start, end, lastStart, lastEnd, shiftStart, shiftEnd uint64
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		start = p.getStartOffset(numRows)
		end = p.getEndOffset(numRows)
		p.curRowIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					if err := slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row { return rows[u] }, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i]); err != nil {
						return nil, err
					}
				}
				if err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk); err != nil {
					return nil, err
				}
			}
			continue
		}
		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			var err error
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				// ROWS frames move monotonically, so sliding implementations can update
				// the previous partial result by evicting/adding only delta rows.
				err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row { return rows[u] }, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				_, err = windowFunc.UpdatePartialResult(ctx.GetExprCtx().GetEvalCtx(), rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			if err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk); err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (p *rowFrameWindowProcessor) resetPartialResult() { p.curRowIdx = 0 }

type rangeFrameWindowProcessor struct {
	windowFuncs       []aggfuncs.AggFunc
	partialResults    []aggfuncs.PartialResult
	start             *logicalop.FrameBound
	end               *logicalop.FrameBound
	curRowIdx         uint64
	lastStartOffset   uint64
	lastEndOffset     uint64
	orderByCols       []*expression.Column
	expectedCmpResult int64
}

func (p *rangeFrameWindowProcessor) getStartOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	if p.start.UnBounded {
		return 0, nil
	}
	numRows := uint64(len(rows))
	// RANGE frames scan offsets monotonically as the current row advances. Reusing
	// lastStartOffset avoids re-checking rows already known to stay inside the frame.
	for ; p.lastStartOffset < numRows; p.lastStartOffset++ {
		var res int64
		var err error
		for i := range p.orderByCols {
			res, _, err = p.start.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), p.start.CompareCols[i], p.start.CalcFuncs[i], rows[p.lastStartOffset], rows[p.curRowIdx])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		if res != p.expectedCmpResult {
			break
		}
	}
	return p.lastStartOffset, nil
}

func (p *rangeFrameWindowProcessor) getEndOffset(ctx sessionctx.Context, rows []chunk.Row) (uint64, error) {
	numRows := uint64(len(rows))
	if p.end.UnBounded {
		return numRows, nil
	}
	// The end pointer follows the same monotonic property as start, so the processor
	// amortizes RANGE frame boundary lookup across the whole partition.
	for ; p.lastEndOffset < numRows; p.lastEndOffset++ {
		var res int64
		var err error
		for i := range p.orderByCols {
			res, _, err = p.end.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), p.end.CalcFuncs[i], p.end.CompareCols[i], rows[p.curRowIdx], rows[p.lastEndOffset])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		if res == p.expectedCmpResult {
			break
		}
	}
	return p.lastEndOffset, nil
}

func (p *rangeFrameWindowProcessor) appendResult2Chunk(ctx sessionctx.Context, rows []chunk.Row, chk *chunk.Chunk, remained int) ([]chunk.Row, error) {
	var initializedSlidingWindow bool
	var start, end, lastStart, lastEnd, shiftStart, shiftEnd uint64
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		var err error
		start, err = p.getStartOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		end, err = p.getEndOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		p.curRowIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					if err := slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row { return rows[u] }, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i]); err != nil {
						return nil, err
					}
				}
				if err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk); err != nil {
					return nil, err
				}
			}
			continue
		}
		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), func(u uint64) chunk.Row { return rows[u] }, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				if minMaxSlidingWindowAggFunc, ok := windowFunc.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
					minMaxSlidingWindowAggFunc.SetWindowStart(start)
				}
				_, err = windowFunc.UpdatePartialResult(ctx.GetExprCtx().GetEvalCtx(), rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			if err := windowFunc.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), p.partialResults[i], chk); err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (*rangeFrameWindowProcessor) consumeGroupRows(_ sessionctx.Context, rows []chunk.Row) ([]chunk.Row, error) {
	return rows, nil
}

func (p *rangeFrameWindowProcessor) resetPartialResult() {
	p.curRowIdx = 0
	p.lastStartOffset = 0
	p.lastEndOffset = 0
}
