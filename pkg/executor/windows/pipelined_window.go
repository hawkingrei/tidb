// Copyright 2021 PingCAP, Inc.
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

type dataInfo struct {
	chk         *chunk.Chunk
	remaining   uint64
	accumulated uint64
}

// PipelinedWindowExec evaluates a partition incrementally and emits results as
// soon as the current row's frame boundaries become stable.
//
// The executor keeps two views of the same data:
//  1. rows: the logical rows still needed for future frame evaluation.
//  2. data: output chunks that already own the non-window columns and are waiting
//     for window columns to be appended.
//
// This split lets the executor stream rows out while still retaining enough
// history to evaluate ROWS/RANGE frames correctly.
type PipelinedWindowExec struct {
	exec.BaseExecutor
	numWindowFuncs     int
	windowFuncs        []aggfuncs.AggFunc
	slidingWindowFuncs []aggfuncs.SlidingWindowAggFunc
	partialResults     []aggfuncs.PartialResult
	start              *logicalop.FrameBound
	end                *logicalop.FrameBound
	groupChecker       *vecgroupchecker.VecGroupChecker

	childResult *chunk.Chunk
	data        []dataInfo
	dataIdx     int

	done         bool
	accumulated  uint64
	dropped      uint64
	rowToConsume uint64
	newPartition bool

	curRowIdx         uint64
	lastStartRow      uint64
	lastEndRow        uint64
	stagedStartRow    uint64
	stagedEndRow      uint64
	rowStart          uint64
	orderByCols       []*expression.Column
	expectedCmpResult int64

	rows                     []chunk.Row
	rowCnt                   uint64
	whole                    bool
	isRangeFrame             bool
	emptyFrame               bool
	initializedSlidingWindow bool
}

// StreamWindowExec is the executor for stream window functions.
type StreamWindowExec struct {
	*PipelinedWindowExec
}

// Close releases the executor and all child resources.
func (e *PipelinedWindowExec) Close() error { return errors.Trace(e.BaseExecutor.Close()) }

// Open initializes the executor and opens its child executor tree.
func (e *PipelinedWindowExec) Open(ctx context.Context) (err error) {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.OpenSelf()
}

// OpenSelf resets the executor-local state without reopening child executors.
// IndexJoin inner workers use this helper because the child side is already open
// when the specialized executor is created.
func (e *PipelinedWindowExec) OpenSelf() error {
	e.done, e.newPartition, e.whole, e.initializedSlidingWindow = false, false, false, false
	e.dataIdx, e.curRowIdx, e.dropped, e.rowToConsume, e.accumulated = 0, 0, 0, 0, 0
	e.lastStartRow, e.lastEndRow, e.stagedStartRow, e.stagedEndRow, e.rowStart, e.rowCnt = 0, 0, 0, 0, 0, 0
	e.rows, e.data = make([]chunk.Row, 0), make([]dataInfo, 0)
	return nil
}

// firstResultChunkNotReady reports whether the oldest buffered output chunk still
// lacks some window-function results. The queue is FIFO, so we only need to look
// at the first chunk to decide whether more child rows must be consumed.
func (e *PipelinedWindowExec) firstResultChunkNotReady() bool {
	if !e.done && len(e.data) == 0 {
		return true
	}
	return len(e.data) > 0 && (e.data[0].remaining != 0 || e.data[0].accumulated > e.dropped)
}

// Next fills the next output chunk. It keeps consuming child rows until the
// oldest buffered chunk has enough information to finalize its window columns.
func (e *PipelinedWindowExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	chk.Reset()
	for e.firstResultChunkNotReady() {
		var enough bool
		enough, err = e.enoughToProduce(e.Ctx())
		if err != nil {
			return
		}
		if !enough {
			if !e.done && e.rowToConsume == 0 {
				err = e.getRowsInPartition(ctx)
				if err != nil {
					return err
				}
			}
			if e.done || e.newPartition {
				e.finish()
				enough, err = e.enoughToProduce(e.Ctx())
				if err != nil {
					return
				}
				if enough {
					continue
				}
				e.newPartition = false
				e.reset()
				if e.rowToConsume == 0 {
					break
				}
			}
			e.rowCnt += e.rowToConsume
			e.rowToConsume = 0
		}

		if len(e.data) > e.dataIdx && e.data[e.dataIdx].remaining != 0 {
			produced, err := e.produce(e.Ctx(), e.data[e.dataIdx].chk, e.data[e.dataIdx].remaining)
			if err != nil {
				return err
			}
			e.data[e.dataIdx].remaining -= produced
			if e.data[e.dataIdx].remaining == 0 {
				e.dataIdx++
			}
		}
	}
	if len(e.data) > 0 {
		chk.SwapColumns(e.data[0].chk)
		e.data = e.data[1:]
		e.dataIdx--
	}
	return nil
}

func (e *PipelinedWindowExec) getRowsInPartition(ctx context.Context) (err error) {
	e.newPartition = true
	if len(e.rows) == 0 {
		e.newPartition = false
	}

	if e.groupChecker.IsExhausted() {
		var drained, samePartition bool
		drained, err = e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if drained {
			e.done = true
			return nil
		}
		samePartition, err = e.groupChecker.SplitIntoGroups(e.childResult)
		if samePartition {
			e.newPartition = false
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.GetNextGroup()
	// The group checker may split a logical partition across multiple input chunks.
	// rowToConsume tracks how many newly arrived rows can now participate in frame
	// evaluation before we decide whether the partition is complete.
	e.rowToConsume += uint64(end - begin)
	for i := begin; i < end; i++ {
		e.rows = append(e.rows, e.childResult.GetRow(i))
	}
	return
}

func (e *PipelinedWindowExec) fetchChild(ctx context.Context) (eof bool, err error) {
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
	e.accumulated += uint64(numRows)
	e.data = append(e.data, dataInfo{chk: resultChk, remaining: uint64(numRows), accumulated: e.accumulated})
	e.childResult = childResult
	return false, nil
}

func (e *PipelinedWindowExec) copyChk(src, dst *chunk.Chunk) error {
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		if err := dst.MakeRefTo(i, src, col.Index); err != nil {
			return err
		}
	}
	return nil
}

func (e *PipelinedWindowExec) getRow(i uint64) chunk.Row { return e.rows[i-e.rowStart] }
func (e *PipelinedWindowExec) getRows(start, end uint64) []chunk.Row {
	return e.rows[start-e.rowStart : end-e.rowStart]
}
func (e *PipelinedWindowExec) finish() { e.whole = true }

func (e *PipelinedWindowExec) getStart(ctx sessionctx.Context) (uint64, error) {
	if e.start.UnBounded {
		return 0, nil
	}
	if e.isRangeFrame {
		var start uint64
		for start = max(e.lastStartRow, e.stagedStartRow); start < e.rowCnt; start++ {
			var res int64
			var err error
			for i := range e.orderByCols {
				res, _, err = e.start.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), e.start.CompareCols[i], e.start.CalcFuncs[i], e.getRow(start), e.getRow(e.curRowIdx))
				if err != nil {
					return 0, err
				}
				if res != 0 {
					break
				}
			}
			if res != e.expectedCmpResult {
				break
			}
		}
		e.stagedStartRow = start
		return start, nil
	}
	switch e.start.Type {
	case ast.Preceding:
		if e.curRowIdx > e.start.Num {
			return e.curRowIdx - e.start.Num, nil
		}
		return 0, nil
	case ast.Following:
		return e.curRowIdx + e.start.Num, nil
	default:
		return e.curRowIdx, nil
	}
}

func (e *PipelinedWindowExec) getEnd(ctx sessionctx.Context) (uint64, error) {
	if e.end.UnBounded {
		return e.rowCnt, nil
	}
	if e.isRangeFrame {
		var end uint64
		for end = max(e.lastEndRow, e.stagedEndRow); end < e.rowCnt; end++ {
			var res int64
			var err error
			for i := range e.orderByCols {
				res, _, err = e.end.CmpFuncs[i](ctx.GetExprCtx().GetEvalCtx(), e.end.CalcFuncs[i], e.end.CompareCols[i], e.getRow(e.curRowIdx), e.getRow(end))
				if err != nil {
					return 0, err
				}
				if res != 0 {
					break
				}
			}
			if res == e.expectedCmpResult {
				break
			}
		}
		e.stagedEndRow = end
		return end, nil
	}
	switch e.end.Type {
	case ast.Preceding:
		if e.curRowIdx >= e.end.Num {
			return e.curRowIdx - e.end.Num + 1, nil
		}
		return 0, nil
	case ast.Following:
		return e.curRowIdx + e.end.Num + 1, nil
	default:
		return e.curRowIdx + 1, nil
	}
}

func (e *PipelinedWindowExec) produce(ctx sessionctx.Context, chk *chunk.Chunk, remained uint64) (produced uint64, err error) {
	var start, end uint64
	for remained > 0 {
		enough, err := e.enoughToProduce(ctx)
		if err != nil {
			return 0, err
		}
		if !enough {
			break
		}
		start, err = e.getStart(ctx)
		if err != nil {
			return 0, err
		}
		end, err = e.getEnd(ctx)
		if err != nil {
			return 0, err
		}
		if end > e.rowCnt {
			end = e.rowCnt
		}
		if start >= e.rowCnt {
			start = e.rowCnt
		}
		if start >= end {
			for i, wf := range e.windowFuncs {
				if !e.emptyFrame {
					// Empty frames are sticky for consecutive rows with the same frame shape.
					// Reset only on the transition into an empty frame so each function can
					// append its canonical empty-frame result.
					wf.ResetPartialResult(e.partialResults[i])
				}
				if err := wf.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), e.partialResults[i], chk); err != nil {
					return 0, err
				}
			}
			if !e.emptyFrame {
				e.emptyFrame = true
				e.initializedSlidingWindow = false
			}
		} else {
			e.emptyFrame = false
			for i, wf := range e.windowFuncs {
				slidingWindowAggFunc := e.slidingWindowFuncs[i]
				if e.lastStartRow != start || e.lastEndRow != end {
					if slidingWindowAggFunc != nil && e.initializedSlidingWindow {
						// When the frame advances monotonically, Slide reuses the previous
						// partial result and only accounts for the delta between frames.
						err = slidingWindowAggFunc.Slide(ctx.GetExprCtx().GetEvalCtx(), e.getRow, e.lastStartRow, e.lastEndRow, start-e.lastStartRow, end-e.lastEndRow, e.partialResults[i])
					} else {
						if minMaxSlidingWindowAggFunc, ok := wf.(aggfuncs.MaxMinSlidingWindowAggFunc); ok {
							minMaxSlidingWindowAggFunc.SetWindowStart(start)
						}
						// Fall back to rebuilding the frame from scratch when the function
						// does not implement the sliding-window contract.
						wf.ResetPartialResult(e.partialResults[i])
						_, err = wf.UpdatePartialResult(ctx.GetExprCtx().GetEvalCtx(), e.getRows(start, end), e.partialResults[i])
					}
				}
				if err != nil {
					return 0, err
				}
				if err := wf.AppendFinalResult2Chunk(ctx.GetExprCtx().GetEvalCtx(), e.partialResults[i], chk); err != nil {
					return 0, err
				}
			}
			e.initializedSlidingWindow = true
		}
		e.curRowIdx++
		e.lastStartRow, e.lastEndRow = start, end
		produced++
		remained--
	}
	// Any row strictly before min(curRowIdx, last frame start, last frame end) can no
	// longer participate in future frames, so we can release it from the retained row
	// buffer and advance the logical base offset.
	extend := min(e.curRowIdx, e.lastEndRow, e.lastStartRow)
	if extend > e.rowStart {
		numDrop := extend - e.rowStart
		e.dropped += numDrop
		e.rows = e.rows[numDrop:]
		e.rowStart = extend
	}
	return produced, nil
}

func (e *PipelinedWindowExec) enoughToProduce(ctx sessionctx.Context) (bool, error) {
	if e.curRowIdx >= e.rowCnt {
		return false, nil
	}
	if e.whole {
		return true, nil
	}
	start, err := e.getStart(ctx)
	if err != nil {
		return false, err
	}
	end, err := e.getEnd(ctx)
	if err != nil {
		return false, err
	}
	return end < e.rowCnt && start < e.rowCnt, nil
}

// reset prepares the executor for the next partition while preserving any output
// chunks that still need to be returned to the parent executor.
func (e *PipelinedWindowExec) reset() {
	e.lastStartRow = 0
	e.lastEndRow = 0
	e.stagedStartRow = 0
	e.stagedEndRow = 0
	e.emptyFrame = false
	e.curRowIdx = 0
	e.whole = false
	numDrop := e.rowCnt - e.rowStart
	e.dropped += numDrop
	e.rows = e.rows[numDrop:]
	e.rowStart = 0
	e.rowCnt = 0
	e.initializedSlidingWindow = false
	for i, windowFunc := range e.windowFuncs {
		windowFunc.ResetPartialResult(e.partialResults[i])
	}
}
