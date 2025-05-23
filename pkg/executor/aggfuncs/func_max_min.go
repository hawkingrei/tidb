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

package aggfuncs

import (
	"cmp"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// NewDeque inits a new MinMaxDeque
func NewDeque(isMax bool, cmpFunc func(i, j any) int) *MinMaxDeque {
	return &MinMaxDeque{[]Pair{}, isMax, cmpFunc}
}

// MinMaxDeque is an array based double end queue
type MinMaxDeque struct {
	Items   []Pair
	IsMax   bool
	cmpFunc func(i, j any) int
}

// PushBack pushes Idx and Item(wrapped in Pair) to the end of MinMaxDeque
func (d *MinMaxDeque) PushBack(idx uint64, item any) {
	d.Items = append(d.Items, Pair{item, idx})
}

// PopFront pops an Item from the front of MinMaxDeque
func (d *MinMaxDeque) PopFront() error {
	if len(d.Items) <= 0 {
		return errors.New("Pop front when MinMaxDeque is empty")
	}
	d.Items = d.Items[1:]
	return nil
}

// PopBack pops an Item from the end of MinMaxDeque
func (d *MinMaxDeque) PopBack() error {
	i := len(d.Items) - 1
	if i < 0 {
		return errors.New("Pop back when MinMaxDeque is empty")
	}
	d.Items = d.Items[:i]
	return nil
}

// Back returns the element at the end of MinMaxDeque, and whether reached end of deque
func (d *MinMaxDeque) Back() (Pair, bool) {
	i := len(d.Items) - 1
	if i < 0 {
		return Pair{}, true
	}
	return d.Items[i], false
}

// Front returns the element at the front of MinMaxDeque, and whether reached end of deque
func (d *MinMaxDeque) Front() (Pair, bool) {
	if len(d.Items) <= 0 {
		return Pair{}, true
	}
	return d.Items[0], false
}

// IsEmpty returns if MinMaxDeque is empty
func (d *MinMaxDeque) IsEmpty() bool {
	return len(d.Items) == 0
}

// Pair pairs items and their indices in MinMaxDeque
type Pair struct {
	Item any
	Idx  uint64
}

// Reset resets the deque for a MaxMinSlidingWindowAggFunc
func (d *MinMaxDeque) Reset() {
	d.Items = d.Items[:0]
}

// Dequeue pops out element from the front, if element's index is out of boundary, i.e. the leftmost element index
func (d *MinMaxDeque) Dequeue(boundary uint64) error {
	for !d.IsEmpty() {
		frontEle, isEnd := d.Front()
		if isEnd {
			return errors.New("Dequeue empty deque")
		}
		if frontEle.Idx <= boundary {
			err := d.PopFront()
			if err != nil {
				return err
			}
			continue
		}
		return nil
	}
	return nil
}

// Enqueue put Item at the back of queue, while popping any element that is lesser element in queue
func (d *MinMaxDeque) Enqueue(idx uint64, item any) error {
	for !d.IsEmpty() {
		pair, isEnd := d.Back()
		if isEnd {
			return errors.New("Dequeue empty deque")
		}

		cmp := d.cmpFunc(item, pair.Item)
		// 1. if MinMaxDeque aims for finding max and Item is equal or bigger than element at back
		// 2. if MinMaxDeque aims for finding min and Item is equal or smaller than element at back
		if !(cmp >= 0 && d.IsMax || cmp <= 0 && !d.IsMax) {
			break
		}
		err := d.PopBack()
		if err != nil {
			return err
		}
	}
	d.PushBack(idx, item)
	return nil
}

const (
	// DefPartialResult4MaxMinIntSize is the size of partialResult4MaxMinInt
	DefPartialResult4MaxMinIntSize = int64(unsafe.Sizeof(partialResult4MaxMinInt{}))
	// DefPartialResult4MaxMinUintSize is the size of partialResult4MaxMinUint
	DefPartialResult4MaxMinUintSize = int64(unsafe.Sizeof(partialResult4MaxMinUint{}))
	// DefPartialResult4MaxMinDecimalSize is the size of partialResult4MaxMinDecimal
	DefPartialResult4MaxMinDecimalSize = int64(unsafe.Sizeof(partialResult4MaxMinDecimal{}))
	// DefPartialResult4MaxMinFloat32Size is the size of partialResult4MaxMinFloat32
	DefPartialResult4MaxMinFloat32Size = int64(unsafe.Sizeof(partialResult4MaxMinFloat32{}))
	// DefPartialResult4MaxMinFloat64Size is the size of partialResult4MaxMinFloat64
	DefPartialResult4MaxMinFloat64Size = int64(unsafe.Sizeof(partialResult4MaxMinFloat64{}))
	// DefPartialResult4MaxMinTimeSize is the size of partialResult4MaxMinTime
	DefPartialResult4MaxMinTimeSize = int64(unsafe.Sizeof(partialResult4MaxMinTime{}))
	// DefPartialResult4MaxMinDurationSize is the size of partialResult4MaxMinDuration
	DefPartialResult4MaxMinDurationSize = int64(unsafe.Sizeof(partialResult4MaxMinDuration{}))
	// DefPartialResult4MaxMinStringSize is the size of partialResult4MaxMinString
	DefPartialResult4MaxMinStringSize = int64(unsafe.Sizeof(partialResult4MaxMinString{}))
	// DefPartialResult4MaxMinJSONSize is the size of partialResult4MaxMinJSON
	DefPartialResult4MaxMinJSONSize = int64(unsafe.Sizeof(partialResult4MaxMinJSON{}))
	// DefPartialResult4MaxMinVectorFloat32Size is the size of partialResult4MaxMinVectorFloat32
	DefPartialResult4MaxMinVectorFloat32Size = int64(unsafe.Sizeof(partialResult4MaxMinVectorFloat32{}))
	// DefPartialResult4MaxMinEnumSize is the size of partialResult4MaxMinEnum
	DefPartialResult4MaxMinEnumSize = int64(unsafe.Sizeof(partialResult4MaxMinEnum{}))
	// DefPartialResult4MaxMinSetSize is the size of partialResult4MaxMinSet
	DefPartialResult4MaxMinSetSize = int64(unsafe.Sizeof(partialResult4MaxMinSet{}))
	// DefMaxMinDequeSize is the size of maxMinHeap
	DefMaxMinDequeSize = int64(unsafe.Sizeof(MinMaxDeque{}))
)

type partialResult4MaxMinInt struct {
	val int64
	// isNull is used to indicates:
	// 1. whether the partial result is the initialization value which should not be compared during evaluation;
	// 2. whether all the values of arg are all null, if so, we should return null as the default value for MAX/MIN.
	isNull bool
	// deque is used for recording max/mins inside current window with sliding algorithm
	deque *MinMaxDeque
}

type partialResult4MaxMinUint struct {
	val    uint64
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinDecimal struct {
	val    types.MyDecimal
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinFloat32 struct {
	val    float32
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinFloat64 struct {
	val    float64
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinTime struct {
	val    types.Time
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinDuration struct {
	val    types.Duration
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinString struct {
	val    string
	isNull bool
	deque  *MinMaxDeque
}

type partialResult4MaxMinJSON struct {
	val    types.BinaryJSON
	isNull bool
}

type partialResult4MaxMinVectorFloat32 struct {
	val    types.VectorFloat32
	isNull bool
}

type partialResult4MaxMinEnum struct {
	val    types.Enum
	isNull bool
}

type partialResult4MaxMinSet struct {
	val    types.Set
	isNull bool
}

type baseMaxMinAggFunc struct {
	baseAggFunc
	isMax    bool
	collator collate.Collator
}

type maxMin4Int struct {
	baseMaxMinAggFunc
}

func (*maxMin4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinInt)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinIntSize
}

func (*maxMin4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinInt)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Int) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinInt)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Int) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Int) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinInt)(src), (*partialResult4MaxMinInt)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Int) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinInt)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinInt(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Int) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Int) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinInt)(pr)
	success := helper.deserializePartialResult4MaxMinInt(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4IntSliding struct {
	maxMin4Int
	windowInfo
}

func (e *maxMin4IntSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Int.ResetPartialResult(pr)
	(*partialResult4MaxMinInt)(pr).deque.Reset()
}

func (e *maxMin4IntSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Int.AllocPartialResult()
	(*partialResult4MaxMinInt)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(int64), j.(int64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4IntSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinInt)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		// MinMaxDeque needs the absolute position of each element, here i only denotes the relative position in rowsInGroup.
		// To get the absolute position, we need to add offset of e.start, which represents the absolute index of start
		// of window.
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4IntSliding{}

func (e *maxMin4IntSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinInt)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	// check for unsigned underflow
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(int64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Uint struct {
	baseMaxMinAggFunc
}

func (*maxMin4Uint) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinUint)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinUintSize
}

func (*maxMin4Uint) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinUint)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Uint) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinUint)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendUint64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Uint) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinUint)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		uintVal := uint64(input)
		if p.isNull {
			p.val = uintVal
			p.isNull = false
			continue
		}
		if e.isMax && uintVal > p.val || !e.isMax && uintVal < p.val {
			p.val = uintVal
		}
	}
	return 0, nil
}

func (e *maxMin4Uint) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinUint)(src), (*partialResult4MaxMinUint)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Uint) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinUint)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinUint(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Uint) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Uint) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinUint)(pr)
	success := helper.deserializePartialResult4MaxMinUint(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4UintSliding struct {
	maxMin4Uint
	windowInfo
}

func (e *maxMin4UintSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Uint.AllocPartialResult()
	(*partialResult4MaxMinUint)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(uint64), j.(uint64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4UintSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Uint.ResetPartialResult(pr)
	(*partialResult4MaxMinUint)(pr).deque.Reset()
}

func (e *maxMin4UintSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinUint)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, uint64(input))
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4UintSliding{}

func (e *maxMin4UintSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinUint)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalInt(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, uint64(input))
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(uint64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

// maxMin4Float32 gets a float32 input and returns a float32 result.
type maxMin4Float32 struct {
	baseMaxMinAggFunc
}

func (*maxMin4Float32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat32)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinFloat32Size
}

func (*maxMin4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat32)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		f := float32(input)
		if p.isNull {
			p.val = f
			p.isNull = false
			continue
		}
		if e.isMax && f > p.val || !e.isMax && f < p.val {
			p.val = f
		}
	}
	return 0, nil
}

func (e *maxMin4Float32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat32)(src), (*partialResult4MaxMinFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Float32) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinFloat32)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinFloat32(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Float32) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Float32) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinFloat32)(pr)
	success := helper.deserializePartialResult4MaxMinFloat32(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Float32Sliding struct {
	maxMin4Float32
	windowInfo
}

func (e *maxMin4Float32Sliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Float32.AllocPartialResult()
	(*partialResult4MaxMinFloat32)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(float64(i.(float32)), float64(j.(float32)))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4Float32Sliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Float32.ResetPartialResult(pr)
	(*partialResult4MaxMinFloat32)(pr).deque.Reset()
}

func (e *maxMin4Float32Sliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, float32(input))
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4Float32Sliding{}

func (e *maxMin4Float32Sliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat32)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, float32(input))
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float32)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Float64 struct {
	baseMaxMinAggFunc
}

func (*maxMin4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinFloat64)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinFloat64Size
}

func (*maxMin4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinFloat64)(pr)
	p.val = 0
	p.isNull = true
}

func (e *maxMin4Float64) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Float64) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat64)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		if e.isMax && input > p.val || !e.isMax && input < p.val {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Float64) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinFloat64)(src), (*partialResult4MaxMinFloat64)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && p1.val > p2.val || !e.isMax && p1.val < p2.val {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Float64) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinFloat64)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinFloat64(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Float64) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Float64) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinFloat64)(pr)
	success := helper.deserializePartialResult4MaxMinFloat64(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Float64Sliding struct {
	maxMin4Float64
	windowInfo
}

func (e *maxMin4Float64Sliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Float64.AllocPartialResult()
	(*partialResult4MaxMinFloat64)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return cmp.Compare(i.(float64), j.(float64))
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4Float64Sliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Float64.ResetPartialResult(pr)
	(*partialResult4MaxMinFloat64)(pr).deque.Reset()
}

func (e *maxMin4Float64Sliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4Float64Sliding{}

func (e *maxMin4Float64Sliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinFloat64)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalReal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(float64)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Decimal struct {
	baseMaxMinAggFunc
}

func (*maxMin4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDecimal)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinDecimalSize
}

func (*maxMin4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDecimal)(pr)
	p.isNull = true
}

func (e *maxMin4Decimal) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	if e.retTp == nil {
		return errors.New("e.retTp of max or min should not be nil")
	}
	frac := e.retTp.GetDecimal()
	if frac == -1 {
		frac = mysql.MaxDecimalScale
	}
	err := p.val.Round(&p.val, frac, types.ModeHalfUp)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *maxMin4Decimal) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDecimal)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		cmp := input.Compare(&p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			p.val = *input
		}
	}
	return 0, nil
}

func (e *maxMin4Decimal) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDecimal)(src), (*partialResult4MaxMinDecimal)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := (&p1.val).Compare(&p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Decimal) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinDecimal)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinDecimal(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Decimal) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Decimal) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinDecimal)(pr)
	success := helper.deserializePartialResult4MaxMinDecimal(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4DecimalSliding struct {
	maxMin4Decimal
	windowInfo
}

// windowInfo is a struct used to store window related info
type windowInfo struct {
	start uint64
}

// SetWindowStart sets absolute start position of window
func (w *windowInfo) SetWindowStart(start uint64) {
	w.start = start
}

func (e *maxMin4DecimalSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Decimal.AllocPartialResult()
	(*partialResult4MaxMinDecimal)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.MyDecimal)
		dst := j.(types.MyDecimal)
		return src.Compare(&dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4DecimalSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Decimal.ResetPartialResult(pr)
	(*partialResult4MaxMinDecimal)(pr).deque.Reset()
}

func (e *maxMin4DecimalSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDecimal)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, *input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4DecimalSliding{}

func (e *maxMin4DecimalSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDecimal)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalDecimal(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, *input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.MyDecimal)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4String struct {
	baseMaxMinAggFunc
	retTp *types.FieldType
}

func (*maxMin4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinString)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinStringSize
}

func (*maxMin4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinString)(pr)
	p.isNull = true
}

func (e *maxMin4String) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinString)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

func (e *maxMin4String) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	tp := e.args[0].GetType(sctx)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			// The string returned by `EvalString` may be referenced to an underlying buffer,
			// for example ‘Chunk’, which could be reset and reused multiply times.
			// We have to deep copy that string to avoid some potential risks
			// when the content of that underlying buffer changed.
			p.val = stringutil.Copy(input)
			memDelta += int64(len(input))
			p.isNull = false
			continue
		}
		cmp := types.CompareString(input, p.val, tp.GetCollate())
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			oldMem := len(p.val)
			newMem := len(input)
			memDelta += int64(newMem - oldMem)
			p.val = stringutil.Copy(input)
		}
	}
	return memDelta, nil
}

func (e *maxMin4String) MergePartialResult(ctx AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinString)(src), (*partialResult4MaxMinString)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	tp := e.args[0].GetType(ctx)
	cmp := types.CompareString(p1.val, p2.val, tp.GetCollate())
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4String) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinString)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinString(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4String) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4String) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinString)(pr)
	success := helper.deserializePartialResult4MaxMinString(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4StringSliding struct {
	maxMin4String
	windowInfo
	collate string
}

func (e *maxMin4StringSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4String.AllocPartialResult()
	(*partialResult4MaxMinString)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		return types.CompareString(i.(string), j.(string), e.collate)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4StringSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4String.ResetPartialResult(pr)
	(*partialResult4MaxMinString)(pr).deque.Reset()
}

func (e *maxMin4StringSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinString)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalString(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4StringSliding{}

func (e *maxMin4StringSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinString)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalString(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(string)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Time struct {
	baseMaxMinAggFunc
}

func (*maxMin4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinTime)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinTimeSize
}

func (*maxMin4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinTime)(pr)
	p.isNull = true
}

func (e *maxMin4Time) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinTime)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Time) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinTime)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Time) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinTime)(src), (*partialResult4MaxMinTime)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Time) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Time) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Time) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinTime)(pr)
	success := helper.deserializePartialResult4MaxMinTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4TimeSliding struct {
	maxMin4Time
	windowInfo
}

func (e *maxMin4TimeSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Time.AllocPartialResult()
	(*partialResult4MaxMinTime)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.Time)
		dst := j.(types.Time)
		return src.Compare(dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4TimeSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Time.ResetPartialResult(pr)
	(*partialResult4MaxMinTime)(pr).deque.Reset()
}

func (e *maxMin4TimeSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinTime)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalTime(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

func (e *maxMin4TimeSliding) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinTime)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinTime(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4TimeSliding) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4TimeSliding) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinTime)(pr)
	success := helper.deserializePartialResult4MaxMinTime(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

var _ SlidingWindowAggFunc = &maxMin4DurationSliding{}

func (e *maxMin4TimeSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinTime)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalTime(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.Time)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4Duration struct {
	baseMaxMinAggFunc
}

func (*maxMin4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinDuration)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinDurationSize
}

func (*maxMin4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinDuration)(pr)
	p.isNull = true
}

func (e *maxMin4Duration) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinDuration)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Duration) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDuration)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
			p.val = input
		}
	}
	return 0, nil
}

func (e *maxMin4Duration) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinDuration)(src), (*partialResult4MaxMinDuration)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp == 1 || !e.isMax && cmp == -1 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Duration) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinDuration)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinDuration(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Duration) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Duration) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinDuration)(pr)
	success := helper.deserializePartialResult4MaxMinDuration(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4DurationSliding struct {
	maxMin4Duration
	windowInfo
}

func (e *maxMin4DurationSliding) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p, memDelta := e.maxMin4Duration.AllocPartialResult()
	(*partialResult4MaxMinDuration)(p).deque = NewDeque(e.isMax, func(i, j any) int {
		src := i.(types.Duration)
		dst := j.(types.Duration)
		return src.Compare(dst)
	})
	return p, memDelta + DefMaxMinDequeSize
}

func (e *maxMin4DurationSliding) ResetPartialResult(pr PartialResult) {
	e.maxMin4Duration.ResetPartialResult(pr)
	(*partialResult4MaxMinDuration)(pr).deque.Reset()
}

func (e *maxMin4DurationSliding) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinDuration)(pr)
	for i, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDuration(sctx, row)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(uint64(i)+e.start, input)
		if err != nil {
			return 0, err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return 0, nil
}

var _ SlidingWindowAggFunc = &maxMin4DurationSliding{}

func (e *maxMin4DurationSliding) Slide(sctx AggFuncUpdateContext, getRow func(uint64) chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4MaxMinDuration)(pr)
	for i := range shiftEnd {
		input, isNull, err := e.args[0].EvalDuration(sctx, getRow(lastEnd+i))
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		err = p.deque.Enqueue(lastEnd+i, input)
		if err != nil {
			return err
		}
	}
	if lastStart+shiftStart >= 1 {
		err := p.deque.Dequeue(lastStart + shiftStart - 1)
		if err != nil {
			return err
		}
	}
	if val, isEnd := p.deque.Front(); !isEnd {
		p.val = val.Item.(types.Duration)
		p.isNull = false
	} else {
		p.isNull = true
	}
	return nil
}

type maxMin4JSON struct {
	baseMaxMinAggFunc
}

func (*maxMin4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinJSON)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinJSONSize
}

func (*maxMin4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinJSON)(pr)
	p.isNull = true
}

func (e *maxMin4JSON) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinJSON)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

func (e *maxMin4JSON) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinJSON)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalJSON(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Copy()
			memDelta += int64(len(input.Value))
			p.isNull = false
			continue
		}
		cmp := types.CompareBinaryJSON(input, p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			oldMem := len(p.val.Value)
			newMem := len(input.Value)
			memDelta += int64(newMem - oldMem)
			p.val = input.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4JSON) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinJSON)(src), (*partialResult4MaxMinJSON)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := types.CompareBinaryJSON(p1.val, p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return 0, nil
}

func (e *maxMin4JSON) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinJSON)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinJSON(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4JSON) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4JSON) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinJSON)(pr)
	success := helper.deserializePartialResult4MaxMinJSON(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4VectorFloat32 struct {
	baseMaxMinAggFunc
}

func (*maxMin4VectorFloat32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinVectorFloat32)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinVectorFloat32Size
}

func (*maxMin4VectorFloat32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	p.isNull = true
}

func (e *maxMin4VectorFloat32) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendVectorFloat32(e.ordinal, p.val)
	return nil
}

func (e *maxMin4VectorFloat32) UpdatePartialResult(sctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinVectorFloat32)(pr)
	for _, row := range rowsInGroup {
		input, isNull, err := e.args[0].EvalVectorFloat32(sctx, row)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		if p.isNull {
			p.val = input.Clone()
			memDelta += int64(input.EstimatedMemUsage())
			p.isNull = false
			continue
		}
		cmp := input.Compare(p.val)
		if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
			oldMem := p.val.EstimatedMemUsage()
			newMem := input.EstimatedMemUsage()
			memDelta += int64(newMem - oldMem)
			p.val = input.Clone()
		}
	}
	return memDelta, nil
}

func (e *maxMin4VectorFloat32) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinVectorFloat32)(src), (*partialResult4MaxMinVectorFloat32)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	cmp := p1.val.Compare(p2.val)
	if e.isMax && cmp > 0 || !e.isMax && cmp < 0 {
		p2.val = p1.val
		p2.isNull = false
	}
	return 0, nil
}

type maxMin4Enum struct {
	baseMaxMinAggFunc
}

func (*maxMin4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinEnum)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinEnumSize
}

func (*maxMin4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinEnum)(pr)
	p.isNull = true
}

func (e *maxMin4Enum) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinEnum)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Enum) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinEnum)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlEnum().Copy()
			memDelta += int64(len(d.GetMysqlEnum().Name))
			p.isNull = false
			continue
		}
		en := d.GetMysqlEnum()
		if e.isMax && e.collator.Compare(en.Name, p.val.Name) > 0 || !e.isMax && e.collator.Compare(en.Name, p.val.Name) < 0 {
			oldMem := len(p.val.Name)
			newMem := len(en.Name)
			memDelta += int64(newMem - oldMem)
			p.val = en.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Enum) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinEnum)(src), (*partialResult4MaxMinEnum)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) > 0 || !e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Enum) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinEnum)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinEnum(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Enum) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Enum) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinEnum)(pr)
	success := helper.deserializePartialResult4MaxMinEnum(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}

type maxMin4Set struct {
	baseMaxMinAggFunc
}

func (*maxMin4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4MaxMinSet)
	p.isNull = true
	return PartialResult(p), DefPartialResult4MaxMinSetSize
}

func (*maxMin4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4MaxMinSet)(pr)
	p.isNull = true
}

func (e *maxMin4Set) AppendFinalResult2Chunk(_ AggFuncUpdateContext, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4MaxMinSet)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}

func (e *maxMin4Set) UpdatePartialResult(ctx AggFuncUpdateContext, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4MaxMinSet)(pr)
	for _, row := range rowsInGroup {
		d, err := e.args[0].Eval(ctx, row)
		if err != nil {
			return memDelta, err
		}
		if d.IsNull() {
			continue
		}
		if p.isNull {
			p.val = d.GetMysqlSet().Copy()
			memDelta += int64(len(d.GetMysqlSet().Name))
			p.isNull = false
			continue
		}
		s := d.GetMysqlSet()
		if e.isMax && e.collator.Compare(s.Name, p.val.Name) > 0 || !e.isMax && e.collator.Compare(s.Name, p.val.Name) < 0 {
			oldMem := len(p.val.Name)
			newMem := len(s.Name)
			memDelta += int64(newMem - oldMem)
			p.val = s.Copy()
		}
	}
	return memDelta, nil
}

func (e *maxMin4Set) MergePartialResult(_ AggFuncUpdateContext, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4MaxMinSet)(src), (*partialResult4MaxMinSet)(dst)
	if p1.isNull {
		return 0, nil
	}
	if p2.isNull {
		*p2 = *p1
		return 0, nil
	}
	if e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) > 0 || !e.isMax && e.collator.Compare(p1.val.Name, p2.val.Name) < 0 {
		p2.val, p2.isNull = p1.val, false
	}
	return 0, nil
}

func (e *maxMin4Set) SerializePartialResult(partialResult PartialResult, chk *chunk.Chunk, spillHelper *SerializeHelper) {
	pr := (*partialResult4MaxMinSet)(partialResult)
	resBuf := spillHelper.serializePartialResult4MaxMinSet(*pr)
	chk.AppendBytes(e.ordinal, resBuf)
}

func (e *maxMin4Set) DeserializePartialResult(src *chunk.Chunk) ([]PartialResult, int64) {
	return deserializePartialResultCommon(src, e.ordinal, e.deserializeForSpill)
}

func (e *maxMin4Set) deserializeForSpill(helper *deserializeHelper) (PartialResult, int64) {
	pr, memDelta := e.AllocPartialResult()
	result := (*partialResult4MaxMinSet)(pr)
	success := helper.deserializePartialResult4MaxMinSet(result)
	if !success {
		return nil, 0
	}
	return pr, memDelta
}
