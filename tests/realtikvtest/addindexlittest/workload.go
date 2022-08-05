// Copyright 2022 PingCAP, Inc.
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

package addindexlittest

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	skippedErrorCode = []string{"8028", "9007"}
	wCtx             = workloadContext{}
)

func initWorkLoadParameters(ctx *suiteContext) {
	ctx.HasWorkload = true
	ctx.EndWorkload = false
	ctx.Wl = newWorkloadContext()
}

func newWorkloadContext() workload {
	return &wCtx
}

func initWorkLoadContext(ctx *suiteContext, ID ...int) {
	if len(ID) < 2 {
		return
	}

	wCtx.tableID = ID[0]
	wCtx.tableName = "t" + strconv.Itoa(wCtx.tableID)
	wCtx.colID = wCtx.colID[:0]
	for i := 1; i < len(ID); i++ {
		wCtx.colID = append(wCtx.colID, ID[i])
	}
	// set started insert id to 10000
	wCtx.insertID = 10000
	wCtx.date = "2008-02-02"
}

type workChan struct {
	err      error
	finished bool
}

type worker struct {
	id       int
	loadType int // 0 is insert, 1 update, 2 delete
	wChan    chan *workChan
}

type workloadContext struct {
	tableName string
	tableID   int
	colID     []int
	workerNum int
	wr        []*worker
	insertID  int
	date      string
}

func newWorker(wID int, ty int) *worker {
	wr := worker{
		id:       wID,
		loadType: ty,
		wChan:    make(chan *workChan, 1),
	}
	return &wr
}

func (w *worker) run(ctx *suiteContext) {
	var err error
	ctx.tk.Exec("use addindexlit")
	for {
		if ctx.EndWorkload {
			break
		}
		if w.loadType == 0 {
			// insert workload
			err = insertWroker(ctx)
		} else if w.loadType == 1 {
			// update workload
			err = updateWorker(ctx)
		} else if w.loadType == 1 {
			// delete workload
			err = deleteWorker(ctx)
		}
		if err != nil {
			break
		}
	}
	var wchan workChan
	wchan.err = err
	wchan.finished = true
	w.wChan <- &wchan
}

func (wc *workloadContext) StartWorkload(ctx *suiteContext, ID ...int) (err error) {
	initWorkLoadContext(ctx, ID...)
	wCtx.wr = wCtx.wr[:0]
	for i := 0; i < 3; i++ {
		worker := newWorker(i, i)
		wCtx.wr = append(wCtx.wr, worker)
		go worker.run(ctx)
	}
	return err
}

func (wc *workloadContext) StopWorkload(ctx *suiteContext) (err error) {
	ctx.EndWorkload = true
	count := 3
	for i := 0; i < 3; i++ {
		wChan := <-wCtx.wr[i].wChan
		if wChan.err != nil {
			fmt.Printf("Workload task failed, err: %q", err.Error())
		}
		if wChan.finished {
			count--
		}
		if count == 0 {
			break
		}
	}
	return err
}

func isSkippedError(err error, ctx *suiteContext) bool {
	if err == nil {
		return true
	}
	if ctx.IsPK || ctx.IsUnique {
		pos := strings.Index(err.Error(), "1062")
		if pos > 0 {
			return true
		}
	}
	for _, errCode := range skippedErrorCode {
		pos := strings.Index(err.Error(), errCode)
		if pos >= 0 {
			return true
		}
	}
	return false
}
func insertStr(tableName string, id int, date string) string {
	insStr := "insert into addindexlit." + tableName + "(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28)" +
		" values(" + strconv.Itoa(id) + "," +
		"3, 3, 3, 3, 3, " + strconv.Itoa(id) + ", 3, 3.0, 3.0, 1113.1111, adddate('" + date + "', " + strconv.Itoa(id-10000) + "), '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', '" + "aaaa" + strconv.Itoa(id) + "', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')"
	return insStr
}

func insertWroker(ctx *suiteContext) error {
	insStr := insertStr(wCtx.tableName, wCtx.insertID, wCtx.date)
	_, err := ctx.tk.Exec(insStr)
	if !isSkippedError(err, ctx) {
		fmt.Printf("Insert failed:%q, err:%q\n", insStr, err.Error())
		return err
	}
	wCtx.insertID++

	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func updateStr(ctx *suiteContext, tableName string, colID []int) (uStr string) {
	var updateStr string
	id := rand.Intn(ctx.RowNum + 1)
	for i := 0; i < len(colID); i++ {
		colNewValue := genColval(colID[i])
		if colNewValue == "" {
			return ""
		}
		if i == 0 {
			updateStr = " set c" + strconv.Itoa(colID[i]) + "=" + colNewValue
		} else {
			updateStr = updateStr + ", c" + strconv.Itoa(colID[i]) + "=" + colNewValue
		}
	}
	updateStr = "update addindexlit." + tableName + updateStr + " where c0=" + strconv.Itoa(id)
	return updateStr
}

func updateWorker(ctx *suiteContext) error {
	upStr := updateStr(ctx, wCtx.tableName, wCtx.colID)
	_, err := ctx.tk.Exec(upStr)
	if !isSkippedError(err, ctx) {
		fmt.Printf("update failed:%q, err:%q\n", upStr, err.Error())
		return err
	} 
	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func deleteStr(tableName string, id int) string {
	delStr := "delete from addindexlit." + tableName + " where id =" + strconv.Itoa(id)
	return delStr
}

func deleteWorker(ctx *suiteContext) error {
	id := rand.Intn(math.MaxInt32)
	if id < 10000 {
		id += 10000
	}
	delStr := deleteStr(wCtx.tableName, id)
	_, err := ctx.tk.Exec(delStr)
	if !isSkippedError(err, ctx) {
		fmt.Printf("delete failed:%q, err:%q\n", delStr, err.Error())
		return err
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func genColval(colID int) string {
	switch colID {
	case 1, 2, 3, 4, 5, 7:
		return "c" + strconv.Itoa(colID) + " + 1"
	case 6:
		return "c" + strconv.Itoa(colID) + " - 64"
	case 8, 9, 10:
		return strconv.FormatFloat(rand.Float64(), 10, 16, 32)
	case 11:
		return "adddate(c11, 90)"
	case 12, 13, 14, 15:
		return time.Now().String()
	case 17:
		return strconv.Itoa(time.Now().Year())
	case 19:
		return "c19 + c0"
	case 18, 20, 21, 22, 23, 24, 25, 26, 27:
		return "ABCDEEEF"
	case 28:
		return "json_object('name', 'NanJing', 'population', 2566)"
	default:
		return ""
	}
}
