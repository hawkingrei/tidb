// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var (
	addr      = flag.String("addr", "127.0.0.1:2379", "pd address")
	tableName = flag.String("table", "benchdb", "name of the table")
	batchSize = flag.Int("batch", 100, "number of statements in a transaction, used for insert and update-random only")
	blobSize  = flag.Int("blob", 1000, "size of the blob column in the row")
	logLevel  = flag.String("L", "warn", "log level")
	runJobs   = flag.String("run", strings.Join([]string{
		"create",
		"truncate",
		"insert:0_10000",
		"update-random:0_10000:100000",
		"select:0_10000:10",
		"update-range:5000_5100:1000",
		"select:0_10000:10",
		"gc",
		"select:0_10000:10",
	}, "|"), "jobs to run")
)

func main() {
	flag.Parse()
	flag.PrintDefaults()
	err := logutil.InitLogger(logutil.NewLogConfig(*logLevel, logutil.DefaultLogFormat, "", "", logutil.EmptyFileLogConfig, false))
	terror.MustNil(err)
	err = store.Register(config.StoreTypeTiKV, &driver.TiKVDriver{})
	terror.MustNil(err)
	ut := newBenchDB()
	works := strings.Split(*runJobs, "|")
	for _, v := range works {
		work := strings.ToLower(strings.TrimSpace(v))
		name, spec := ut.mustParseWork(work)
		switch name {
		case "create":
			ut.createTable()
		case "truncate":
			ut.truncateTable()
		case "insert":
			ut.insertRows(spec)
		case "update-random", "update_random":
			ut.updateRandomRows(spec)
		case "update-range", "update_range":
			ut.updateRangeRows(spec)
		case "select":
			ut.selectRows(spec)
		case "query":
			ut.query(spec)
		default:
			cLog("Unknown job ", v)
			return
		}
	}
}

type benchDB struct {
	store   tikv.Storage
	session sessionapi.Session
}

func newBenchDB() *benchDB {
	// Create TiKV store and disable GC as we will trigger GC manually.
	store, err := store.New("tikv://" + *addr + "?disableGC=true")
	terror.MustNil(err)
	// maybe close below components, but it's for test anyway.
	ctx := context.Background()
	config.GetGlobalConfig().Store = config.StoreTypeTiKV
	err = ddl.StartOwnerManager(ctx, store)
	terror.MustNil(err)
	_, err = session.BootstrapSession(store)
	terror.MustNil(err)
	se, err := session.CreateSession(store)
	terror.MustNil(err)
	_, err = se.ExecuteInternal(ctx, "use test")
	terror.MustNil(err)

	return &benchDB{
		store:   store.(tikv.Storage),
		session: se,
	}
}

func (ut *benchDB) mustExec(sql string, args ...any) {
	// executeInternal only return one resultSet for this.
	rs, err := ut.session.ExecuteInternal(context.Background(), sql, args...)
	defer func() {
		if rs != nil {
			err = rs.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	if rs != nil {
		ctx := context.Background()
		req := rs.NewChunk(nil)
		for {
			err := rs.Next(ctx, req)
			if err != nil {
				log.Fatal(err.Error())
			}
			if req.NumRows() == 0 {
				break
			}
		}
	}
}

func (ut *benchDB) mustParseWork(work string) (name string, spec string) {
	strs := strings.Split(work, ":")
	if len(strs) == 1 {
		return strs[0], ""
	}
	return strs[0], strings.Join(strs[1:], ":")
}

func (ut *benchDB) mustParseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err.Error())
	}
	return i
}

func (ut *benchDB) mustParseRange(s string) (start, end int) {
	strs := strings.Split(s, "_")
	if len(strs) != 2 {
		log.Fatal("parse range failed", zap.String("invalid range", s))
	}
	startStr, endStr := strs[0], strs[1]
	start = ut.mustParseInt(startStr)
	end = ut.mustParseInt(endStr)
	if start < 0 || end < start {
		log.Fatal("parse range failed", zap.String("invalid range", s))
	}
	return
}

func (ut *benchDB) mustParseSpec(s string) (start, end, count int) {
	strs := strings.Split(s, ":")
	start, end = ut.mustParseRange(strs[0])
	if len(strs) == 1 {
		count = 1
		return
	}
	count = ut.mustParseInt(strs[1])
	return
}

func (ut *benchDB) createTable() {
	cLog("create table")
	createSQL := `CREATE TABLE IF NOT EXISTS %n (
  id bigint(20) NOT NULL,
  name varchar(32) NOT NULL,
  exp bigint(20) NOT NULL DEFAULT '0',
  data blob,
  PRIMARY KEY (id),
  UNIQUE KEY name (name)
)`
	ut.mustExec(createSQL, *tableName)
}

func (ut *benchDB) truncateTable() {
	cLog("truncate table")
	ut.mustExec("truncate table %n", *tableName)
}

func (ut *benchDB) runCountTimes(name string, count int, f func()) {
	var (
		sum, first, last time.Duration
		minv             = time.Minute
		maxv             = time.Nanosecond
	)
	cLogf("%s started", name)
	for range count {
		before := time.Now()
		f()
		dur := time.Since(before)
		if first == 0 {
			first = dur
		}
		last = dur
		if dur < minv {
			minv = dur
		}
		if dur > maxv {
			maxv = dur
		}
		sum += dur
	}
	cLogf("%s done, avg %s, count %d, sum %s, first %s, last %s, max %s, min %s\n\n",
		name, sum/time.Duration(count), count, sum, first, last, maxv, minv)
}

// #nosec G404
func (ut *benchDB) insertRows(spec string) {
	start, end, _ := ut.mustParseSpec(spec)
	loopCount := (end - start + *batchSize - 1) / *batchSize
	id := start
	ut.runCountTimes("insert", loopCount, func() {
		ut.mustExec("begin")
		buf := make([]byte, *blobSize/2)
		for range *batchSize {
			if id == end {
				break
			}
			rand.Read(buf)
			insertQuery := "insert %n (id, name, data) values(%?, %?, %?)"
			ut.mustExec(insertQuery, *tableName, id, id, buf)
			id++
		}
		ut.mustExec("commit")
	})
}

// #nosec G404
func (ut *benchDB) updateRandomRows(spec string) {
	start, end, totalCount := ut.mustParseSpec(spec)
	loopCount := (totalCount + *batchSize - 1) / *batchSize
	var runCount = 0
	ut.runCountTimes("update-random", loopCount, func() {
		ut.mustExec("begin")
		for range *batchSize {
			if runCount == totalCount {
				break
			}
			id := rand.Intn(end-start) + start
			updateQuery := "update %n set exp = exp + 1 where id = %?"
			ut.mustExec(updateQuery, *tableName, id)
			runCount++
		}
		ut.mustExec("commit")
	})
}

func (ut *benchDB) updateRangeRows(spec string) {
	start, end, count := ut.mustParseSpec(spec)
	ut.runCountTimes("update-range", count, func() {
		ut.mustExec("begin")
		updateQuery := "update %n set exp = exp + 1 where id >= %? and id < %?"
		ut.mustExec(updateQuery, *tableName, start, end)
		ut.mustExec("commit")
	})
}

func (ut *benchDB) selectRows(spec string) {
	start, end, count := ut.mustParseSpec(spec)
	ut.runCountTimes("select", count, func() {
		selectQuery := "select * from %n where id >= %? and id < %?"
		ut.mustExec(selectQuery, *tableName, start, end)
	})
}

func (ut *benchDB) query(spec string) {
	strs := strings.Split(spec, ":")
	sql := strs[0]
	count, err := strconv.Atoi(strs[1])
	terror.MustNil(err)
	ut.runCountTimes("query", count, func() {
		ut.mustExec(sql)
	})
}

func cLogf(format string, args ...any) {
	str := fmt.Sprintf(format, args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}

func cLog(args ...any) {
	str := fmt.Sprint(args...)
	fmt.Println("\033[0;32m" + str + "\033[0m\n")
}
