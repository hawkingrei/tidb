// Copyright 2023 PingCAP, Inc.
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

package handle

import (
	"time"

	"github.com/pingcap/tidb/util"
	"go.uber.org/atomic"
)

const CacheItemTTL = 10 * time.Second

type CacheItem struct {
	item    *mapCache
	StartTs time.Time
}

type CacheItemGC struct {
	gcItem chan CacheItem
	exitCh chan struct{}
	wg     util.WaitGroupWrapper
	cnt    atomic.Uint64
}

func newCacheItemGC() *CacheItemGC {
	return &CacheItemGC{
		gcItem: make(chan CacheItem, 1000),
		exitCh: make(chan struct{}),
	}
}

func (c *CacheItemGC) AddCacheItem(item *mapCache) {
	c.gcItem <- CacheItem{
		item:    item,
		StartTs: time.Now(),
	}
	c.cnt.Add(1)
}

func (c *CacheItemGC) Start() {
	c.wg.Run(func() {
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.exitCh:
				return
			case <-ticker.C:
				c.gc()
			}
		}
	})
}

func (c *CacheItemGC) Stop() {
	close(c.exitCh)
	c.wg.Wait()
}

func (c *CacheItemGC) gc() {
	cnt := c.cnt.Load()
	select {
	case <-c.exitCh:
		return
	case item := <-c.gcItem:
		if time.Since(item.StartTs) > CacheItemTTL {
			item.item.Release()
		} else {
			c.gcItem <- item
		}
		cnt--
		if cnt == 0 {
			return
		}
	default:
	}
}
