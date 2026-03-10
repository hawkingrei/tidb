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

package executor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	handletype "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

// https://github.com/pingcap/tidb/issues/45690
func TestGetAnalyzePanicErr(t *testing.T) {
	errMsg := fmt.Sprintf("%s", getAnalyzePanicErr(exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(123)))
	require.NotContains(t, errMsg, `%!(EXTRA`)
}

type noopStatsAnalyze struct{}

func (noopStatsAnalyze) InsertAnalyzeJob(*statistics.AnalyzeJob, string, uint64) error {
	return nil
}

func (noopStatsAnalyze) StartAnalyzeJob(*statistics.AnalyzeJob) {}

func (noopStatsAnalyze) UpdateAnalyzeJobProgress(*statistics.AnalyzeJob, int64) {}

func (noopStatsAnalyze) FinishAnalyzeJob(*statistics.AnalyzeJob, error, statistics.JobType) {}

func (noopStatsAnalyze) DeleteAnalyzeJobs(time.Time) error { return nil }

func (noopStatsAnalyze) CleanupCorruptedAnalyzeJobsOnCurrentInstance(map[uint64]struct{}) error {
	return nil
}

func (noopStatsAnalyze) CleanupCorruptedAnalyzeJobsOnDeadInstances() error { return nil }

func (noopStatsAnalyze) HandleAutoAnalyze() bool { return false }

func (noopStatsAnalyze) CheckAnalyzeVersion(*model.TableInfo, []int64, *int) bool { return false }

func (noopStatsAnalyze) GetPriorityQueueSnapshot() (handletype.PriorityQueueSnapshot, error) {
	return handletype.PriorityQueueSnapshot{}, nil
}

func (noopStatsAnalyze) ClosePriorityQueue() {}

func (noopStatsAnalyze) Close() {}

func TestSendAnalyzeResultDestroyDroppedResult(t *testing.T) {
	statsHandle := &handle.Handle{StatsAnalyze: noopStatsAnalyze{}}
	e := &AnalyzeExec{}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	errExitCtx := context.Background()
	e.errExitCh = make(chan struct{})
	close(e.errExitCh)

	testCases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "ctx done", ctx: canceledCtx},
		{name: "err exit", ctx: errExitCtx},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hist := statistics.NewHistogram(1, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 1, 0)
			lower, upper := types.NewIntDatum(1), types.NewIntDatum(1)
			hist.AppendBucket(&lower, &upper, 1, 1)
			require.Greater(t, hist.Bounds.MemoryUsage(), int64(0))

			result := &statistics.AnalyzeResults{
				Ars: []*statistics.AnalyzeResult{{
					Hist: []*statistics.Histogram{hist},
				}},
			}

			e.sendAnalyzeResult(tc.ctx, statsHandle, make(chan *statistics.AnalyzeResults), result)

			require.Zero(t, hist.Bounds.MemoryUsage())
		})
	}
}
