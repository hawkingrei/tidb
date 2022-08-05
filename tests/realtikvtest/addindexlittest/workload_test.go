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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateNonUniqueIndexWorkload(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{3, 6, 9, 12, 15, 18, 21, 24, 27},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	initWorkLoadParameters(ctx)
	testOneColFrame(ctx, colIDs, addIndexLitNonUnique)
}

func TestCreateUniqueIndexWorkload(t *testing.T) {
	var colIDs [][]int = [][]int{
		{4, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{5, 9, 12, 17, 24},
		{8, 10, 14, 20},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	initWorkLoadParameters(ctx)
	testOneColFrame(ctx, colIDs, addIndexLitUnique)
}

func TestCreatePKWorkload(t *testing.T) {
	ctx, err := initTest(t)
	require.NoError(t, err)
	initWorkLoadParameters(ctx)
	testOneIndexFrame(ctx, 0, addIndexLitPK)
}

func TestCreateGenColIndexWorkload(t *testing.T) {
	ctx, err := initTest(t)
	require.NoError(t, err)
	initWorkLoadParameters(ctx)
	testOneIndexFrame(ctx, 29, addIndexLitGenCol)
}

func TestCreateMultiColsIndexWorkload(t *testing.T) {
	var coliIDs [][]int = [][]int{
		{2, 5, 8, 11},
		{1, 4, 7, 10, 13},
		{3, 6, 9, 12, 15},
	}
	var coljIDs [][]int = [][]int{
		{18, 21, 24, 27},
		{16, 19, 22, 25},
		{14, 17, 20, 23, 26},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	initWorkLoadParameters(ctx)
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexLitMultiCols)
}
