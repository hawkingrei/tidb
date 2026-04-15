// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestIndexInfo2Cols(t *testing.T) {
	col0 := &expression.Column{UniqueID: 0, ID: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &expression.Column{UniqueID: 1, ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col2 := &expression.Column{UniqueID: 2, ID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colInfo0 := &model.ColumnInfo{ID: 0, Name: ast.NewCIStr("0")}
	colInfo1 := &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("1")}
	colInfo2 := &model.ColumnInfo{ID: 2, Name: ast.NewCIStr("2")}
	indexCol0, indexCol1, indexCol2 := &model.IndexColumn{Name: ast.NewCIStr("0")}, &model.IndexColumn{Name: ast.NewCIStr("1")}, &model.IndexColumn{Name: ast.NewCIStr("2")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1, indexCol2}}

	cols := []*expression.Column{col0}
	colInfos := []*model.ColumnInfo{colInfo0}
	resCols, lengths := IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 1)
	require.Len(t, lengths, 1)
	require.True(t, resCols[0].EqualColumn(col0))

	cols = []*expression.Column{col1}
	colInfos = []*model.ColumnInfo{colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 0)
	require.Len(t, lengths, 0)

	cols = []*expression.Column{col0, col1}
	colInfos = []*model.ColumnInfo{colInfo0, colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 2)
	require.Len(t, lengths, 2)
	require.True(t, resCols[0].EqualColumn(col0))
	require.True(t, resCols[1].EqualColumn(col1))

	// If col1 has been pruned, the prefix columns should just be [col0]
	cols = []*expression.Column{col0, col2}
	colInfos = []*model.ColumnInfo{colInfo0, colInfo2}
	prefixCols, prefixLens, fullCols, fullLens := IndexInfo2Cols(colInfos, cols, indexInfo)
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.True(t, slices.Equal(resCols, prefixCols))
	require.True(t, slices.Equal(lengths, prefixLens))
	require.Len(t, resCols, 1)
	require.Len(t, lengths, 1)
	require.True(t, resCols[0].EqualColumn(col0))

	// If col1 has been pruned, the full columns should be [col0, nil, col2]
	resCols, lengths = IndexInfo2FullCols(colInfos, cols, indexInfo)
	require.True(t, slices.Equal(resCols, fullCols))
	require.True(t, slices.Equal(lengths, fullLens))
	require.Len(t, resCols, 3)
	require.Len(t, lengths, 3)
	require.True(t, resCols[0].EqualColumn(col0))
	require.Nil(t, resCols[1])
	require.True(t, resCols[2].EqualColumn(col2))

	t.Run("null-rejected-nested-in", func(t *testing.T) {
		ctx := mock.NewContext()
		tbl := buildNullableNullRejectTestTable()
		schema, names, err := expression.TableInfo2SchemaAndNames(ctx.GetExprCtx(), ast.NewCIStr(""), tbl)
		require.NoError(t, err)
		colE := findColumnByName(t, schema, names, "e")
		innerSchema := expression.NewSchema(colE)

		safeIn, err := expression.NewFunction(
			ctx.GetExprCtx(),
			ast.In,
			types.NewFieldType(mysql.TypeLonglong),
			expression.NewOne(),
			colE,
			newIntConstant(2),
		)
		require.NoError(t, err)
		safeExpr, err := expression.NewFunction(ctx.GetExprCtx(), ast.NE, types.NewFieldType(mysql.TypeLonglong), safeIn, expression.NewOne())
		require.NoError(t, err)
		require.True(t, IsNullRejected(ctx, innerSchema, safeExpr, true))

		unsafeListItem, err := expression.NewFunction(
			ctx.GetExprCtx(),
			ast.Ifnull,
			types.NewFieldType(mysql.TypeLonglong),
			colE,
			expression.NewOne(),
		)
		require.NoError(t, err)
		unsafeIn, err := expression.NewFunction(
			ctx.GetExprCtx(),
			ast.In,
			types.NewFieldType(mysql.TypeLonglong),
			expression.NewOne(),
			colE,
			unsafeListItem,
		)
		require.NoError(t, err)
		unsafeExpr, err := expression.NewFunction(ctx.GetExprCtx(), ast.NE, types.NewFieldType(mysql.TypeLonglong), unsafeIn, expression.NewOne())
		require.NoError(t, err)
		require.False(t, IsNullRejected(ctx, innerSchema, unsafeExpr, true))
	})
}

func buildNullableNullRejectTestTable() *model.TableInfo {
	return &model.TableInfo{
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Offset:    0,
				Name:      ast.NewCIStr("e"),
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
				State:     model.StatePublic,
			},
		},
		State: model.StatePublic,
	}
}

func newIntConstant(v int64) *expression.Constant {
	return &expression.Constant{
		Value:   types.NewIntDatum(v),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func findColumnByName(t *testing.T, schema *expression.Schema, names types.NameSlice, colName string) *expression.Column {
	t.Helper()
	for i, name := range names {
		if name.ColName.L == colName {
			return schema.Columns[i]
		}
	}
	t.Fatalf("column %s not found", colName)
	return nil
}
