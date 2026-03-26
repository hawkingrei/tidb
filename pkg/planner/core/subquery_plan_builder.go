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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
)

type subqueryExprExtractor struct {
	exprs []ast.ExprNode
}

func (e *subqueryExprExtractor) collect(node ast.Node) {
	if node == nil {
		return
	}
	node.Accept(e)
}

// Enter implements Visitor interface.
func (e *subqueryExprExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch subq := n.(type) {
	case *ast.SubqueryExpr:
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.ExistsSubqueryExpr:
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.CompareSubqueryExpr:
		e.collect(subq.L)
		e.exprs = append(e.exprs, subq)
		return n, true
	case *ast.PatternInExpr:
		if subq.Sel == nil {
			return n, false
		}
		e.collect(subq.Expr)
		e.exprs = append(e.exprs, subq)
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (*subqueryExprExtractor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

type subqueryBuildState struct {
	builder                    *PlanBuilder
	restoreOuterScope          bool
	restoreCorrelatedAggMapper bool
	oldCurClause               clauseCode
	oldSubQueryCtx             subQueryCtx
	oldSubQueryHintFlags       uint64
	oldWindowSpecs             map[string]*ast.WindowSpec
	oldCorrelatedAggMapper     map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn
}

func (s *subqueryBuildState) restore() {
	if s.restoreOuterScope {
		s.builder.outerSchemas = s.builder.outerSchemas[:len(s.builder.outerSchemas)-1]
		s.builder.outerNames = s.builder.outerNames[:len(s.builder.outerNames)-1]
		s.builder.currentBlockExpand = s.builder.outerBlockExpand[len(s.builder.outerBlockExpand)-1]
		s.builder.outerBlockExpand = s.builder.outerBlockExpand[:len(s.builder.outerBlockExpand)-1]
	}
	s.builder.curClause = s.oldCurClause
	s.builder.windowSpecs = s.oldWindowSpecs
	s.builder.subQueryCtx = s.oldSubQueryCtx
	s.builder.subQueryHintFlags = s.oldSubQueryHintFlags
	if s.restoreCorrelatedAggMapper {
		s.builder.correlatedAggMapper = s.oldCorrelatedAggMapper
	}
}

func (b *PlanBuilder) enterSubqueryBuild(outerSchema *expression.Schema, outerNames types.NameSlice, subqueryCtx subQueryCtx, resetCorrelatedAggMapper bool) *subqueryBuildState {
	state := &subqueryBuildState{
		builder:                    b,
		restoreOuterScope:          outerSchema != nil,
		restoreCorrelatedAggMapper: resetCorrelatedAggMapper,
		oldCurClause:               b.curClause,
		oldSubQueryCtx:             b.subQueryCtx,
		oldSubQueryHintFlags:       b.subQueryHintFlags,
		oldWindowSpecs:             b.windowSpecs,
		oldCorrelatedAggMapper:     b.correlatedAggMapper,
	}
	if outerSchema != nil {
		b.outerSchemas = append(b.outerSchemas, outerSchema.Clone())
		b.outerNames = append(b.outerNames, outerNames)
		b.outerBlockExpand = append(b.outerBlockExpand, b.currentBlockExpand)
		// Clear outer expand metadata, otherwise the inner block can rewrite expressions against it.
		b.currentBlockExpand = nil
	}
	b.subQueryCtx = subqueryCtx
	b.subQueryHintFlags = 0
	if resetCorrelatedAggMapper {
		b.correlatedAggMapper = make(map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn)
	}
	return state
}

func findColumnNameByUniqueID(p base.LogicalPlan, uniqueID int64) *ast.ColumnName {
	for idx, pCol := range p.Schema().Columns {
		if uniqueID != pCol.UniqueID {
			continue
		}
		pName := p.OutputNames()[idx]
		return &ast.ColumnName{
			Schema: pName.DBName,
			Table:  pName.TblName,
			Name:   pName.ColName,
		}
	}
	// USING/NATURAL JOIN can keep table-qualified outer references only in FullSchema/FullNames.
	// LogicalApply embeds LogicalJoin and can carry the same redundant columns when a
	// LATERAL join sits above a USING/NATURAL join.
	var fullSchema *expression.Schema
	var fullNames types.NameSlice
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		fullSchema, fullNames = x.FullSchema, x.FullNames
	case *logicalop.LogicalApply:
		fullSchema, fullNames = x.FullSchema, x.FullNames
	}
	if fullSchema != nil && len(fullNames) != 0 {
		for idx, pCol := range fullSchema.Columns {
			if uniqueID != pCol.UniqueID {
				continue
			}
			pName := fullNames[idx]
			return &ast.ColumnName{
				Schema: pName.DBName,
				Table:  pName.TblName,
				Name:   pName.ColName,
			}
		}
	}
	// Selection/Projection/Window and similar unary wrappers can sit above the join that keeps
	// redundant USING/NATURAL JOIN columns only in FullSchema/FullNames.
	if len(p.Children()) == 1 {
		return findColumnNameByUniqueID(p.Children()[0], uniqueID)
	}
	return nil
}

// cloneResultSetNodeForAuxiliaryFields creates a syntactic copy by restoring the
// query to SQL text and reparsing it. The auxiliary-field path only needs a
// fresh AST for correlated outer-column discovery, so dropping resolver state is
// acceptable here and keeps the implementation simpler than a deep AST clone.
func cloneResultSetNodeForAuxiliaryFields(ctx base.PlanContext, node ast.ResultSetNode) (ast.ResultSetNode, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := node.Restore(restoreCtx); err != nil {
		return nil, err
	}
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	stmt, err := parser.New().ParseOneStmt(sb.String(), charset, collation)
	if err != nil {
		return nil, err
	}
	resultNode, ok := stmt.(ast.ResultSetNode)
	if !ok {
		return nil, errors.Errorf("unexpected auxiliary subquery type %T", stmt)
	}
	return resultNode, nil
}

// buildSubqueryPlanForAuxiliaryFields builds only the inner subquery plan for correlated outer-column discovery.
// This avoids rewriting deferred window-expression wrappers before the window outputs are materialized.
func (b *PlanBuilder) buildSubqueryPlanForAuxiliaryFields(ctx context.Context, p base.LogicalPlan, expr ast.ExprNode) (base.LogicalPlan, bool, error) {
	var (
		subq *ast.SubqueryExpr
		sCtx subQueryCtx
	)
	switch v := expr.(type) {
	case *ast.SubqueryExpr:
		subq = v
		sCtx = handlingScalarSubquery
	case *ast.ExistsSubqueryExpr:
		q, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			return nil, false, nil
		}
		subq = q
		sCtx = handlingExistsSubquery
	case *ast.CompareSubqueryExpr:
		q, ok := v.R.(*ast.SubqueryExpr)
		if !ok {
			return nil, false, nil
		}
		subq = q
		sCtx = handlingCompareSubquery
	case *ast.PatternInExpr:
		q, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			return nil, false, nil
		}
		subq = q
		sCtx = handlingInSubquery
	default:
		return nil, false, nil
	}
	clonedQuery, err := cloneResultSetNodeForAuxiliaryFields(b.ctx, subq.Query)
	if err != nil {
		return nil, true, err
	}

	outerSchema, outerNames := p.Schema(), p.OutputNames()
	if join, ok := p.(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
		outerSchema = join.FullSchema
		outerNames = join.FullNames
	}

	restore := b.enterSubqueryBuild(outerSchema, outerNames, sCtx, true)
	defer restore.restore()

	np, err := b.buildResultSetNode(ctx, clonedQuery, false)
	if err != nil {
		return nil, true, err
	}
	b.handleHelper.popMap()
	return np, true, nil
}

func (b *PlanBuilder) appendAuxiliaryFieldsForSubqueries(ctx context.Context, p base.LogicalPlan, selectFields []*ast.SelectField, nodes ...ast.Node) ([]*ast.SelectField, error) {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		extractor := &subqueryExprExtractor{}
		node.Accept(extractor)
		for _, expr := range extractor.exprs {
			// Correlated aggregates are handled separately; here we only need the outer columns
			// so subqueries inside deferred window expressions can still resolve against this query block.
			// TODO: Reuse the rewritten expression/plan from the pre-build phase instead of rebuilding it
			// only for correlated outer-column discovery.
			np, handled, err := b.buildSubqueryPlanForAuxiliaryFields(ctx, p, expr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !handled {
				_, np, err = b.rewrite(ctx, expr, p, nil, true)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(np)
			for _, corCol := range correlatedCols {
				colName := findColumnNameByUniqueID(p, corCol.UniqueID)
				if colName == nil {
					continue
				}
				columnNameExpr := &ast.ColumnNameExpr{Name: colName}
				for _, field := range selectFields {
					if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(columnNameExpr.Name) && field.AsName.L == "" {
						// Keep the old behavior: aliased select fields still count as distinct output columns here.
						columnNameExpr = nil
						break
					}
				}
				if columnNameExpr != nil {
					selectFields = append(selectFields, &ast.SelectField{
						Auxiliary: true,
						Expr:      columnNameExpr,
					})
				}
			}
		}
	}
	return selectFields, nil
}
