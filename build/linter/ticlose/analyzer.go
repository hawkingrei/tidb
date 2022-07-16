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

package ticlose

import (
	"go/types"

	"github.com/gostaticanalysis/analysisutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/buildssa"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

var Analyzer = &analysis.Analyzer{
	Name: "ticlose",
	Doc:  "check whether to close the sqlexec.RecordSet",
	Requires: []*analysis.Analyzer{
		inspect.Analyzer,
		buildssa.Analyzer,
	},
	Run: run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	funcs := pass.ResultOf[buildssa.Analyzer].(*buildssa.SSA).SrcFuncs
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Fast path: if the package doesn't import database/sql,
	// skip the traversal.
	if !imports(pass.Pkg, "database/sql") {
		return nil, nil
	}

	rowsType := analysisutil.TypeOf(pass, "github.com/pingcap/tidb/util/sqlexec", "sqlexec.RecordSet")
	if rowsType == nil {
		// skip checking
		return nil, nil
	}

	var methods []*types.Func
	if m := analysisutil.MethodOf(rowsType, "Close"); m != nil {
		methods = append(methods, m)
	}
	var errMethod *types.Func
	if m := analysisutil.MethodOf(rowsType, "Err"); m != nil {
		errMethod = m
	}
}

func imports(pkg *types.Package, path string) bool {
	for _, imp := range pkg.Imports() {
		if imp.Path() == path {
			return true
		}
	}
	return false
}
