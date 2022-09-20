package linter

import (
	"github.com/pingcap/tidb/build/linter/allrevive"
	"github.com/pingcap/tidb/build/linter/asciicheck"
	"github.com/pingcap/tidb/build/linter/bodyclose"
	"github.com/pingcap/tidb/build/linter/noloopclosure"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/asmdecl"
	"golang.org/x/tools/go/analysis/passes/assign"
	"golang.org/x/tools/go/analysis/passes/atomic"
	"golang.org/x/tools/go/analysis/passes/bools"
	"golang.org/x/tools/go/analysis/passes/buildtag"
	"golang.org/x/tools/go/analysis/passes/cgocall"
	"golang.org/x/tools/go/analysis/passes/composite"
	"golang.org/x/tools/go/analysis/passes/copylock"
	"golang.org/x/tools/go/analysis/passes/errorsas"
	"golang.org/x/tools/go/analysis/passes/httpresponse"
	"golang.org/x/tools/go/analysis/passes/lostcancel"
	"golang.org/x/tools/go/analysis/passes/nilfunc"
	"golang.org/x/tools/go/analysis/passes/printf"
	"golang.org/x/tools/go/analysis/passes/shadow"
	"golang.org/x/tools/go/analysis/passes/shift"
	"golang.org/x/tools/go/analysis/passes/stdmethods"
	"golang.org/x/tools/go/analysis/passes/structtag"
	"golang.org/x/tools/go/analysis/passes/tests"
	"golang.org/x/tools/go/analysis/passes/unmarshal"
	"golang.org/x/tools/go/analysis/passes/unreachable"
	"golang.org/x/tools/go/analysis/passes/unsafeptr"
	"golang.org/x/tools/go/analysis/passes/unusedresult"
	"honnef.co/go/tools/analysis/lint"
	"honnef.co/go/tools/quickfix"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/stylecheck"
	"honnef.co/go/tools/unused"
)

func getStaticLinter() map[string]*analysis.Analyzer {
	resMap := make(map[string]*analysis.Analyzer)

	for _, analyzers := range [][]*lint.Analyzer{
		quickfix.Analyzers,
		simple.Analyzers,
		staticcheck.Analyzers,
		stylecheck.Analyzers,
		{unused.Analyzer},
	} {
		for _, a := range analyzers {
			resMap[a.Analyzer.Name] = a.Analyzer
		}
	}

	return resMap
}

func Makelint() (as []*analysis.Analyzer) {
	sc := getStaticLinter()
	// First-party analyzers:
	as = append(as,
		allrevive.Analyzer,
		asciicheck.Analyzer,
		bodyclose.Analyzer,
		noloopclosure.Analyzer,
	)

	as = append(as,
		sc["S1000"],
		sc["S1001"],
		sc["S1002"],
		sc["S1003"],
		sc["S1004"],
		sc["S1005"],
		sc["S1006"],
		sc["S1007"],
		sc["S1008"],
		sc["S1009"],
		sc["S1010"],
		sc["S1011"],
		sc["S1012"],
		sc["S1016"],
		sc["S1017"],
		sc["S1018"],
		sc["S1019"],
		sc["S1020"],
		sc["S1021"],
		sc["S1023"],
		sc["S1024"],
		sc["S1025"],
		sc["S1028"],
		sc["S1029"],
		sc["S1030"],
		sc["S1031"],
		sc["S1032"],
		sc["S1033"],
		sc["S1034"],
		sc["S1035"],
		sc["S1036"],
		sc["S1037"],
		sc["S1038"],
		sc["S1039"],
		sc["S1040"],
		sc["SA1019"],
		sc["SA2000"],
		sc["SA2001"],
		sc["SA2003"],
		sc["SA3000"],
		sc["SA3001"],
		sc["SA4009"],
		sc["SA5000"],
		sc["SA5001"],
		sc["SA5002"],
		sc["SA5003"],
		sc["SA5004"],
		sc["SA5005"],
		sc["SA5007"],
		sc["SA5008"],
		sc["SA5009"],
		sc["SA5010"],
		sc["SA5012"],
		sc["SA6000"],
		sc["SA6001"],
		sc["SA6005"],
		sc["U1000"],
	)

	// Standard go vet analyzers:
	as = append(as,
		asmdecl.Analyzer,
		assign.Analyzer,
		atomic.Analyzer,
		bools.Analyzer,
		buildtag.Analyzer,
		cgocall.Analyzer,
		composite.Analyzer,
		copylock.Analyzer,
		errorsas.Analyzer,
		httpresponse.Analyzer,
		lostcancel.Analyzer,
		nilfunc.Analyzer,
		printf.Analyzer,
		shift.Analyzer,
		stdmethods.Analyzer,
		structtag.Analyzer,
		tests.Analyzer,
		unmarshal.Analyzer,
		unreachable.Analyzer,
		unsafeptr.Analyzer,
		unusedresult.Analyzer,
	)

	// Additional analyzers:
	as = append(as,
		shadow.Analyzer,
	)
	return as
}
