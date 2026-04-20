package main

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

var analyzer = &analysis.Analyzer{
	Name: "patterncheck",
	Doc:  "reports repo-specific bug-prone patterns",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		filename := pass.Fset.Position(file.Pos()).Filename
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}

		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.AssignStmt:
				if reportsDiscardedWebsocketResponse(pass, value) {
					pass.Reportf(value.Lhs[1].Pos(), "discarding the websocket DialContext HTTP response prevents response-body cleanup on failed handshakes")
				}
			case *ast.FuncDecl:
				if reportsUnstoppableTicker(value) {
					pass.Reportf(value.Name.Pos(), "Start* ticker goroutines should expose a stop function or context to avoid leaks")
				}
			}
			return true
		})
	}
	return nil, nil
}

func reportsDiscardedWebsocketResponse(pass *analysis.Pass, assign *ast.AssignStmt) bool {
	if len(assign.Lhs) < 2 || len(assign.Rhs) != 1 {
		return false
	}
	if !isBlankIdentifier(assign.Lhs[1]) {
		return false
	}

	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel == nil || selector.Sel.Name != "DialContext" {
		return false
	}

	receiverType := pass.TypesInfo.TypeOf(selector.X)
	if receiverType == nil {
		return false
	}
	for {
		pointer, isPointer := receiverType.(*types.Pointer)
		if !isPointer {
			break
		}
		receiverType = pointer.Elem()
	}
	named, ok := receiverType.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "github.com/gorilla/websocket" && named.Obj().Name() == "Dialer"
}

func reportsUnstoppableTicker(function *ast.FuncDecl) bool {
	if function == nil || function.Name == nil || !strings.HasPrefix(function.Name.Name, "Start") {
		return false
	}
	if function.Type == nil || function.Type.Results != nil && len(function.Type.Results.List) > 0 {
		return false
	}
	if function.Body == nil {
		return false
	}

	var reported bool
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if reported {
			return false
		}
		goStatement, ok := node.(*ast.GoStmt)
		if !ok {
			return true
		}
		call := goStatement.Call
		literal, ok := call.Fun.(*ast.FuncLit)
		if !ok || literal.Body == nil {
			return true
		}
		var hasTickerRange bool
		var hasSelect bool
		ast.Inspect(literal.Body, func(inner ast.Node) bool {
			switch innerValue := inner.(type) {
			case *ast.RangeStmt:
				if selector, ok := innerValue.X.(*ast.SelectorExpr); ok && selector.Sel != nil && selector.Sel.Name == "C" {
					hasTickerRange = true
				}
			case *ast.SelectStmt:
				hasSelect = true
			}
			return true
		})
		reported = hasTickerRange && !hasSelect
		return !reported
	})
	return reported
}

func isBlankIdentifier(expr ast.Expr) bool {
	identifier, ok := expr.(*ast.Ident)
	return ok && identifier.Name == "_"
}
