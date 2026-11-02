package hiddenprocess

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func processVariable(statement ast.Stmt) string {
	var name *ast.Ident
	var value ast.Expr
	switch statement := statement.(type) {
	case *ast.AssignStmt:
		if len(statement.Lhs) != 1 || len(statement.Rhs) != 1 {
			return ""
		}
		name, _ = statement.Lhs[0].(*ast.Ident)
		value = statement.Rhs[0]
	case *ast.DeclStmt:
		var declaration, ok = statement.Decl.(*ast.GenDecl)
		if !ok || len(declaration.Specs) != 1 {
			return ""
		}
		var spec, valid = declaration.Specs[0].(*ast.ValueSpec)
		if !valid || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return ""
		}
		name = spec.Names[0]
		value = spec.Values[0]
	default:
		return ""
	}
	var call, ok = value.(*ast.CallExpr)
	if name == nil || !ok || !selectorIs(call.Fun, "exec", "Command", "CommandContext") {
		return ""
	}
	return name.Name
}

func selectorIs(expression ast.Expr, receiver string, names ...string) bool {
	var selector, ok = expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	var identifier, valid = selector.X.(*ast.Ident)
	if !valid || identifier.Name != receiver {
		return false
	}
	for _, name := range names {
		if selector.Sel.Name == name {
			return true
		}
	}
	return false
}

func hidesProcess(statement ast.Stmt, name string) bool {
	var expression, ok = statement.(*ast.ExprStmt)
	if !ok {
		return false
	}
	var call, valid = expression.X.(*ast.CallExpr)
	if !valid || len(call.Args) != 1 || !selectorIs(call.Fun, "hiddenprocess", "HideConsole") {
		return false
	}
	var argument, isIdentifier = call.Args[0].(*ast.Ident)
	return isIdentifier && argument.Name == name
}

func TestReleaseCommandsHideConsolesBeforeUse(t *testing.T) {
	for _, test := range []struct {
		path  string
		count int
	}{
		{path: "tools/fakeampsgate/main.go", count: 2},
		{path: "internal/gofercli/integration_test.go", count: 2},
		{path: "cmd/gofer/gofer_test.go", count: 4},
		{path: "tools/withtoolchain/main.go", count: 1},
		{path: "tools/perfgate/main.go", count: 1},
		{path: "tools/perfreport/main.go", count: 1},
	} {
		t.Run(test.path, func(t *testing.T) {
			var source = filepath.Join("..", "..", filepath.FromSlash(test.path))
			var fileSet = token.NewFileSet()
			var file, err = parser.ParseFile(fileSet, source, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			ast.Inspect(file, func(node ast.Node) bool {
				var block, ok = node.(*ast.BlockStmt)
				if !ok {
					return true
				}
				for index, statement := range block.List {
					var name = processVariable(statement)
					if name == "" {
						continue
					}
					count++
					if index+1 >= len(block.List) || !hidesProcess(block.List[index+1], name) {
						t.Errorf("%s: command %s is not configured to hide its console before use", fileSet.Position(statement.Pos()), name)
					}
				}
				return true
			})
			if count != test.count {
				t.Errorf("found %d command constructions, want %d", count, test.count)
			}
		})
	}
}
