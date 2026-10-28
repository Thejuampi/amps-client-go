package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Audit coverage is distinct from behavioral equivalence: this only prevents a
// public declaration from disappearing from the function-by-function review.
func TestBehaviorReviewInventoryCoversExportedDeclarations(t *testing.T) {
	var root = filepath.Join("..", "..")
	var report, err = os.ReadFile(filepath.Join(root, "docs", "behavior_parity_inventory.md"))
	if err != nil {
		t.Fatal(err)
	}
	var rows = regexp.MustCompile("(?m)^\\| `([^`]+)` \\|").FindAllSubmatch(report, -1)
	var reviewed = make(map[string]bool, len(rows))
	for _, row := range rows {
		var name = string(row[1])
		if reviewed[name] {
			t.Errorf("duplicate review row: %s", name)
		}
		reviewed[name] = true
	}
	err = filepath.WalkDir(filepath.Join(root, "amps"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "internal" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		var source, parseErr = parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		var packagePath, relErr = filepath.Rel(root, filepath.Dir(path))
		if relErr != nil {
			return relErr
		}
		for _, declaration := range source.Decls {
			var function, ok = declaration.(*ast.FuncDecl)
			if !ok || !function.Name.IsExported() {
				continue
			}
			var name = filepath.ToSlash(packagePath) + "."
			if function.Recv != nil {
				var receiver = function.Recv.List[0].Type
				if pointer, ok := receiver.(*ast.StarExpr); ok {
					receiver = pointer.X
				}
				name += receiver.(*ast.Ident).Name + "."
			}
			name += function.Name.Name
			if !reviewed[name] {
				t.Errorf("missing exported declaration from behavior review: %s", name)
			}
			delete(reviewed, name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range reviewed {
		t.Errorf("review row has no exported declaration: %s", name)
	}
}
