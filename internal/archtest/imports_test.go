package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/wikiabhi/rainwise"

var vendors = []string{"raindrop", "openrouter", "jev", "cloudflare"}

func stdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func allowed(root, path string) bool {
	if stdlib(path) {
		return true
	}
	switch path {
	case module + "/internal/domain":
		return true
	case module + "/internal/ports":
		return root != "domain"
	}
	if root == "core" {
		return strings.HasPrefix(path, module+"/internal/core")
	}
	return false
}

func TestCoreImports(t *testing.T) {
	for _, root := range []string{"core", "domain", "ports"} {
		dir := filepath.Join("..", root)
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				return err
			}
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if !allowed(root, path) {
					t.Errorf("%s: disallowed import %q", p, path)
				}
			}
			check := func(s string) {
				l := strings.ToLower(s)
				for _, v := range vendors {
					if strings.Contains(l, v) {
						t.Errorf("%s: vendor name %q in %q", p, v, s)
					}
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.Ident:
					check(x.Name)
				case *ast.BasicLit:
					if x.Kind == token.STRING {
						check(x.Value)
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
