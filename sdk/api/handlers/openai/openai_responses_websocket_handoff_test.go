package openai

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"
)

// The handoff moves ResponsesWebsocket's per-session locals by address. When
// upstream adds a local to the session prologue, this test fails until the
// variable is either carried (responsesWebsocketHandoffVars) or listed here as
// not session state.
var responsesWebsocketHandoffCarried = map[string]bool{
	"lastRequest":                    true,
	"lastResponseOutput":             true,
	"observedCompaction":             true,
	"lastResponseID":                 true,
	"pendingPrewarmID":               true,
	"lastResponsePendingToolCallIDs": true,
	"pinnedAuthID":                   true,
	"pinnedAuthByProvider":           true,
	"passthroughModelName":           true,
	"upstreamMode":                   true,
	"upstreamWebsocketAuthID":        true,
}

var responsesWebsocketHandoffNotState = map[string]bool{}

func TestResponsesWebsocketHandoffCarriesEverySessionLocal(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "openai_responses_websocket.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "ResponsesWebsocket" {
			body = fn.Body
		}
	}
	if body == nil {
		t.Fatal("ResponsesWebsocket not found")
	}

	// The session prologue runs from the declaration of lastRequest to the
	// handoff bind; closures declared there are behaviour, not state.
	var declared []string
	inPrologue := false
	sawBind := false
	for _, stmt := range body.List {
		names := declaredLocals(stmt)
		if !inPrologue {
			for _, name := range names {
				if name == "lastRequest" {
					inPrologue = true
				}
			}
			if !inPrologue {
				continue
			}
		}
		if isHandoffBind(stmt) {
			sawBind = true
			break
		}
		declared = append(declared, names...)
	}
	if !inPrologue || !sawBind {
		t.Fatalf("session prologue not found (lastRequest=%v bind=%v)", inPrologue, sawBind)
	}
	var missing []string
	seen := map[string]bool{}
	for _, name := range declared {
		seen[name] = true
		if !responsesWebsocketHandoffCarried[name] && !responsesWebsocketHandoffNotState[name] {
			missing = append(missing, name)
		}
	}
	for name := range responsesWebsocketHandoffCarried {
		if !seen[name] {
			missing = append(missing, name+" (carried but no longer declared)")
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("session locals not covered by the handoff: %v", missing)
	}
}

func declaredLocals(stmt ast.Stmt) []string {
	var names []string
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		if s.Tok != token.DEFINE {
			return nil
		}
		for i, lhs := range s.Lhs {
			if i < len(s.Rhs) {
				if _, isFunc := s.Rhs[i].(*ast.FuncLit); isFunc {
					continue
				}
			}
			if ident, ok := lhs.(*ast.Ident); ok && ident.Name != "_" {
				names = append(names, ident.Name)
			}
		}
	case *ast.DeclStmt:
		if gen, ok := s.Decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
			for _, spec := range gen.Specs {
				if value, ok := spec.(*ast.ValueSpec); ok {
					for _, ident := range value.Names {
						names = append(names, ident.Name)
					}
				}
			}
		}
	}
	return names
}

func isHandoffBind(stmt ast.Stmt) bool {
	assign, ok := stmt.(*ast.AssignStmt)
	if !ok || len(assign.Rhs) != 1 {
		return false
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "bindResponsesWebsocketHandoff"
}
