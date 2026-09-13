package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/nikitasavinov/flink-tui"

func TestUIRootIsOnlyTheStableFacade(t *testing.T) {
	uiRoot := filepath.Join(repositoryRoot(t), "internal", "ui")
	entries, err := os.ReadDir(uiRoot)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"doc.go": true, "ui.go": true}
	unexpected := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		if !allowed[entry.Name()] {
			unexpected = append(unexpected, entry.Name())
		}
	}
	if len(unexpected) > 0 {
		slices.Sort(unexpected)
		t.Fatalf("internal/ui is an API facade; move implementation into an owned package: %s", strings.Join(unexpected, ", "))
	}
}

func TestUIFeaturePackagesDoNotDependOnCoordinator(t *testing.T) {
	uiRoot := filepath.Join(repositoryRoot(t), "internal", "ui")
	forEachGoImport(t, uiRoot, func(source, imported string) {
		directory := filepath.Dir(source)
		if directory == uiRoot || directory == filepath.Join(uiRoot, "coordinator") {
			return
		}
		if imported == modulePath+"/internal/ui" || imported == modulePath+"/internal/ui/coordinator" {
			relative, err := filepath.Rel(repositoryRoot(t), source)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("%s imports the coordinator layer %q", relative, imported)
		}
	})
}

func TestSharedUIPrimitivesHaveNoApplicationDependencies(t *testing.T) {
	sharedRoot := filepath.Join(repositoryRoot(t), "internal", "ui", "shared")
	forEachGoImport(t, sharedRoot, func(source, imported string) {
		if !strings.HasPrefix(imported, modulePath+"/internal/") {
			return
		}
		relative, err := filepath.Rel(repositoryRoot(t), source)
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("%s is shared terminal code and cannot import application package %q", relative, imported)
	})
}

func TestDomainPackagesDoNotDependOnUI(t *testing.T) {
	for _, name := range []string{"flink", "graph"} {
		root := filepath.Join(repositoryRoot(t), "internal", name)
		forEachGoImport(t, root, func(source, imported string) {
			if imported != modulePath+"/internal/ui" && !strings.HasPrefix(imported, modulePath+"/internal/ui/") {
				return
			}
			relative, err := filepath.Rel(repositoryRoot(t), source)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("%s is domain code and cannot import UI package %q", relative, imported)
		})
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate architecture test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func forEachGoImport(t *testing.T, root string, visit func(source, imported string)) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, specification := range parsed.Imports {
			imported, unquoteErr := strconv.Unquote(specification.Path.Value)
			if unquoteErr != nil {
				return unquoteErr
			}
			visit(path, imported)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
