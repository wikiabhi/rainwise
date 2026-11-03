package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wikiabhi/rainwise/internal/adapters/store/sqlite"
	"github.com/wikiabhi/rainwise/internal/adapters/store/storetest"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func open(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ports.Store {
		return open(t, filepath.Join(t.TempDir(), "r.db"))
	})
}

func TestReopenPersists(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "r.db")
	s, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BookmarkStates().PutSetting(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, path)
	v, ok, err := s2.BookmarkStates().GetSetting(ctx, "k")
	if err != nil || !ok || v != "v" {
		t.Fatalf("got %q %v %v", v, ok, err)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "r.db")
	for i := 0; i < 3; i++ {
		s, err := sqlite.Open(ctx, path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		s.Close()
	}
}

func TestConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "r.db"))
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := domain.ID{Provider: "p", Local: fmt.Sprint(i)}
			for n := 0; n < 10; n++ {
				if err := s.BookmarkStates().PutState(ctx, domain.BookmarkState{ID: id, FetchAttempts: n}); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
