package fake_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/provider/providertest"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func id(n int) domain.ID { return domain.ID{Provider: "fake", Local: fmt.Sprint(n)} }

func seeded() *fake.Provider {
	p := fake.New()
	for i := range 5 {
		col := domain.ID{Provider: "fake", Local: fmt.Sprintf("col-%d", i%2)}
		p.Add(domain.Bookmark{ID: id(i), URL: fmt.Sprintf("https://example.test/%d", i), Tags: []string{"Keep", "other"}, Collection: col})
	}
	return p
}

func TestConformance(t *testing.T) {
	providertest.Run(t, func(t *testing.T) ports.Provider { return seeded() })
}

func TestConformanceWithoutOptionalCapabilities(t *testing.T) {
	providertest.Run(t, func(t *testing.T) ports.Provider {
		p := seeded()
		p.SetCapabilities(ports.ProviderCapabilities{})
		return p
	})
}

func TestWritesRecorded(t *testing.T) {
	p := seeded()
	target := domain.ID{Provider: "fake", Local: "col"}
	patch := ports.BookmarkPatch{Collection: &target, AddTags: []string{"new"}}
	got, err := p.UpdateBookmark(context.Background(), id(1), patch)
	if err != nil {
		t.Fatal(err)
	}
	if got.Collection != target || len(got.Tags) != 3 {
		t.Fatalf("after update: %+v", got)
	}
	w := p.Writes()
	if len(w) != 1 || w[0].ID != id(1) || w[0].Patch.AddTags[0] != "new" {
		t.Fatalf("Writes() = %+v", w)
	}
}

func TestFailNextIsOneShot(t *testing.T) {
	p := seeded()
	boom := errors.New("boom")
	p.FailNext(boom)
	if _, err := p.GetBookmark(context.Background(), id(1)); !errors.Is(err, boom) {
		t.Fatalf("first call err = %v", err)
	}
	if _, err := p.GetBookmark(context.Background(), id(1)); err != nil {
		t.Fatalf("second call err = %v", err)
	}
}

func TestFailNextFailsWriteWithoutRecording(t *testing.T) {
	p := seeded()
	p.FailNext(domain.ErrNotFound)
	if _, err := p.UpdateBookmark(context.Background(), id(1), ports.BookmarkPatch{AddTags: []string{"x"}}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if len(p.Writes()) != 0 {
		t.Fatalf("failed write recorded: %+v", p.Writes())
	}
}

func TestOnGetChangesStateBeforeTheRead(t *testing.T) {
	p := seeded()
	moved := domain.ID{Provider: "fake", Local: "elsewhere"}
	p.OnGet(func(b domain.ID) { p.SetCollection(b, moved) })
	got, err := p.GetBookmark(context.Background(), id(1))
	if err != nil {
		t.Fatal(err)
	}
	if got.Collection != moved {
		t.Fatalf("collection = %v", got.Collection)
	}
}

func TestReturnedBookmarksDoNotAliasState(t *testing.T) {
	p := seeded()
	got, _ := p.GetBookmark(context.Background(), id(1))
	got.Tags[0] = "changed"
	again, _ := p.GetBookmark(context.Background(), id(1))
	if again.Tags[0] != "Keep" {
		t.Fatalf("state aliased: %v", again.Tags)
	}
}

// dropsAtBoundary loses the first item of every page after the first, while the
// pages it does return have no duplicates.
type dropsAtBoundary struct{ *fake.Provider }

func (d dropsAtBoundary) ListBookmarks(ctx context.Context, q ports.ListQuery) (domain.Page[domain.Bookmark], error) {
	page, err := d.Provider.ListBookmarks(ctx, q)
	if err == nil && q.PageToken != "" && len(page.Items) > 0 {
		page.Items = page.Items[1:]
	}
	return page, err
}

func TestCheckPaginationCatchesItemsLostAtBoundaries(t *testing.T) {
	if err := providertest.CheckPagination(context.Background(), seeded()); err != nil {
		t.Fatalf("good provider: %v", err)
	}
	if err := providertest.CheckPagination(context.Background(), dropsAtBoundary{seeded()}); err == nil {
		t.Fatal("a provider losing items at page boundaries passed")
	}
}

type moveIgnored struct{ *fake.Provider }

func (m moveIgnored) UpdateBookmark(ctx context.Context, i domain.ID, p ports.BookmarkPatch) (domain.Bookmark, error) {
	p.Collection = nil
	return m.Provider.UpdateBookmark(ctx, i, p)
}

type moveDropsTags struct{ *fake.Provider }

func (m moveDropsTags) UpdateBookmark(ctx context.Context, i domain.ID, p ports.BookmarkPatch) (domain.Bookmark, error) {
	p.SetTags = []string{}
	return m.Provider.UpdateBookmark(ctx, i, p)
}

func TestCheckMoveCatchesBrokenMoves(t *testing.T) {
	ctx := context.Background()
	if err := providertest.CheckMove(ctx, seeded()); err != nil {
		t.Fatalf("good provider: %v", err)
	}
	if err := providertest.CheckMove(ctx, moveIgnored{seeded()}); err == nil {
		t.Error("a provider ignoring moves passed")
	}
	if err := providertest.CheckMove(ctx, moveDropsTags{seeded()}); err == nil {
		t.Error("a provider dropping tags on a move passed")
	}
	same := fake.New()
	for i := range 3 {
		same.Add(domain.Bookmark{ID: id(i), Tags: []string{"t"}})
	}
	if err := providertest.CheckMove(ctx, same); err == nil {
		t.Error("a seed with one collection made the check a no-op")
	}
}
