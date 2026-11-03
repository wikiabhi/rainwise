// Package providertest holds the conformance suite every ports.Provider must pass.
package providertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// pageSizes are walked and compared. The largest fits a seeded set in one page,
// so it has no page boundary at which to lose an item.
var pageSizes = []int{1, 2, 3, 50}

// Run executes the suite. newProvider returns a fresh provider seeded with at
// least 3 and at most 50 bookmarks, in at least 2 collections, the first listed
// of which has at least one tag.
func Run(t *testing.T, newProvider func(t *testing.T) ports.Provider) {
	t.Run("PaginationReturnsEverythingOnce", func(t *testing.T) {
		if err := CheckPagination(context.Background(), newProvider(t)); err != nil {
			t.Error(err)
		}
	})

	t.Run("AddTagsNeverDropsATag", func(t *testing.T) {
		ctx := context.Background()
		p := newProvider(t)
		b := first(t, p)
		if len(b.Tags) == 0 {
			t.Fatal("suite needs the first bookmark to have a tag")
		}
		add := []string{"conformance-new", strings.ToUpper(b.Tags[0])}
		updated, err := p.UpdateBookmark(ctx, b.ID, ports.BookmarkPatch{AddTags: add})
		if err != nil {
			t.Fatal(err)
		}
		reread, err := p.GetBookmark(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		for name, got := range map[string]domain.Bookmark{"returned": updated, "re-read": reread} {
			for _, want := range b.Tags {
				if !slices.Contains(got.Tags, want) {
					t.Errorf("%s bookmark lost tag %q: %v", name, want, got.Tags)
				}
			}
			if !slices.Contains(got.Tags, "conformance-new") {
				t.Errorf("%s bookmark missing new tag: %v", name, got.Tags)
			}
			if want := len(b.Tags) + 1; len(got.Tags) != want {
				t.Errorf("%s bookmark has %d tags, want %d: %v", name, len(got.Tags), want, got.Tags)
			}
		}
	})

	t.Run("MoveKeepsTags", func(t *testing.T) {
		if err := CheckMove(context.Background(), newProvider(t)); err != nil {
			t.Error(err)
		}
	})

	t.Run("GetMissingWrapsNotFound", func(t *testing.T) {
		p := newProvider(t)
		_, err := p.GetBookmark(context.Background(), missingID(p))
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})

	t.Run("OptionalPortsMatchCapabilities", func(t *testing.T) {
		ctx := context.Background()
		p := newProvider(t)
		caps := p.Capabilities()

		hr, ok := p.(ports.HighlightReader)
		checkPort(t, "Highlights", caps.Highlights, ok, func() error {
			_, err := hr.ListHighlights(ctx, first(t, p).ID)
			return err
		})
		cm, ok := p.(ports.CollectionManager)
		checkPort(t, "CreateCollections", caps.CreateCollections, ok, func() error {
			c, err := cm.CreateCollection(ctx, domain.Collection{Name: "conformance"})
			if err == nil && c.ID.IsZero() {
				t.Error("CreateCollection returned no ID")
			}
			return err
		})
		tl, ok := p.(ports.TagLister)
		checkPort(t, "TagListing", caps.TagListing, ok, func() error {
			_, err := tl.ListTags(ctx)
			return err
		})
		if _, ok := p.(ports.EventNormalizer); caps.NativeWebhooks && !ok {
			t.Error("NativeWebhooks declared but EventNormalizer not implemented")
		}
	})
}

// checkPort requires a declared capability to work. An undeclared one must be
// absent, or refuse with ErrCapabilityUnsupported.
func checkPort(t *testing.T, name string, declared, implemented bool, call func() error) {
	t.Helper()
	switch {
	case declared && !implemented:
		t.Errorf("%s declared but its port is not implemented", name)
	case declared:
		if err := call(); err != nil {
			t.Errorf("%s declared but call failed: %v", name, err)
		}
	case implemented:
		if err := call(); !errors.Is(err, domain.ErrCapabilityUnsupported) {
			t.Errorf("%s not declared, want ErrCapabilityUnsupported, got %v", name, err)
		}
	}
}

func missingID(p ports.Provider) domain.ID {
	return domain.ID{Provider: p.Name(), Local: "999999999"}
}

func first(t *testing.T, p ports.Provider) domain.Bookmark {
	t.Helper()
	page, err := p.ListBookmarks(context.Background(), ports.ListQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) == 0 {
		t.Fatal("suite needs seeded bookmarks")
	}
	return page.Items[0]
}

// CheckPagination walks the listing at several page sizes and requires each walk
// to return the same bookmarks, each exactly once. Comparing them catches items
// lost at page boundaries, which a single walk can't show.
func CheckPagination(ctx context.Context, p ports.Provider) error {
	var want []domain.ID
	for i := len(pageSizes) - 1; i >= 0; i-- {
		items, err := listAll(ctx, p, pageSizes[i])
		if err != nil {
			return fmt.Errorf("page size %d: %w", pageSizes[i], err)
		}
		ids := make([]domain.ID, len(items))
		for j, b := range items {
			ids[j] = b.ID
		}
		sorted := slices.SortedFunc(slices.Values(ids), func(a, b domain.ID) int { return strings.Compare(a.String(), b.String()) })
		if dup := slices.Compact(slices.Clone(sorted)); len(dup) != len(sorted) {
			return fmt.Errorf("page size %d returned a bookmark twice: %v", pageSizes[i], ids)
		}
		if want == nil {
			if len(sorted) < 3 {
				return fmt.Errorf("suite needs at least 3 seeded bookmarks, got %d", len(sorted))
			}
			want = sorted
		} else if !slices.Equal(sorted, want) {
			return fmt.Errorf("page size %d returned %v, want %v", pageSizes[i], sorted, want)
		}
	}
	return nil
}

// CheckMove moves a tagged bookmark to a different collection and requires the
// move to land and its tags to survive, in both the response and a re-read.
func CheckMove(ctx context.Context, p ports.Provider) error {
	all, err := listAll(ctx, p, pageSizes[len(pageSizes)-1])
	if err != nil {
		return err
	}
	if len(all) == 0 || len(all[0].Tags) == 0 {
		return errors.New("suite needs the first bookmark to have a tag")
	}
	b := all[0]
	idx := slices.IndexFunc(all, func(o domain.Bookmark) bool { return o.Collection != b.Collection })
	if idx < 0 {
		return errors.New("suite needs seeded bookmarks in at least 2 collections")
	}
	target := all[idx].Collection
	updated, err := p.UpdateBookmark(ctx, b.ID, ports.BookmarkPatch{Collection: &target})
	if err != nil {
		return err
	}
	reread, err := p.GetBookmark(ctx, b.ID)
	if err != nil {
		return err
	}
	for name, got := range map[string]domain.Bookmark{"returned": updated, "re-read": reread} {
		if got.Collection != target {
			return fmt.Errorf("%s bookmark in %v, want %v", name, got.Collection, target)
		}
		if !slices.Equal(got.Tags, b.Tags) {
			return fmt.Errorf("%s bookmark's tags changed by a move: %v -> %v", name, b.Tags, got.Tags)
		}
	}
	return nil
}

func listAll(ctx context.Context, p ports.Provider, size int) ([]domain.Bookmark, error) {
	var all []domain.Bookmark
	token := ""
	for range 1000 {
		page, err := p.ListBookmarks(ctx, ports.ListQuery{PageToken: token, Limit: size})
		if err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextToken == "" {
			return all, nil
		}
		token = page.NextToken
	}
	return nil, errors.New("pagination never ended")
}
