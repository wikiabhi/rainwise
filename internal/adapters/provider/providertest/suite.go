// Package providertest holds the conformance suite every ports.Provider must pass.
package providertest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const pageSize = 2

// Run executes the suite. newProvider returns a fresh provider seeded with at
// least 3 bookmarks, the first listed of which has at least one tag.
func Run(t *testing.T, newProvider func(t *testing.T) ports.Provider) {
	t.Run("PaginationReturnsEverythingOnce", func(t *testing.T) {
		all := listAll(t, newProvider(t))
		if len(all) < 3 {
			t.Fatalf("suite needs at least 3 seeded bookmarks, got %d", len(all))
		}
		seen := map[domain.ID]bool{}
		for _, b := range all {
			if seen[b.ID] {
				t.Errorf("bookmark %s returned twice", b.ID)
			}
			seen[b.ID] = true
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
		ctx := context.Background()
		p := newProvider(t)
		all := listAll(t, p)
		b := all[0]
		target := b.Collection
		for _, o := range all {
			if o.Collection != b.Collection {
				target = o.Collection
			}
		}
		updated, err := p.UpdateBookmark(ctx, b.ID, ports.BookmarkPatch{Collection: &target})
		if err != nil {
			t.Fatal(err)
		}
		if updated.Collection != target {
			t.Errorf("collection = %v, want %v", updated.Collection, target)
		}
		if !slices.Equal(updated.Tags, b.Tags) {
			t.Errorf("tags changed by a move: %v -> %v", b.Tags, updated.Tags)
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

func listAll(t *testing.T, p ports.Provider) []domain.Bookmark {
	t.Helper()
	var all []domain.Bookmark
	token := ""
	for range 1000 {
		page, err := p.ListBookmarks(context.Background(), ports.ListQuery{PageToken: token, Limit: pageSize})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Items...)
		if page.NextToken == "" {
			return all
		}
		token = page.NextToken
	}
	t.Fatal("pagination never ended")
	return nil
}
