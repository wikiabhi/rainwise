package pipeline

import (
	"slices"
	"testing"

	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
)

var (
	idTech   = domain.ID{Provider: "p", Local: "1"}
	idReview = domain.ID{Provider: "p", Local: "9"}
	idOther  = domain.ID{Provider: "p", Local: "5"}
)

func baseInput() ValidateInput {
	tax := domain.Taxonomy{
		Mode: domain.ModeFixed,
		Collections: []domain.TaxonomyCollection{
			{Key: "tech", Name: "Tech"},
			{Key: "needs-review", Name: "Needs Review", Role: "review"},
		},
		Rules: domain.TaxonomyRules{MinTags: 2, MaxTags: 5, MinConfidence: 0.6, AllowNewTags: true},
		Dynamic: domain.DynamicSettings{
			MaxCollections: 5, AutoCreateMinConfidence: 0.85,
		},
	}
	return ValidateInput{
		Decision: domain.Decision{
			Collection: domain.CollectionChoice{Key: "tech"},
			Confidence: 0.9,
		},
		Bookmark:       domain.Bookmark{Collection: idOther},
		Taxonomy:       tax,
		Vocab:          taxonomy.NewRunVocab(nil, nil, true),
		WantCollection: true,
		Resolved:       map[string]domain.ID{"tech": idTech, "needs-review": idReview},
	}
}

func tags(names ...string) []domain.TagSuggestion {
	var out []domain.TagSuggestion
	for _, n := range names {
		out = append(out, domain.TagSuggestion{Name: n})
	}
	return out
}

func TestValidateCollection(t *testing.T) {
	cases := []struct {
		name       string
		mod        func(*ValidateInput)
		wantTarget *domain.ID
		wantReview bool
		wantReason bool
	}{
		{"allowed", func(in *ValidateInput) {}, &idTech, false, false},
		{"key not allowed", func(in *ValidateInput) { in.Decision.Collection.Key = "bogus" }, nil, true, true},
		{"name different case", func(in *ValidateInput) { in.Decision.Collection.Key = "TECH" }, &idTech, false, false},
		{"display name", func(in *ValidateInput) {
			in.Taxonomy.Collections[0].Key = "tech-stuff"
			in.Resolved["tech-stuff"] = idTech
			in.Decision.Collection.Key = "tEcH"
		}, &idTech, false, false},
		{"review chosen", func(in *ValidateInput) { in.Decision.Collection.Key = "needs-review" }, nil, true, false},
		{"low confidence unrelated", func(in *ValidateInput) { in.Decision.Confidence = 0.5 }, nil, true, true},
		{"low confidence in allowed", func(in *ValidateInput) {
			in.Decision.Confidence = 0.5
			in.Bookmark.Collection = idTech
		}, nil, false, true},
		{"want collection false", func(in *ValidateInput) {
			in.WantCollection = false
			in.Decision.Confidence = 0.1
		}, nil, false, false},
		{"already in target", func(in *ValidateInput) { in.Bookmark.Collection = idTech }, nil, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := baseInput()
			in.Taxonomy.Rules.MinTags = 0
			c.mod(&in)
			v := Validate(in)
			if (v.Target == nil) != (c.wantTarget == nil) || (v.Target != nil && *v.Target != *c.wantTarget) {
				t.Errorf("Target = %v; want %v", v.Target, c.wantTarget)
			}
			if v.ToReview != c.wantReview {
				t.Errorf("ToReview = %v; want %v", v.ToReview, c.wantReview)
			}
			if (len(v.Reasons) > 0) != c.wantReason {
				t.Errorf("Reasons = %v; wantReason %v", v.Reasons, c.wantReason)
			}
		})
	}
}

func TestValidateTags(t *testing.T) {
	t.Run("room", func(t *testing.T) {
		in := baseInput()
		in.Bookmark.Tags = []string{"a", "b", "c"}
		in.Decision.Tags = tags("w", "x", "y", "z")
		v := Validate(in)
		if !slices.Equal(v.AddTags, []string{"w", "x"}) {
			t.Errorf("AddTags = %v", v.AddTags)
		}
	})
	for _, n := range []int{5, 7} {
		in := baseInput()
		for i := 0; i < n; i++ {
			in.Bookmark.Tags = append(in.Bookmark.Tags, string(rune('a'+i))+"tag")
		}
		in.Decision.Tags = tags("new1", "new2")
		if v := Validate(in); len(v.AddTags) != 0 {
			t.Errorf("%d existing: AddTags = %v", n, v.AddTags)
		}
	}
	t.Run("existing different case", func(t *testing.T) {
		in := baseInput()
		in.Bookmark.Tags = []string{"Go"}
		in.Decision.Tags = tags("go", "rust")
		v := Validate(in)
		if !slices.Equal(v.AddTags, []string{"rust"}) {
			t.Errorf("AddTags = %v", v.AddTags)
		}
	})
	t.Run("no new tags", func(t *testing.T) {
		in := baseInput()
		in.Vocab = taxonomy.NewRunVocab([]string{"rust"}, nil, false)
		in.Decision.Tags = tags("rust", "zig")
		v := Validate(in)
		if !slices.Equal(v.AddTags, []string{"rust"}) || len(v.NewTags) != 0 {
			t.Errorf("AddTags = %v NewTags = %v", v.AddTags, v.NewTags)
		}
	})
	t.Run("new tags reported", func(t *testing.T) {
		in := baseInput()
		in.Decision.Tags = tags("Zig", "#Rust")
		v := Validate(in)
		if !slices.Equal(v.NewTags, []string{"zig", "rust"}) {
			t.Errorf("NewTags = %v", v.NewTags)
		}
	})
	t.Run("shortfall", func(t *testing.T) {
		in := baseInput()
		in.Decision.Tags = tags("only")
		v := Validate(in)
		if len(v.AddTags) != 1 || len(v.Reasons) == 0 {
			t.Errorf("AddTags = %v Reasons = %v", v.AddTags, v.Reasons)
		}
	})
}

func TestValidateProposal(t *testing.T) {
	prop := func(in *ValidateInput) {
		in.Decision.Collection = domain.CollectionChoice{ProposedName: "Cooking"}
		in.Decision.Confidence = 0.9
		in.Taxonomy.Dynamic.AutoCreate = true
		in.CanCreateCollections = true
	}
	t.Run("auto create", func(t *testing.T) {
		in := baseInput()
		prop(&in)
		v := Validate(in)
		if v.ProposedCollection == nil || v.ProposedCollection.Name != "Cooking" || v.ToReview || v.Target != nil {
			t.Errorf("got %+v", v)
		}
	})
	for name, mod := range map[string]func(*ValidateInput){
		"auto off":       func(in *ValidateInput) { in.Taxonomy.Dynamic.AutoCreate = false },
		"low confidence": func(in *ValidateInput) { in.Decision.Confidence = 0.8 },
		"at cap":         func(in *ValidateInput) { in.Taxonomy.Dynamic.MaxCollections = 2 },
		"cannot create":  func(in *ValidateInput) { in.CanCreateCollections = false },
	} {
		t.Run(name, func(t *testing.T) {
			in := baseInput()
			prop(&in)
			mod(&in)
			v := Validate(in)
			if v.ProposedCollection == nil || !v.ToReview {
				t.Errorf("got %+v", v)
			}
		})
	}
}

func TestValidateDropsTagsThatRepeatTheCollection(t *testing.T) {
	t.Run("the chosen collection's key or name", func(t *testing.T) {
		in := baseInput()
		in.Taxonomy.Collections[0] = domain.TaxonomyCollection{Key: "dev_tools", Name: "Dev Tools"}
		in.Decision.Collection.Key = "dev_tools"
		in.Resolved = map[string]domain.ID{"dev_tools": idTech}
		in.Decision.Tags = tags("dev-tools", "Dev_Tools", "kafka", "go")
		got := Validate(in)
		if !slices.Equal(got.AddTags, []string{"kafka", "go"}) {
			t.Errorf("AddTags = %v", got.AddTags)
		}
	})
	t.Run("another collection's name is still a fair topic", func(t *testing.T) {
		in := baseInput()
		in.Taxonomy.Collections = append(in.Taxonomy.Collections, domain.TaxonomyCollection{Key: "backend", Name: "Backend"})
		in.Decision.Tags = tags("backend", "go")
		if got := Validate(in); !slices.Equal(got.AddTags, []string{"backend", "go"}) {
			t.Errorf("AddTags = %v", got.AddTags)
		}
	})
	t.Run("a tags-only run compares with the current collection", func(t *testing.T) {
		in := baseInput()
		in.WantCollection = false
		in.Decision.Collection = domain.CollectionChoice{}
		in.Bookmark.Collection = idTech
		in.Decision.Tags = tags("tech", "go")
		if got := Validate(in); !slices.Equal(got.AddTags, []string{"go"}) {
			t.Errorf("AddTags = %v", got.AddTags)
		}
	})
	t.Run("a tag the user already has is kept", func(t *testing.T) {
		in := baseInput()
		in.Bookmark.Tags = []string{"tech"}
		in.Decision.Tags = tags("go")
		got := Validate(in)
		if !slices.Equal(got.AddTags, []string{"go"}) || len(got.Reasons) != 0 {
			t.Errorf("AddTags = %v, reasons = %v", got.AddTags, got.Reasons)
		}
	})
}

func TestValidateDropsMalformedTags(t *testing.T) {
	in := baseInput()
	in.Decision.Tags = tags("??", "]}the-user-asks:", "kafka", "the…….", "go")
	got := Validate(in)
	if !slices.Equal(got.AddTags, []string{"kafka", "go"}) || !slices.Equal(got.NewTags, []string{"kafka", "go"}) {
		t.Errorf("AddTags = %v, NewTags = %v", got.AddTags, got.NewTags)
	}
}
