package pipeline

import (
	"fmt"
	"strings"

	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/domain"
)

const roleReview = "review"

// ValidateInput is a classifier decision plus the context to check it against.
// Resolved maps taxonomy collection keys to provider collection IDs.
type ValidateInput struct {
	Decision             domain.Decision
	Bookmark             domain.Bookmark
	Taxonomy             domain.Taxonomy
	Vocab                *taxonomy.RunVocab
	WantCollection       bool
	Resolved             map[string]domain.ID
	CanCreateCollections bool
}

// Validated is the safe subset of a decision. Target is nil when the bookmark
// should not move; ToReview means it should go to the review collection.
type Validated struct {
	Target             *domain.ID
	ToReview           bool
	AddTags, NewTags   []string
	ProposedCollection *domain.CollectionProposal
	Reasons            []string
}

func Validate(in ValidateInput) Validated {
	var v Validated
	if in.WantCollection {
		validateCollection(in, &v)
	}
	validateTags(in, &v)
	return v
}

func findCollection(tax domain.Taxonomy, s string) (domain.TaxonomyCollection, bool) {
	s = strings.TrimSpace(s)
	for _, c := range tax.Collections {
		if strings.EqualFold(c.Key, s) {
			return c, true
		}
	}
	for _, c := range tax.Collections {
		if strings.EqualFold(c.Name, s) {
			return c, true
		}
	}
	return domain.TaxonomyCollection{}, false
}

// inAllowed reports whether the bookmark sits in a non-review taxonomy collection.
func inAllowed(in ValidateInput) bool {
	for _, c := range in.Taxonomy.Collections {
		if c.Role == roleReview {
			continue
		}
		if id, ok := in.Resolved[c.Key]; ok && id == in.Bookmark.Collection {
			return true
		}
	}
	return false
}

func validateCollection(in ValidateInput, v *Validated) {
	d := in.Decision
	choice := d.Collection
	col, found := findCollection(in.Taxonomy, choice.Key)
	if !found {
		if choice.ProposedName != "" {
			validateProposal(in, v)
			return
		}
		v.Reasons = append(v.Reasons, fmt.Sprintf("collection %q is not allowed", choice.Key))
		v.ToReview = true
		return
	}
	if col.Role == roleReview {
		v.ToReview = true
		return
	}
	if d.Confidence < in.Taxonomy.Rules.MinConfidence {
		v.Reasons = append(v.Reasons, fmt.Sprintf("confidence %.2f below minimum %.2f", d.Confidence, in.Taxonomy.Rules.MinConfidence))
		if !inAllowed(in) {
			v.ToReview = true
		}
		return
	}
	id, ok := in.Resolved[col.Key]
	if !ok {
		v.Reasons = append(v.Reasons, fmt.Sprintf("collection %q has no resolved id", col.Key))
		v.ToReview = true
		return
	}
	if id == in.Bookmark.Collection {
		return
	}
	v.Target = &id
}

func validateProposal(in ValidateInput, v *Validated) {
	name := strings.TrimSpace(in.Decision.Collection.ProposedName)
	v.ProposedCollection = &domain.CollectionProposal{Name: name, Count: 1}
	dyn := in.Taxonomy.Dynamic
	switch {
	case !dyn.AutoCreate:
		v.Reasons = append(v.Reasons, fmt.Sprintf("proposed collection %q needs approval", name))
	case !in.CanCreateCollections:
		v.Reasons = append(v.Reasons, fmt.Sprintf("proposed collection %q: provider cannot create collections", name))
	case in.Decision.Confidence < dyn.AutoCreateMinConfidence:
		v.Reasons = append(v.Reasons, fmt.Sprintf("proposed collection %q: confidence %.2f below auto-create minimum %.2f", name, in.Decision.Confidence, dyn.AutoCreateMinConfidence))
	case len(in.Taxonomy.Collections) >= dyn.MaxCollections:
		v.Reasons = append(v.Reasons, fmt.Sprintf("proposed collection %q: collection cap %d reached", name, dyn.MaxCollections))
	default:
		return // accepted for creation; not sent to review
	}
	v.ToReview = true
}

func validateTags(in ValidateInput, v *Validated) {
	rules := in.Taxonomy.Rules
	existing := in.Bookmark.Tags
	room := tagRoom(existing, rules)
	vocab := in.Vocab
	if vocab == nil {
		vocab = taxonomy.NewRunVocab(existing, rules.TagAliases, rules.AllowNewTags)
	}
	seen := map[string]bool{}
	for _, e := range existing {
		seen[taxonomy.ComparisonKey(e)] = true
	}
	for _, s := range in.Decision.Tags {
		if len(v.AddTags) >= room {
			break
		}
		n := taxonomy.Normalize(s.Name, rules.TagAliases)
		if n == "" || seen[taxonomy.ComparisonKey(n)] {
			continue
		}
		final, isNew, ok := vocab.Resolve(s.Name)
		if !ok {
			continue
		}
		k := taxonomy.ComparisonKey(final)
		if seen[k] {
			continue
		}
		seen[k] = true
		v.AddTags = append(v.AddTags, final)
		if isNew {
			v.NewTags = append(v.NewTags, final)
		}
	}
	if total := len(existing) + len(v.AddTags); total < rules.MinTags {
		v.Reasons = append(v.Reasons, fmt.Sprintf("only %d tags, below target %d", total, rules.MinTags))
	}
}
