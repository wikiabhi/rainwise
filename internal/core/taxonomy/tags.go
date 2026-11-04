// Package taxonomy holds tag normalization and the per-run tag vocabulary.
package taxonomy

import (
	"strings"
	"sync"
)

// Normalize trims, drops a leading '#', lowercases, turns whitespace into
// hyphens, and applies aliases. It returns "" for an empty tag.
func Normalize(tag string, aliases map[string]string) string {
	n := clean(tag)
	if n == "" {
		return ""
	}
	if a, ok := aliases[n]; ok {
		return clean(a)
	}
	for k, a := range aliases {
		if clean(k) == n {
			return clean(a)
		}
	}
	return n
}

func clean(tag string) string {
	s := strings.TrimSpace(tag)
	s = strings.TrimLeft(s, "#")
	s = strings.ToLower(strings.Join(strings.Fields(s), "-"))
	return s
}

// ComparisonKey folds case, word breaks and simple plurals, so "System Design",
// "system-design", "systemdesign" and "system_designs" are one tag. It is for comparison only and
// is never written to a tag.
func ComparisonKey(tag string) string {
	s := strings.ToLower(strings.Join(strings.Fields(tag), ""))
	s = strings.NewReplacer("_", "", "-", "").Replace(s)
	if len(s) > 3 && strings.HasSuffix(s, "s") &&
		!strings.HasSuffix(s, "ss") && !strings.HasSuffix(s, "us") && !strings.HasSuffix(s, "is") {
		s = s[:len(s)-1]
	}
	return s
}

// RunVocab is the tag vocabulary for one run. New tags accepted during the run
// join it so later bookmarks reuse them. Safe for concurrent use.
type RunVocab struct {
	mu       sync.Mutex
	aliases  map[string]string
	allowNew bool
	byKey    map[string]string
	order    []string // every term in byKey, oldest first
	newKeys  map[string]bool
	proposed map[string]int
}

func NewRunVocab(existing []string, aliases map[string]string, allowNew bool) *RunVocab {
	v := &RunVocab{
		aliases:  aliases,
		allowNew: allowNew,
		byKey:    map[string]string{},
		newKeys:  map[string]bool{},
		proposed: map[string]int{},
	}
	for _, e := range existing {
		if strings.TrimSpace(e) == "" {
			continue
		}
		k := ComparisonKey(e)
		if _, ok := v.byKey[k]; !ok {
			v.byKey[k] = e
			v.order = append(v.order, e)
		}
	}
	return v
}

// Resolve maps a tag to its final form. Existing spellings win. isNew is true
// the first time a tag outside the existing vocabulary is accepted. ok is false
// for empty tags and for unknown tags when new tags are not allowed.
func (v *RunVocab) Resolve(tag string) (final string, isNew, ok bool) {
	n := Normalize(tag, v.aliases)
	if n == "" {
		return "", false, false
	}
	k := ComparisonKey(n)
	v.mu.Lock()
	defer v.mu.Unlock()
	if f, found := v.byKey[k]; found {
		if v.newKeys[k] {
			v.proposed[f]++
		}
		return f, false, true
	}
	if !v.allowNew {
		return "", false, false
	}
	v.byKey[k] = n
	v.order = append(v.order, n)
	v.newKeys[k] = true
	v.proposed[n]++
	return n, true, true
}

// Existing looks a tag up in the vocabulary the run started with, returning its
// existing spelling. Tags accepted during the run don't count. It changes nothing.
func (v *RunVocab) Existing(tag string) (string, bool) {
	n := Normalize(tag, v.aliases)
	if n == "" {
		return "", false
	}
	k := ComparisonKey(n)
	v.mu.Lock()
	defer v.mu.Unlock()
	f, found := v.byKey[k]
	if !found || v.newKeys[k] {
		return "", false
	}
	return f, true
}

// Terms lists the vocabulary in its original order, then new tags as accepted.
func (v *RunVocab) Terms() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.order...)
}

// ProposedCounts returns each new tag accepted this run with its use count.
func (v *RunVocab) ProposedCounts() map[string]int {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]int, len(v.proposed))
	for k, c := range v.proposed {
		out[k] = c
	}
	return out
}
