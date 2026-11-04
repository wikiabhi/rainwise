package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

type providerView struct {
	Name         string           `json:"name"`
	Capabilities providerCapsView `json:"capabilities"`
}

type providerCapsView struct {
	Highlights        bool    `json:"highlights"`
	CreateCollections bool    `json:"create_collections"`
	NativeWebhooks    bool    `json:"native_webhooks"`
	IncrementalSince  bool    `json:"incremental_since"`
	CursorPagination  bool    `json:"cursor_pagination"`
	TagListing        bool    `json:"tag_listing"`
	InboxCollection   *string `json:"inbox_collection"`
}

func (s *server) providers(w http.ResponseWriter, r *http.Request) {
	items := make([]providerView, len(s.Providers))
	for i, p := range s.Providers {
		c := p.Capabilities
		items[i] = providerView{Name: p.Name, Capabilities: providerCapsView{
			Highlights: c.Highlights, CreateCollections: c.CreateCollections, NativeWebhooks: c.NativeWebhooks,
			IncrementalSince: c.IncrementalSince, CursorPagination: c.CursorPagination, TagListing: c.TagListing,
			InboxCollection: idPtr(c.InboxCollection),
		}}
	}
	writeJSON(w, http.StatusOK, newPage(items, ""))
}

type classifierView struct {
	Name         string             `json:"name"`
	Roles        []string           `json:"roles"`
	Capabilities classifierCapsView `json:"capabilities"`
}

type classifierCapsView struct {
	FixedChoice          bool `json:"fixed_choice"`
	ProposeCollections   bool `json:"propose_collections"`
	ProposeTags          bool `json:"propose_tags"`
	CalibratedConfidence bool `json:"calibrated_confidence"`
	MaxChoices           int  `json:"max_choices"`
}

func (s *server) classifiers(w http.ResponseWriter, r *http.Request) {
	items := make([]classifierView, len(s.Classifiers))
	for i, c := range s.Classifiers {
		items[i] = classifierView{Name: c.Name, Roles: nonNil(c.Roles), Capabilities: viewClassifierCaps(c.Capabilities)}
	}
	writeJSON(w, http.StatusOK, newPage(items, ""))
}

func viewClassifierCaps(c ports.ClassifierCapabilities) classifierCapsView {
	return classifierCapsView{c.FixedChoice, c.ProposeCollections, c.ProposeTags, c.CalibratedConfidence, c.MaxChoices}
}

// taxonomyView keeps lists out of null.
func taxonomyView(t domain.Taxonomy) domain.Taxonomy {
	if t.Collections == nil {
		t.Collections = []domain.TaxonomyCollection{}
	}
	return t
}

func etag(version int) string { return `"` + strconv.Itoa(version) + `"` }

func (s *server) listTaxonomies(w http.ResponseWriter, r *http.Request) {
	ts, err := s.Taxonomies.List(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	for i := range ts {
		ts[i] = taxonomyView(ts[i])
	}
	writeJSON(w, http.StatusOK, newPage(ts, ""))
}

func (s *server) getTaxonomy(w http.ResponseWriter, r *http.Request) {
	t, err := s.Taxonomies.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("ETag", etag(t.Version))
	writeJSON(w, http.StatusOK, taxonomyView(t))
}

// putTaxonomy replaces a taxonomy. The path names it, whatever the body says.
// If-Match holds a version number, bare or quoted.
func (s *server) putTaxonomy(w http.ResponseWriter, r *http.Request) {
	var ifVersion *int
	if h := strings.TrimSpace(r.Header.Get("If-Match")); h != "" {
		n, err := strconv.Atoi(strings.Trim(h, `"`))
		if err != nil || n < 0 {
			s.fail(w, badRequest("If-Match must be a taxonomy version number"))
			return
		}
		ifVersion = &n
	}
	var body domain.Taxonomy
	if err := decodeBody(w, r, &body); err != nil {
		s.fail(w, err)
		return
	}
	t, err := s.Taxonomies.Put(r.Context(), r.PathValue("id"), body, ifVersion)
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("ETag", etag(t.Version))
	writeJSON(w, http.StatusOK, taxonomyView(t))
}

func (s *server) proposals(w http.ResponseWriter, r *http.Request) {
	t, err := s.Taxonomies.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newPage(t.Proposals, ""))
}
