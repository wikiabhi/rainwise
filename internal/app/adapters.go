package app

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/wikiabhi/rainwise/internal/adapters/classifier/jev"
	"github.com/wikiabhi/rainwise/internal/adapters/classifier/llm"
	"github.com/wikiabhi/rainwise/internal/adapters/provider/raindrop"
	"github.com/wikiabhi/rainwise/internal/config"
	"github.com/wikiabhi/rainwise/internal/core/events"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/httpapi"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	providerRaindrop = "raindrop"
	classifierLLM    = "llm"
	classifierJev    = "jev"

	contentFetchOff    = "off"
	contentFetchAuto   = "auto"
	contentFetchAlways = "always"
)

// adapters is what config selects. One provider runs per process in the MVP
// (§12). Fallback is nil if unset, and proposer nil if the role's classifier can't
// propose, which is an error only with a dynamic taxonomy.
type adapters struct {
	name        string // The provider's name, which IDs and jobs carry.
	provider    ports.Provider
	classifiers map[string]ports.Classifier
	classifier  ports.Classifier
	fallback    ports.Classifier
	proposer    ports.TaxonomyProposer
	configured  []domain.Taxonomy
}

// buildAdapters reports every bad value at once. Constructing an adapter makes no
// calls, so this is safe for `config validate` too.
func buildAdapters(cfg *config.Config, deps BuildDeps) (*adapters, error) {
	var errs []error
	switch cfg.Processing.ContentFetch {
	case contentFetchOff, contentFetchAuto, contentFetchAlways:
	default:
		errs = append(errs, fmt.Errorf("config: processing.content_fetch must be %q, %q, or %q, got %q",
			contentFetchOff, contentFetchAuto, contentFetchAlways, cfg.Processing.ContentFetch))
	}

	a := &adapters{provider: deps.Provider, classifiers: map[string]ports.Classifier{}}
	for _, name := range slices.Sorted(maps.Keys(cfg.Providers)) {
		p := cfg.Providers[name]
		selected := name == cfg.Processing.Provider
		switch {
		case selected && deps.Provider != nil:
		case p.Type != providerRaindrop:
			errs = append(errs, fmt.Errorf("config: providers.%s.type must be %q, got %q", name, providerRaindrop, p.Type))
		case selected:
			a.provider = raindrop.New(raindrop.Config{
				Token:             p.Token.Value(),
				RequestsPerMinute: p.RequestsPerMinute,
				IncrementalSince:  p.IncrementalSince,
				TagListing:        p.TagListing,
			})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Classifiers)) {
		c := cfg.Classifiers[name]
		switch {
		case name == cfg.Roles.Classifier && deps.Classifier != nil:
			a.classifiers[name] = deps.Classifier
		case c.Type == classifierLLM:
			a.classifiers[name] = llm.New(llm.Config{
				Name: name, BaseURL: c.BaseURL, Model: c.Model, APIKey: c.APIKey.Value(), ExtraBody: c.ExtraBody,
			})
		case c.Type == classifierJev:
			a.classifiers[name] = jev.New(jev.Config{
				Name: name, BaseURL: c.BaseURL, Model: c.Model, APIKey: c.APIKey.Value(),
				MaxTagCandidates: c.MaxTagCandidates, TagThreshold: c.TagThreshold,
			})
		default:
			errs = append(errs, fmt.Errorf("config: classifiers.%s.type must be %q or %q, got %q", name, classifierLLM, classifierJev, c.Type))
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	// IDs and job rows carry the adapter's own name, so the config key has to agree.
	a.name = a.provider.Name()
	if a.name != cfg.Processing.Provider {
		return nil, fmt.Errorf("config: processing.provider %q must be named after its adapter, %q", cfg.Processing.Provider, a.name)
	}
	a.classifier = a.classifiers[cfg.Roles.Classifier]
	if cfg.Roles.Fallback != nil {
		a.fallback = a.classifiers[*cfg.Roles.Fallback]
	}
	a.proposer, _ = a.classifiers[cfg.Roles.Proposer].(ports.TaxonomyProposer)
	return a, nil
}

// checkTaxonomies applies §6.4's startup rules.
func checkTaxonomies(cfg *config.Config, a *adapters, taxonomies []domain.Taxonomy) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf("config: "+format, args...)) }
	tooMany := func(t domain.Taxonomy, role, name string, c ports.Classifier) {
		if most := c.Capabilities().MaxChoices; most > 0 && len(t.Collections) > most {
			add("taxonomies.%s has %d collections, more than the %d that roles.%s %q can choose from", t.ID, len(t.Collections), most, role, name)
		}
	}
	for _, t := range taxonomies {
		if t.Mode == domain.ModeDynamic && a.proposer == nil {
			add("roles.proposer %q can't propose taxonomies, and taxonomies.%s is dynamic", cfg.Roles.Proposer, t.ID)
		}
		if t.Dynamic.AutoCreate && !a.classifier.Capabilities().ProposeCollections {
			add("roles.classifier %q can't propose collections, and taxonomies.%s has dynamic.auto_create on", cfg.Roles.Classifier, t.ID)
		}
		tooMany(t, "classifier", cfg.Roles.Classifier, a.classifier)
		if a.fallback != nil {
			tooMany(t, "fallback", *cfg.Roles.Fallback, a.fallback)
		}
	}
	return errors.Join(errs...)
}

// webhookSecrets opens the webhook route only for a provider with a secret set.
func (a *adapters) webhookSecrets(cfg *config.Config) map[string]string {
	secrets := map[string]string{}
	if s := cfg.Providers[cfg.Processing.Provider].WebhookSecret.Value(); s != "" {
		secrets[a.name] = s
	}
	return secrets
}

// normalizer is the provider's own, or else the canonical payload of §7.2, which
// any source can send.
func (a *adapters) normalizer() ports.EventNormalizer {
	if n, ok := a.provider.(ports.EventNormalizer); ok {
		return n
	}
	return canonicalNormalizer(a.name)
}

type canonicalNormalizer string

func (n canonicalNormalizer) Normalize(_ context.Context, _ http.Header, body []byte) ([]domain.Event, error) {
	return events.NormalizeCanonical(string(n), body)
}

// classifierInfo lists every configured classifier and the roles it fills.
func (a *adapters) classifierInfo(cfg *config.Config) []httpapi.ClassifierInfo {
	var out []httpapi.ClassifierInfo
	for _, name := range slices.Sorted(maps.Keys(a.classifiers)) {
		roles := []string{}
		if name == cfg.Roles.Classifier {
			roles = append(roles, "classifier")
		}
		if name == cfg.Roles.Proposer {
			roles = append(roles, "proposer")
		}
		if cfg.Roles.Fallback != nil && name == *cfg.Roles.Fallback {
			roles = append(roles, "fallback")
		}
		out = append(out, httpapi.ClassifierInfo{Name: name, Roles: roles, Capabilities: a.classifiers[name].Capabilities()})
	}
	return out
}
