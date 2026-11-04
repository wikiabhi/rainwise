package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const roleReview = "review"

// responseFormat wraps a JSON schema in the OpenAI response_format envelope.
func responseFormat(name string, schema map[string]any) map[string]any {
	return map[string]any{
		"type":        "json_schema",
		"json_schema": map[string]any{"name": name, "strict": true, "schema": schema},
	}
}

// decisionSchema asks only for the fields the request wants. Strict mode
// requires every property to be listed as required.
func decisionSchema(r ports.ClassifyRequest) map[string]any {
	props := map[string]any{}
	if r.WantCollection {
		fixed := r.Taxonomy.Mode == domain.ModeFixed
		keys := []any{}
		if !fixed {
			keys = append(keys, "")
		}
		for _, c := range r.Taxonomy.Collections {
			if c.Role != roleReview {
				keys = append(keys, c.Key)
			}
		}
		props["collection"] = map[string]any{"type": "string", "enum": keys}
		if !fixed {
			props["proposed_name"] = map[string]any{"type": "string"}
		}
	}
	if r.TagsWanted > 0 {
		props["tags"] = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
	}
	props["confidence"] = map[string]any{"type": "number", "minimum": 0, "maximum": 1}
	props["reasoning"] = map[string]any{"type": "string"}
	return objectSchema(props)
}

func proposalSchema() map[string]any {
	item := objectSchema(map[string]any{
		"name":        map[string]any{"type": "string"},
		"description": map[string]any{"type": "string"},
		"count":       map[string]any{"type": "integer"},
	})
	return objectSchema(map[string]any{
		"collections": map[string]any{"type": "array", "items": item},
	})
}

func objectSchema(props map[string]any) map[string]any {
	req := make([]string, 0, len(props))
	for k := range props {
		req = append(req, k)
	}
	return map[string]any{
		"type": "object", "properties": props, "required": req, "additionalProperties": false,
	}
}

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", domain.ErrInvalidOutput, fmt.Sprintf(format, a...))
}

// decodeJSON strips an optional markdown code fence and decodes strictly.
func decodeJSON(content string, v any) error {
	s := strings.TrimSpace(content)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```")
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			s = s[i+1:]
		}
		s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "```"))
	}
	if s == "" {
		return invalid("empty response")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	if err := dec.Decode(v); err != nil {
		return invalid("%v", err)
	}
	return nil
}

type rawDecision struct {
	Collection   string   `json:"collection"`
	ProposedName string   `json:"proposed_name"`
	Tags         []string `json:"tags"`
	Confidence   *float64 `json:"confidence"`
	Reasoning    string   `json:"reasoning"`
}

func parseDecision(content string, r ports.ClassifyRequest) (domain.Decision, error) {
	var raw rawDecision
	if err := decodeJSON(content, &raw); err != nil {
		return domain.Decision{}, err
	}
	if raw.Confidence == nil {
		return domain.Decision{}, invalid("missing confidence")
	}
	if c := *raw.Confidence; c < 0 || c > 1 || c != c {
		return domain.Decision{}, invalid("confidence %v outside 0..1", c)
	}
	d := domain.Decision{Confidence: *raw.Confidence, Reasoning: raw.Reasoning}
	if r.WantCollection {
		d.Collection = domain.CollectionChoice{
			Key:          strings.TrimSpace(raw.Collection),
			ProposedName: strings.TrimSpace(raw.ProposedName),
		}
		if k := d.Collection.Key; k != "" && !knownCollection(r, k) {
			return domain.Decision{}, invalid("collection %q is not in the taxonomy", k)
		}
	}
	if r.TagsWanted > 0 {
		for _, t := range raw.Tags {
			if t = strings.TrimSpace(t); t != "" && len(d.Tags) < r.TagsWanted {
				d.Tags = append(d.Tags, domain.TagSuggestion{Name: t})
			}
		}
	}
	return d, nil
}

// knownCollection matches a collection's key or name, ignoring case. The review
// collection counts: the schema doesn't offer it, but choosing it is a fair answer.
func knownCollection(r ports.ClassifyRequest, s string) bool {
	for _, c := range r.Taxonomy.Collections {
		if strings.EqualFold(c.Key, s) || strings.EqualFold(c.Name, s) {
			return true
		}
	}
	return false
}

func parseProposals(content string) ([]domain.CollectionProposal, error) {
	var raw struct {
		Collections []domain.CollectionProposal `json:"collections"`
	}
	if err := decodeJSON(content, &raw); err != nil {
		return nil, err
	}
	out := raw.Collections[:0]
	for _, c := range raw.Collections {
		if c.Name = strings.TrimSpace(c.Name); c.Name != "" {
			out = append(out, c)
		}
	}
	return out, nil
}
