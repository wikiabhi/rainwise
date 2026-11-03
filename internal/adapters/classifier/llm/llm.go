// Package llm is a ports.Classifier and ports.TaxonomyProposer for any
// OpenAI-compatible chat completions API.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	maxRetries     = 3
	baseBackoff    = time.Second
	proposeBatch   = 200
	maxResponseLen = 4 << 20
)

// Config configures a Classifier. APIKey is a plain string; the caller reads
// it from the environment.
type Config struct {
	Name      string
	BaseURL   string
	Model     string
	APIKey    string
	Headers   map[string]string
	ExtraBody map[string]any // merged into the request's top level
	HTTP      *http.Client
}

type Classifier struct {
	cfg   Config
	sleep func(context.Context, time.Duration) error // injected in tests
}

var (
	_ ports.Classifier       = (*Classifier)(nil)
	_ ports.TaxonomyProposer = (*Classifier)(nil)
)

func New(cfg Config) *Classifier {
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 120 * time.Second}
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	return &Classifier{cfg: cfg, sleep: realSleep}
}

func realSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Classifier) Name() string { return c.cfg.Name }

func (c *Classifier) Capabilities() ports.ClassifierCapabilities {
	return ports.ClassifierCapabilities{FixedChoice: true, ProposeCollections: true, ProposeTags: true}
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completion struct {
	content   string
	model     string
	inTokens  int
	outTokens int
}

func (c *Classifier) Classify(ctx context.Context, r ports.ClassifyRequest) (domain.Decision, error) {
	out, err := c.complete(ctx, []message{
		{"system", classifySystem(r)},
		{"user", classifyUser(r)},
	}, "decision", decisionSchema(r))
	if err != nil {
		return domain.Decision{}, err
	}
	d, err := parseDecision(out.content, r)
	if err != nil {
		return domain.Decision{}, err
	}
	d.Model = c.modelInfo(out)
	return d, nil
}

func (c *Classifier) ProposeTaxonomy(ctx context.Context, r ports.ProposeRequest) (ports.TaxonomyProposal, error) {
	var (
		parts [][]domain.CollectionProposal
		info  domain.ModelInfo
	)
	system := proposeSystem(r)
	add := func(out completion) {
		mi := c.modelInfo(out)
		info.Classifier, info.Model = mi.Classifier, mi.Model
		info.InputTokens += mi.InputTokens
		info.OutputTokens += mi.OutputTokens
	}
	for start := 0; start < len(r.Samples) || start == 0; start += proposeBatch {
		end := min(start+proposeBatch, len(r.Samples))
		out, err := c.complete(ctx, []message{{"system", system}, {"user", proposeUser(r.Samples[start:end])}}, "proposal", proposalSchema())
		if err != nil {
			return ports.TaxonomyProposal{}, err
		}
		add(out)
		p, err := parseProposals(out.content)
		if err != nil {
			return ports.TaxonomyProposal{}, err
		}
		parts = append(parts, p)
	}
	merged := parts[0]
	if len(parts) > 1 {
		out, err := c.complete(ctx, []message{{"system", system}, {"user", mergeUser(parts)}}, "proposal", proposalSchema())
		if err != nil {
			return ports.TaxonomyProposal{}, err
		}
		add(out)
		if merged, err = parseProposals(out.content); err != nil {
			return ports.TaxonomyProposal{}, err
		}
	}
	if r.MaxCollections > 0 && len(merged) > r.MaxCollections {
		merged = merged[:r.MaxCollections]
	}
	return ports.TaxonomyProposal{Collections: merged, Model: info}, nil
}

func (c *Classifier) modelInfo(o completion) domain.ModelInfo {
	m := o.model
	if m == "" {
		m = c.cfg.Model
	}
	return domain.ModelInfo{Classifier: c.cfg.Name, Model: m, InputTokens: o.inTokens, OutputTokens: o.outTokens}
}

// complete makes one logical request. It retries 429, 5xx and transport
// failures with backoff; invalid output is the caller's concern.
func (c *Classifier) complete(ctx context.Context, msgs []message, schemaName string, schema map[string]any) (completion, error) {
	body := map[string]any{}
	for k, v := range c.cfg.ExtraBody {
		body[k] = v
	}
	body["model"] = c.cfg.Model
	body["messages"] = msgs
	body["response_format"] = responseFormat(schemaName, schema)
	payload, err := json.Marshal(body)
	if err != nil {
		return completion{}, fmt.Errorf("encode request: %w", err)
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		out, retryable, err := c.post(ctx, payload)
		if err == nil || !retryable {
			return out, err
		}
		lastErr = err
		if attempt >= maxRetries {
			return completion{}, lastErr
		}
		if err := c.sleep(ctx, baseBackoff<<attempt); err != nil {
			return completion{}, err
		}
	}
}

func (c *Classifier) post(ctx context.Context, payload []byte) (completion, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return completion{}, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return completion{}, false, ctx.Err()
		}
		// The client's error text can include the URL; it never includes the key.
		return completion{}, true, fmt.Errorf("%w: %v", domain.ErrTemporary, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseLen))
	if err != nil {
		return completion{}, true, fmt.Errorf("%w: read response: %v", domain.ErrTemporary, err)
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return completion{}, true, fmt.Errorf("%w: llm endpoint returned 429", domain.ErrRateLimited)
	case resp.StatusCode >= 500:
		return completion{}, true, fmt.Errorf("%w: llm endpoint returned %d", domain.ErrTemporary, resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		return completion{}, false, fmt.Errorf("llm endpoint returned %d: %s", resp.StatusCode, snippet(raw))
	}
	var env struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			Prompt     int `json:"prompt_tokens"`
			Completion int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return completion{}, false, invalid("response envelope: %v", err)
	}
	if len(env.Choices) == 0 {
		return completion{}, false, invalid("response has no choices")
	}
	return completion{
		content: env.Choices[0].Message.Content, model: env.Model,
		inTokens: env.Usage.Prompt, outTokens: env.Usage.Completion,
	}, false, nil
}

func snippet(b []byte) string {
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
