// Package raindrop adapts the Raindrop.io REST API to the provider ports.
package raindrop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	providerName   = "raindrop"
	defaultBaseURL = "https://api.raindrop.io/rest/v1"
	defaultRPM     = 100
	maxAttempts    = 5
	maxPerPage     = 50
)

type Config struct {
	Token             string // never logged or put in errors
	BaseURL           string
	RequestsPerMinute int
	IncrementalSince  bool
	TagListing        bool
	HTTP              *http.Client
	Sleep             func(context.Context, time.Duration) error
	Now               func() time.Time
}

type Provider struct {
	cfg     Config
	limiter *limiter
}

var (
	_ ports.Provider          = (*Provider)(nil)
	_ ports.HighlightReader   = (*Provider)(nil)
	_ ports.CollectionManager = (*Provider)(nil)
	_ ports.EventNormalizer   = (*Provider)(nil)
	_ ports.TagLister         = (*Provider)(nil)
	_ ports.URLResolver       = (*Provider)(nil)
)

func New(cfg Config) *Provider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.RequestsPerMinute <= 0 {
		cfg.RequestsPerMinute = defaultRPM
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	return &Provider{cfg: cfg, limiter: newLimiter(cfg.RequestsPerMinute, cfg.Now)}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (p *Provider) Name() string { return providerName }

func (p *Provider) Capabilities() ports.ProviderCapabilities {
	inbox := domain.ID{Provider: providerName, Local: "-1"}
	return ports.ProviderCapabilities{
		Highlights:        true,
		CreateCollections: true,
		IncrementalSince:  p.cfg.IncrementalSince,
		TagListing:        p.cfg.TagListing,
		InboxCollection:   &inbox,
	}
}

func (p *Provider) backoff(attempt int) time.Duration {
	return min(time.Duration(500<<attempt)*time.Millisecond, 8*time.Second)
}

// do sends one logical request, retrying 429 and 5xx. Errors carry the method,
// path, and status only, never the token or response bodies.
func (p *Provider) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var payload []byte
	if in != nil {
		var err error
		if payload, err = json.Marshal(in); err != nil {
			return err
		}
	}
	target := p.cfg.BaseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var last error
	for attempt := range maxAttempts {
		if err := p.cfg.Sleep(ctx, p.limiter.reserve()); err != nil {
			return err
		}
		var body io.Reader
		if payload != nil {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return fmt.Errorf("raindrop: %s %s: bad request", method, path)
		}
		req.Header.Set("Authorization", "Bearer "+p.cfg.Token)
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := p.cfg.HTTP.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			last = fmt.Errorf("raindrop: %s %s: transport error: %w", method, path, domain.ErrTemporary)
			if err := p.cfg.Sleep(ctx, p.backoff(attempt)); err != nil {
				return err
			}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if exhausted(resp.Header) {
			if t, ok := resetTime(resp.Header); ok {
				p.limiter.pause(t)
			}
		}
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			last = fmt.Errorf("raindrop: %s %s: %w", method, path, domain.ErrRateLimited)
			if t, ok := resetTime(resp.Header); ok {
				p.limiter.pause(t)
			} else {
				p.limiter.pause(p.cfg.Now().Add(p.backoff(attempt)))
			}
			continue
		case resp.StatusCode >= 500:
			last = fmt.Errorf("raindrop: %s %s: status %d: %w", method, path, resp.StatusCode, domain.ErrTemporary)
			if err := p.cfg.Sleep(ctx, p.backoff(attempt)); err != nil {
				return err
			}
			continue
		case resp.StatusCode == http.StatusNotFound:
			return fmt.Errorf("raindrop: %s %s: %w", method, path, domain.ErrNotFound)
		case resp.StatusCode < 200 || resp.StatusCode > 299:
			return fmt.Errorf("raindrop: %s %s: unexpected status %d", method, path, resp.StatusCode)
		}
		if readErr != nil {
			last = fmt.Errorf("raindrop: %s %s: reading response: %w", method, path, domain.ErrTemporary)
			continue
		}
		if out != nil {
			if err := json.Unmarshal(data, out); err != nil {
				return fmt.Errorf("raindrop: %s %s: decoding response: %w", method, path, errors.New("malformed JSON"))
			}
		}
		return nil
	}
	return last
}

// localID returns the Raindrop-local part of an ID owned by this provider.
func localID(id domain.ID) (string, error) {
	if id.Provider != providerName || id.Local == "" || strings.ContainsAny(id.Local, "/?#") {
		return "", fmt.Errorf("id %q is not a raindrop id: %w", id, domain.ErrValidation)
	}
	return id.Local, nil
}

func (p *Provider) ListTags(ctx context.Context) ([]domain.TagCount, error) {
	if !p.cfg.TagListing {
		return nil, fmt.Errorf("raindrop tag listing is not enabled: %w", domain.ErrCapabilityUnsupported)
	}
	var resp struct {
		Items []struct {
			ID    string `json:"_id"`
			Count int    `json:"count"`
		} `json:"items"`
	}
	if err := p.do(ctx, http.MethodGet, "/tags", nil, nil, &resp); err != nil {
		return nil, err
	}
	out := make([]domain.TagCount, 0, len(resp.Items))
	for _, it := range resp.Items {
		out = append(out, domain.TagCount{Name: it.ID, Count: it.Count})
	}
	return out, nil
}
