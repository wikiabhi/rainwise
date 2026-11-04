// Package fake is a scripted ports.Classifier for tests.
package fake

import (
	"context"
	"slices"
	"sync"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

// Result is one scripted answer.
type Result struct {
	Decision domain.Decision
	Err      error
}

type Classifier struct {
	mu       sync.Mutex
	name     string
	caps     ports.ClassifierCapabilities
	script   []Result
	requests []ports.ClassifyRequest
	hook     func(ports.ClassifyRequest)
}

// New returns a classifier with every capability on. Results are answered in
// order and the last one repeats.
func New(name string, script ...Result) *Classifier {
	return &Classifier{
		name: name,
		caps: ports.ClassifierCapabilities{
			FixedChoice:          true,
			ProposeCollections:   true,
			ProposeTags:          true,
			CalibratedConfidence: true,
			MaxChoices:           100,
		},
		script: script,
	}
}

// Script replaces the remaining answers.
func (c *Classifier) Script(results ...Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.script = results
}

// SetCapabilities overrides the declared capabilities.
func (c *Classifier) SetCapabilities(caps ports.ClassifierCapabilities) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.caps = caps
}

// OnClassify registers a hook run during each call. No lock is held.
func (c *Classifier) OnClassify(f func(ports.ClassifyRequest)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hook = f
}

// Calls returns how many times Classify ran.
func (c *Classifier) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

// Requests returns each request received, in order.
func (c *Classifier) Requests() []ports.ClassifyRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.requests)
}

func (c *Classifier) Name() string { return c.name }

func (c *Classifier) Capabilities() ports.ClassifierCapabilities {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps
}

// Classify answers from the script, dropping whatever the request didn't ask
// for or the declared capabilities rule out, as a real classifier would.
func (c *Classifier) Classify(ctx context.Context, r ports.ClassifyRequest) (domain.Decision, error) {
	c.mu.Lock()
	c.requests = append(c.requests, r)
	hook := c.hook
	var res Result
	if n := len(c.script); n > 0 {
		res = c.script[0]
		if n > 1 {
			c.script = c.script[1:]
		}
	}
	caps := c.caps
	c.mu.Unlock()

	if hook != nil {
		hook(r)
	}
	if err := ctx.Err(); err != nil {
		return domain.Decision{}, err
	}
	if res.Err != nil {
		return domain.Decision{}, res.Err
	}
	d := res.Decision
	d.Tags = slices.Clone(d.Tags)
	switch {
	case r.TagsWanted == 0:
		d.Tags = nil
	case !caps.ProposeTags: // It can't invent tags, but it can choose from the vocabulary.
		d.Tags = slices.DeleteFunc(d.Tags, func(t domain.TagSuggestion) bool {
			return !slices.Contains(r.TagVocab, t.Name)
		})
	}
	if !r.WantCollection {
		d.Collection = domain.CollectionChoice{}
	}
	if !caps.ProposeCollections {
		d.Collection.ProposedName = ""
	}
	if d.Model.Classifier == "" {
		d.Model.Classifier = c.name
	}
	return d, nil
}
