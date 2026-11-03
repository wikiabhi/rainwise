package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wikiabhi/rainwise/internal/adapters/classifier/classifiertest"
	"github.com/wikiabhi/rainwise/internal/adapters/classifier/fake"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func TestConformance(t *testing.T) {
	c := fake.New("fake")
	classifiertest.Run(t, c, func(d domain.Decision) { c.Script(fake.Result{Decision: d}) })
}

func TestConformanceFixedChoiceOnly(t *testing.T) {
	c := fake.New("fake")
	c.SetCapabilities(ports.ClassifierCapabilities{FixedChoice: true, MaxChoices: 8})
	classifiertest.Run(t, c, func(d domain.Decision) { c.Script(fake.Result{Decision: d}) })
}

func TestScriptOrderAndCallCount(t *testing.T) {
	boom := errors.New("boom")
	c := fake.New("fake", fake.Result{Err: boom}, fake.Result{Decision: domain.Decision{Confidence: 0.5}})
	req := ports.ClassifyRequest{WantCollection: true, TagsWanted: 1}
	if _, err := c.Classify(context.Background(), req); !errors.Is(err, boom) {
		t.Fatalf("first = %v", err)
	}
	for range 2 { // the last entry repeats
		d, err := c.Classify(context.Background(), req)
		if err != nil || d.Confidence != 0.5 {
			t.Fatalf("got %+v, %v", d, err)
		}
	}
	if c.Calls() != 3 || len(c.Requests()) != 3 {
		t.Fatalf("calls = %d, requests = %d", c.Calls(), len(c.Requests()))
	}
}
