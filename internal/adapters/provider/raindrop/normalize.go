package raindrop

import (
	"context"
	"net/http"

	"github.com/wikiabhi/rainwise/internal/core/events"
	"github.com/wikiabhi/rainwise/internal/domain"
)

// Normalize accepts the canonical webhook payload.
func (p *Provider) Normalize(_ context.Context, _ http.Header, body []byte) ([]domain.Event, error) {
	return events.NormalizeCanonical(providerName, body)
}
