package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
)

// webhook is §7.2. The provider must have a secret; its own secret, not the API
// token, opens the route.
func (s *server) webhook(w http.ResponseWriter, r *http.Request) {
	provider := r.PathValue("provider")
	secret, ok := s.WebhookSecrets[provider]
	if !ok {
		s.fail(w, &apiError{status: http.StatusNotFound, code: "not_found", message: "unknown provider"})
		return
	}
	if !webhookAuthorized(r, secret) {
		s.unauthorized(w)
		return
	}
	norm, ok := s.Normalizers[provider]
	if !ok {
		s.fail(w, &apiError{status: http.StatusNotFound, code: "not_found", message: "provider has no webhook normalizer"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		s.fail(w, bodyError(err))
		return
	}
	if !json.Valid(body) {
		s.fail(w, badRequest("webhook body is not valid JSON"))
		return
	}
	evs, err := norm.Normalize(r.Context(), r.Header, body)
	if err != nil {
		s.fail(w, err)
		return
	}
	for i := range evs {
		evs[i].Provider = provider
	}
	if err := s.Ingestor.Ingest(r.Context(), evs); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": len(evs)})
}
