package httpapi

import (
	"errors"
	"net/http"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

// apiError is an error with its response already decided.
type apiError struct {
	status  int
	code    string
	message string
	details map[string]any
}

func (e *apiError) Error() string { return e.message }

func badRequest(msg string) *apiError {
	return &apiError{status: http.StatusBadRequest, code: "malformed_request", message: msg}
}

func bodyError(err error) *apiError {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return &apiError{status: http.StatusRequestEntityTooLarge, code: "payload_too_large",
			message: "request body is over the 1 MB limit"}
	}
	return badRequest("malformed JSON body: " + err.Error())
}

// fail answers with err, mapped to a status. Anything unrecognized is a 500 whose
// body says nothing of the cause; the cause goes to the log.
func (s *server) fail(w http.ResponseWriter, err error) {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
	case errors.Is(err, domain.ErrAutomationDisabled):
		ae = &apiError{status: http.StatusConflict, code: "automation_disabled", message: err.Error()}
	case errors.Is(err, domain.ErrNotFound):
		ae = &apiError{status: http.StatusNotFound, code: "not_found", message: err.Error()}
	case errors.Is(err, domain.ErrConflict):
		ae = &apiError{status: http.StatusConflict, code: "conflict", message: err.Error()}
	case errors.Is(err, domain.ErrCapabilityUnsupported):
		ae = &apiError{status: http.StatusUnprocessableEntity, code: "capability_unsupported", message: err.Error()}
	case errors.Is(err, domain.ErrValidation):
		ae = &apiError{status: http.StatusUnprocessableEntity, code: "validation_failed", message: err.Error()}
	case errors.Is(err, domain.ErrRateLimited):
		ae = &apiError{status: http.StatusTooManyRequests, code: "rate_limited", message: "rate limited"}
	default:
		s.log.Error("request failed", "error", err)
		ae = &apiError{status: http.StatusInternalServerError, code: "internal", message: "internal error"}
	}
	s.writeError(w, ae)
}

func (s *server) writeError(w http.ResponseWriter, ae *apiError) {
	d := ae.details
	if d == nil {
		d = map[string]any{}
	}
	writeJSON(w, ae.status, errorBody{errorDetail{Code: ae.code, Message: ae.message, Details: d}})
}
