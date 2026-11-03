// Package httpapi is the REST API (§7).
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/events"
	"github.com/wikiabhi/rainwise/internal/core/jobs"
	"github.com/wikiabhi/rainwise/internal/core/taxonomy"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	maxBody      = 1 << 20
	defaultLimit = 50
	maxLimit     = 200
)

// ProviderInfo describes a configured provider for GET /v1/providers.
type ProviderInfo struct {
	Name         string
	Capabilities ports.ProviderCapabilities
}

// ClassifierInfo describes a configured classifier for GET /v1/classifiers.
type ClassifierInfo struct {
	Name         string
	Roles        []string
	Capabilities ports.ClassifierCapabilities
}

type Deps struct {
	APIToken string
	// WebhookSecrets maps a provider to the secret its webhook must present.
	WebhookSecrets map[string]string
	Normalizers    map[string]ports.EventNormalizer
	Jobs           *jobs.Service
	Taxonomies     *taxonomy.Service
	Snapshots      *jobs.SnapshotService
	Ingestor       *events.Ingestor
	Store          ports.Store
	Providers      []ProviderInfo
	Classifiers    []ClassifierInfo
	// DefaultProvider and DefaultTaxonomy fill what a job request omits. A
	// snapshot or restore never gets a taxonomy.
	DefaultProvider string
	DefaultTaxonomy string
	Log             *slog.Logger
}

type server struct {
	Deps
	log *slog.Logger
}

// New builds the API handler.
func New(d Deps) http.Handler {
	s := &server{Deps: d, log: d.Log}
	if s.log == nil {
		s.log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /v1/webhooks/{provider}", s.webhook)

	api := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, s.auth(h)) }
	api("POST /v1/jobs", s.createJob)
	api("GET /v1/jobs", s.listJobs)
	api("GET /v1/jobs/{id}", s.getJob)
	api("DELETE /v1/jobs/{id}", s.cancelJob)
	api("GET /v1/jobs/{id}/decisions", s.jobDecisions)
	api("GET /v1/jobs/{id}/proposed-tags", s.proposedTags)
	api("GET /v1/providers", s.providers)
	api("GET /v1/classifiers", s.classifiers)
	api("GET /v1/taxonomies", s.listTaxonomies)
	api("GET /v1/taxonomies/{id}", s.getTaxonomy)
	api("PUT /v1/taxonomies/{id}", s.putTaxonomy)
	api("GET /v1/taxonomies/{id}/proposals", s.proposals)
	api("GET /v1/snapshots", s.listSnapshots)
	api("GET /v1/snapshots/{id}", s.getSnapshot)
	api("GET /v1/snapshots/{id}/export", s.exportSnapshot)

	return s.logRequests(s.recoverer(s.shapeMisses(mux)))
}

// shapeMisses gives the mux's own 404 and 405 answers the API's error shape.
func (s *server) shapeMisses(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		p := &probe{header: http.Header{}, status: http.StatusOK}
		h.ServeHTTP(p, r)
		switch p.status {
		case http.StatusMethodNotAllowed:
			w.Header().Set("Allow", p.header.Get("Allow"))
			s.writeError(w, &apiError{status: p.status, code: "method_not_allowed", message: "method not allowed"})
		case http.StatusNotFound:
			s.writeError(w, &apiError{status: p.status, code: "not_found", message: "no such route"})
		default: // a redirect to the cleaned path
			for k, v := range p.header {
				w.Header()[k] = v
			}
			w.WriteHeader(p.status)
		}
	})
}

// probe captures the status and headers of the mux's miss handlers.
type probe struct {
	header http.Header
	status int
}

func (p *probe) Header() http.Header         { return p.header }
func (p *probe) Write(b []byte) (int, error) { return len(b), nil }
func (p *probe) WriteHeader(code int)        { p.status = code }

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// logRequests logs method, path, status, and duration. It never logs headers or
// the query, which may carry a cursor.
func (s *server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		s.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration", time.Since(start).String())
	})
}

func (s *server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler {
				panic(v)
			}
			s.log.Error("panic in handler", "panic", v, "stack", string(debug.Stack()))
			s.writeError(w, &apiError{status: http.StatusInternalServerError, code: "internal", message: "internal error"})
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.Store.Jobs().ListJobs(ctx, "", 1); err != nil {
		s.log.Error("health check", "error", err)
		s.writeError(w, &apiError{status: http.StatusServiceUnavailable, code: "unavailable", message: "store unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, `{"error":{"code":"internal","message":"internal error","details":{}}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

// decodeBody reads one JSON value, rejecting unknown fields and trailing data.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return bodyError(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return bodyError(err)
		}
		return badRequest("unexpected data after the JSON body")
	}
	return nil
}

// page is a list response. Items is never null.
type page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func newPage[T any](items []T, next string) page[T] {
	if items == nil {
		items = []T{}
	}
	p := page[T]{Items: items}
	if next != "" {
		p.NextCursor = &next
	}
	return p
}

// listParams reads limit and cursor.
func listParams(r *http.Request) (cursor string, limit int, err error) {
	limit = defaultLimit
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 1 {
			return "", 0, badRequest("limit must be a positive integer")
		}
		limit = min(n, maxLimit)
	}
	return q.Get("cursor"), limit, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
