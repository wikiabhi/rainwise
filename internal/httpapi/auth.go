package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// equal compares secrets in constant time, whatever their lengths. An empty
// expected secret matches nothing, so an unset one can't open a route.
func equal(got, expected string) bool {
	if expected == "" {
		return false
	}
	a, b := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func (s *server) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	s.writeError(w, &apiError{status: http.StatusUnauthorized, code: "unauthorized", message: "missing or invalid credentials"})
}

// auth guards a route with the API token.
func (s *server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !equal(bearerToken(r), s.APIToken) {
			s.unauthorized(w)
			return
		}
		next(w, r)
	})
}

// webhookAuthorized checks the source's own secret, sent as a bearer token or in
// X-Rainwise-Secret. Both are compared, so timing doesn't tell which was sent.
func webhookAuthorized(r *http.Request, secret string) bool {
	a := equal(bearerToken(r), secret)
	b := equal(r.Header.Get("X-Rainwise-Secret"), secret)
	return a || b
}
