package contentfetch

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

var _ ports.ContentFetcher = (*Fetcher)(nil)

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

func TestFetchExtractsText(t *testing.T) {
	s := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<html><head><title>Hi</title><style>p{color:red}</style>
<script>var secret = "x";</script></head><body><h1>Hello</h1><p>World &amp; <b>more</b></p>
<noscript>nope</noscript></body></html>`))
	})
	got, err := New(Options{AllowPrivate: true}).Fetch(context.Background(), s.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got != "Hi Hello World & more" {
		t.Fatalf("got %q", got)
	}
}

func TestFetchBlocksPrivate(t *testing.T) {
	var hits atomic.Int32
	s := serve(t, func(w http.ResponseWriter, r *http.Request) { hits.Add(1) })
	_ = s
	for _, u := range []string{
		"http://127.0.0.1:1/", "http://10.0.0.1/", "http://169.254.169.254/latest",
		"http://[::1]/", "http://[::ffff:127.0.0.1]/", "http://[fe80::1]/", "http://[fd00::1]/",
		"file:///etc/passwd", "ftp://x/",
	} {
		_, err := New(Options{Timeout: 2 * time.Second}).Fetch(context.Background(), u)
		if err == nil {
			t.Errorf("%s: expected error", u)
			continue
		}
		if errors.Is(err, domain.ErrTemporary) {
			t.Errorf("%s: want permanent, got %v", u, err)
		}
	}
	if hits.Load() != 0 {
		t.Fatal("connection reached server")
	}
	// A live loopback server is blocked too.
	_, err := New(Options{}).Fetch(context.Background(), s.URL)
	if err == nil || errors.Is(err, domain.ErrTemporary) || hits.Load() != 0 {
		t.Fatalf("loopback server: err=%v hits=%d", err, hits.Load())
	}
}

func TestFetchBlocksRedirectToPrivate(t *testing.T) {
	// Redirect hops use the same dialer, so the dial-time guard covers them.
	for _, a := range []string{
		"127.0.0.1:80", "10.1.2.3:80", "172.16.0.1:80", "192.168.1.1:80", "169.254.169.254:80",
		"[::1]:80", "[fe80::1]:80", "[fc00::1]:80", "[::ffff:10.0.0.1]:80", "[::ffff:169.254.169.254]:80",
		"0.0.0.0:80", "100.64.0.1:80",
	} {
		if err := guardControl("tcp", a, nil); !errors.Is(err, errBlocked) {
			t.Errorf("%s: want errBlocked, got %v", a, err)
		}
	}
	for _, a := range []string{"93.184.216.34:80", "[2606:2800:220:1::1]:80"} {
		if err := guardControl("tcp", a, nil); err != nil {
			t.Errorf("%s: unexpected %v", a, err)
		}
	}
	// A redirect to a non-http scheme is not followed.
	var hits atomic.Int32
	s := serve(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "ftp://example.com/x", http.StatusFound)
	})
	_, err := New(Options{AllowPrivate: true}).Fetch(context.Background(), s.URL)
	if err == nil || errors.Is(err, domain.ErrTemporary) || hits.Load() != 1 {
		t.Fatalf("err=%v hits=%d", err, hits.Load())
	}
}

func TestFetchErrorKinds(t *testing.T) {
	cases := []struct {
		name string
		code int
		temp bool
	}{{"500", 500, true}, {"503", 503, true}, {"429", 429, true}, {"404", 404, false}, {"403", 403, false}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := serve(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(c.code) })
			_, err := New(Options{AllowPrivate: true}).Fetch(context.Background(), s.URL)
			if err == nil || errors.Is(err, domain.ErrTemporary) != c.temp {
				t.Fatalf("err=%v temp=%v", err, c.temp)
			}
		})
	}
	t.Run("timeout", func(t *testing.T) {
		release := make(chan struct{})
		s := serve(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		t.Cleanup(func() { close(release) })
		_, err := New(Options{AllowPrivate: true, Timeout: 50 * time.Millisecond}).Fetch(context.Background(), s.URL)
		if !errors.Is(err, domain.ErrTemporary) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("connection refused", func(t *testing.T) {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := l.Addr().String()
		_ = l.Close()
		_, err := New(Options{AllowPrivate: true}).Fetch(context.Background(), "http://"+addr)
		if !errors.Is(err, domain.ErrTemporary) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestFetchSizeCap(t *testing.T) {
	s := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(strings.Repeat("a", 3<<20)))
	})
	got, err := New(Options{AllowPrivate: true, MaxChars: 1 << 30}).Fetch(context.Background(), s.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2<<20 {
		t.Fatalf("len=%d", len(got))
	}
	got, err = New(Options{AllowPrivate: true}).Fetch(context.Background(), s.URL)
	if err != nil || len([]rune(got)) != 20000 {
		t.Fatalf("chars=%d err=%v", len([]rune(got)), err)
	}
}

func TestFetchNonText(t *testing.T) {
	s := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("%PDF-1.4"))
	})
	_, err := New(Options{AllowPrivate: true}).Fetch(context.Background(), s.URL)
	if err == nil || errors.Is(err, domain.ErrTemporary) {
		t.Fatalf("err=%v", err)
	}
}
