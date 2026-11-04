// Package contentfetch is a guarded ports.ContentFetcher. Bookmark URLs are
// untrusted, so connections to private networks are refused at dial time,
// which also covers redirects and DNS answers that point at private addresses.
package contentfetch

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/wikiabhi/rainwise/internal/domain"
)

const (
	defaultTimeout  = 10 * time.Second
	defaultMaxBytes = 2 << 20
	defaultMaxChars = 20000
	maxRedirects    = 5
)

var (
	errBlocked   = errors.New("address blocked")
	errScheme    = errors.New("unsupported url scheme")
	errNonText   = errors.New("content is not text")
	errPermanent = errors.New("permanent fetch failure")
)

// Options configures a Fetcher. Zero values take the defaults (10 s, 2 MB,
// 20,000 chars). AllowPrivate exists only for tests against httptest.
type Options struct {
	Timeout      time.Duration
	MaxBytes     int64
	MaxChars     int
	AllowPrivate bool
}

// Fetcher implements ports.ContentFetcher.
type Fetcher struct {
	client   *http.Client
	maxBytes int64
	maxChars int
}

// New builds a Fetcher.
func New(o Options) *Fetcher {
	if o.Timeout <= 0 {
		o.Timeout = defaultTimeout
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = defaultMaxBytes
	}
	if o.MaxChars <= 0 {
		o.MaxChars = defaultMaxChars
	}
	d := &net.Dialer{Timeout: o.Timeout}
	if !o.AllowPrivate {
		d.Control = guardControl
	}
	tr := &http.Transport{
		Proxy:                 nil, // never honor environment proxies; they would bypass the guard
		DialContext:           d.DialContext,
		TLSHandshakeTimeout:   o.Timeout,
		ResponseHeaderTimeout: o.Timeout,
		DisableKeepAlives:     true,
	}
	return &Fetcher{
		maxBytes: o.MaxBytes,
		maxChars: o.MaxChars,
		client: &http.Client{
			Timeout:   o.Timeout,
			Transport: tr,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > maxRedirects {
					return fmt.Errorf("%w: too many redirects", errPermanent)
				}
				return checkScheme(req.URL)
			},
		},
	}
}

func checkScheme(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: %q", errScheme, u.Scheme)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("%w: missing host", errScheme)
	}
	return nil
}

// guardControl is a net.Dialer.Control function. It runs after DNS
// resolution with the concrete IP about to be dialed.
func guardControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %v", errBlocked, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: %v", errBlocked, err)
	}
	if blockedIP(ip) {
		return fmt.Errorf("%w: %s", errBlocked, ip)
	}
	return nil
}

var nat64 = netip.MustParsePrefix("64:ff9b::/96")

func blockedIP(ip netip.Addr) bool {
	ip = ip.WithZone("")
	if ip.Is4In6() {
		ip = ip.Unmap()
	}
	if nat64.Contains(ip) { // embedded IPv4 in the low 32 bits
		b := ip.As16()
		return blockedIP(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	if ip.Is4() {
		b := ip.As4()
		if b[0] == 100 && b[1]&0xc0 == 64 { // 100.64.0.0/10 carrier-grade NAT
			return true
		}
	}
	return false
}

// Fetch returns the readable text of the page at rawURL.
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errPermanent, err)
	}
	if err := checkScheme(u); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errPermanent, err)
	}
	req.Header.Set("User-Agent", "rainwise/1.0 (+bookmark organizer)")
	req.Header.Set("Accept", "text/html,text/plain;q=0.9,*/*;q=0.1")
	resp, err := f.client.Do(req)
	if err != nil {
		return "", classify(ctx, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode >= 500:
		return "", fmt.Errorf("%w: http status %d", domain.ErrTemporary, resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return "", fmt.Errorf("%w: http status %d", errPermanent, resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	isHTML := true
	if ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil {
			return "", fmt.Errorf("%w: bad content type %q", errNonText, ct)
		}
		switch {
		case mt == "text/html" || mt == "application/xhtml+xml":
		case strings.HasPrefix(mt, "text/"):
			isHTML = false
		default:
			return "", fmt.Errorf("%w: %s", errNonText, mt)
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes))
	if err != nil {
		return "", classify(ctx, err)
	}
	text := strings.ToValidUTF8(string(body), "")
	if isHTML {
		text = extractText(text)
	} else {
		text = strings.Join(strings.Fields(text), " ")
	}
	if utf8.RuneCountInString(text) > f.maxChars {
		text = string([]rune(text)[:f.maxChars])
	}
	return text, nil
}

func classify(ctx context.Context, err error) error {
	if errors.Is(err, errBlocked) || errors.Is(err, errScheme) || errors.Is(err, errPermanent) {
		return fmt.Errorf("%w: %v", errPermanent, err)
	}
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return fmt.Errorf("%w: %v", domain.ErrTemporary, err)
}

// extractText strips tags, drops script, style and noscript content, decodes
// entities and collapses whitespace.
func extractText(s string) string {
	var b strings.Builder
	lower := asciiLower(s)
	for i := 0; i < len(s); {
		lt := strings.IndexByte(s[i:], '<')
		if lt < 0 {
			b.WriteString(s[i:])
			break
		}
		b.WriteString(s[i : i+lt])
		i += lt
		if strings.HasPrefix(s[i:], "<!--") {
			end := strings.Index(s[i+4:], "-->")
			if end < 0 {
				break
			}
			i += 4 + end + 3
			b.WriteByte(' ')
			continue
		}
		gt := strings.IndexByte(s[i:], '>')
		if gt < 0 {
			break
		}
		tag := lower[i+1 : i+gt]
		i += gt + 1
		b.WriteByte(' ')
		name := tagName(tag)
		switch name {
		case "script", "style", "noscript":
			if end := strings.Index(lower[i:], "</"+name); end >= 0 {
				i += end
			} else {
				i = len(s)
			}
		}
	}
	return strings.Join(strings.Fields(html.UnescapeString(b.String())), " ")
}

func tagName(tag string) string {
	if strings.HasPrefix(tag, "/") {
		return "/"
	}
	end := strings.IndexAny(tag, " \t\r\n/")
	if end < 0 {
		return tag
	}
	return tag[:end]
}

// asciiLower keeps byte offsets identical to s, which strings.ToLower does not
// guarantee for some non-ASCII runes.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}
