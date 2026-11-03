// Package e2e drives the whole service through its API, with a fake provider and
// classifier and a real SQLite file. It is the definition of done from §16.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pfake "github.com/wikiabhi/rainwise/internal/adapters/provider/fake"
	"github.com/wikiabhi/rainwise/internal/adapters/store/sqlite"
	"github.com/wikiabhi/rainwise/internal/app"
	"github.com/wikiabhi/rainwise/internal/config"
	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const (
	apiToken   = "api-token"
	hookSecret = "hook-secret"
	testWait   = 60 * time.Second
	longText   = "A long and detailed description of the subject, written to be classified."
)

func TestMain(m *testing.M) {
	// Tests poll the API, and every request logs a line.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	os.Exit(m.Run())
}

const configYAML = `
server: {addr: "127.0.0.1:0", api_token: ${API}}
state: {sqlite_path: ${DB}}
automation: {enabled: ${AUTOMATION}}
processing: {provider: fake, taxonomy: default, concurrency: 1, content_fetch: off}
providers:
  fake: {type: fake, webhook_secret: ${HOOK}}
classifiers:
  test: {type: fake}
roles: {classifier: test, proposer: test, fallback: null}
taxonomies:
  default:
    mode: fixed
    provision_missing_collections: true
    rules: {min_tags: 0, max_tags: 5, allow_new_tags: true}
    collections:
      - {key: tech, name: Tech}
      - {key: food, name: Food}
      - {key: review, name: Needs Review, role: review}
`

var (
	inbox  = pfake.Inbox
	colOld = cid("old")
	colTec = cid("tech")
	colFoo = cid("food")
	colRev = cid("review")
)

func cid(local string) domain.ID { return domain.ID{Provider: "fake", Local: local} }

// classifier answers from the bookmark's title: "tech: ..." goes to tech. It
// counts calls per bookmark, and block, if set, runs during each call.
type classifier struct {
	mu    sync.Mutex
	calls map[domain.ID]int
	block func(ctx context.Context, id domain.ID) error
}

func newClassifier() *classifier { return &classifier{calls: map[domain.ID]int{}} }

func (c *classifier) Name() string { return "test" }

func (c *classifier) Capabilities() ports.ClassifierCapabilities {
	return ports.ClassifierCapabilities{FixedChoice: true, ProposeTags: true, CalibratedConfidence: true, MaxChoices: 100}
}

func (c *classifier) Classify(ctx context.Context, r ports.ClassifyRequest) (domain.Decision, error) {
	id := r.Subject.Bookmark.ID
	c.mu.Lock()
	c.calls[id]++
	block := c.block
	c.mu.Unlock()
	if block != nil {
		if err := block(ctx, id); err != nil {
			return domain.Decision{}, err
		}
	}
	key, _, _ := strings.Cut(r.Subject.Bookmark.Title, ":")
	d := domain.Decision{Confidence: 0.9, Model: domain.ModelInfo{Classifier: "test", Model: "m"}}
	if r.WantCollection {
		d.Collection.Key = key
	}
	if r.TagsWanted > 0 {
		d.Tags = []domain.TagSuggestion{{Name: "about-" + key}}
	}
	return d, nil
}

func (c *classifier) setBlock(f func(context.Context, domain.ID) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.block = f
}

func (c *classifier) callsOf(id domain.ID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[id]
}

func (c *classifier) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

// env is one library and one database, across however many App lifetimes.
type env struct {
	t        *testing.T
	path     string
	cfgPath  string
	vals     map[string]string
	p        *pfake.Provider
	c        *classifier
	app      *app.App
	stopApp  context.CancelFunc
	store    *sqlite.Store
	db       *sql.DB
	nextHook int
	// classifierOverride and stopWait apply from the next boot.
	classifierOverride ports.Classifier
	stopWait           time.Duration
}

func newEnv(t *testing.T, automation bool) *env {
	t.Helper()
	dir := t.TempDir()
	e := &env{t: t, path: filepath.Join(dir, "rainwise.db"), cfgPath: filepath.Join(dir, "config.yaml"), p: pfake.New(), c: newClassifier()}
	e.vals = map[string]string{"API": apiToken, "HOOK": hookSecret, "DB": e.path, "AUTOMATION": fmt.Sprint(automation)}
	if err := os.WriteFile(e.cfgPath, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	e.boot()

	// A second handle on the file, to read what the API doesn't show.
	var err error
	if e.store, err = sqlite.Open(context.Background(), e.path); err != nil {
		t.Fatal(err)
	}
	if e.db, err = sql.Open("sqlite", e.path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.shutdown()
		e.db.Close()
		e.store.Close()
	})
	return e
}

// boot builds and starts a new App on the file.
func (e *env) boot() {
	e.t.Helper()
	cfg, err := config.Load(e.cfgPath, func(k string) string { return e.vals[k] })
	if err != nil {
		e.t.Fatal(err)
	}
	var c ports.Classifier = e.c
	if e.classifierOverride != nil {
		c = e.classifierOverride
	}
	a, err := app.Build(cfg, app.BuildDeps{Provider: e.p, Classifier: c, PollEvery: time.Millisecond, StopWait: e.stopWait})
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	e.app, e.stopApp = a, cancel
}

func (e *env) shutdown() {
	e.t.Helper()
	if err := e.stop(); err != nil {
		e.t.Fatalf("Shutdown: %v", err)
	}
}

// stop is shutdown for any goroutine: it leaves failing the test to the caller.
func (e *env) stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	err := e.app.Shutdown(ctx)
	e.stopApp()
	return err
}

func (e *env) do(method, path, body string, hdr ...string) (int, []byte) {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiToken)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	e.app.Handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

type jobView struct {
	ID         string             `json:"id"`
	Status     domain.JobStatus   `json:"status"`
	Error      *string            `json:"error"`
	SnapshotID *string            `json:"snapshot_id"`
	Progress   domain.JobProgress `json:"progress"`
}

func (e *env) createJob(body string) jobView {
	e.t.Helper()
	code, out := e.do(http.MethodPost, "/v1/jobs", body)
	if code != http.StatusAccepted {
		e.t.Fatalf("POST /v1/jobs %s = %d %s", body, code, out)
	}
	return decode[jobView](e.t, out)
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %T from %q: %v", v, b, err)
	}
	return v
}

// waitJob spins rather than sleeps; tests never really sleep.
func (e *env) waitJob(id string) jobView {
	e.t.Helper()
	deadline := time.Now().Add(testWait)
	for {
		code, out := e.do(http.MethodGet, "/v1/jobs/"+id, "")
		if code != http.StatusOK {
			e.t.Fatalf("GET job = %d %s", code, out)
		}
		j := decode[jobView](e.t, out)
		switch j.Status {
		case domain.JobSucceeded, domain.JobSucceededWithErrors, domain.JobFailed, domain.JobCanceled:
			return j
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("job %s stuck: %s", id, out)
		}
		runtime.Gosched()
	}
}

// run creates a job and requires it to succeed with no failed item.
func (e *env) run(body string) jobView {
	e.t.Helper()
	j := e.waitJob(e.createJob(body).ID)
	if j.Status != domain.JobSucceeded {
		e.t.Fatalf("job %s = %s, error %v, progress %+v", body, j.Status, j.Error, j.Progress)
	}
	return j
}

// hook posts one event to the webhook route and returns its dedup key.
func (e *env) hook(typ, local string) string {
	e.t.Helper()
	e.nextHook++
	key := fmt.Sprintf("event-%d", e.nextHook)
	body := fmt.Sprintf(`{"type":%q,"resource":{"id":%q},"occurred_at":"2026-11-03T10:00:00Z","event_id":%q}`, typ, local, key)
	code, out := e.do(http.MethodPost, "/v1/webhooks/fake", body, "Authorization", "", "X-Rainwise-Secret", hookSecret)
	if code != http.StatusAccepted {
		e.t.Fatalf("webhook = %d %s", code, out)
	}
	return key
}

func (e *env) eventStatus(key string) string {
	e.t.Helper()
	var status string
	if err := e.db.QueryRow(`SELECT status FROM events WHERE dedup_key = ?`, key).Scan(&status); err != nil {
		e.t.Fatalf("event %s: %v", key, err)
	}
	return status
}

// waitEvent waits for the worker to finish an event, which must succeed.
func (e *env) waitEvent(key string) {
	e.t.Helper()
	deadline := time.Now().Add(testWait)
	for {
		switch s := e.eventStatus(key); s {
		case domain.EventDone:
			return
		case domain.EventFailed:
			e.t.Fatalf("event %s failed", key)
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("event %s is still %s", key, e.eventStatus(key))
		}
		runtime.Gosched()
	}
}

// restartAndWaitForWorker restarts the App, and returns once its event worker has
// been at the inbox. The worker drops stale held events first thing and claims due
// ones straight after, so a stale one going shows it got that far. The deterministic
// test that a tick leaves events alone is in the events package; this one checks
// the wiring. It leaves a new App running.
func (e *env) restartAndWaitForWorker() {
	e.t.Helper()
	e.shutdown()
	stale := time.Now().Add(-8 * 24 * time.Hour)
	marker := domain.StoredEvent{
		Event: domain.Event{Provider: "fake", Type: domain.EventBookmarkMoved, Resource: domain.ResourceRef{ID: "no-such-bookmark"}},
		ID:    "stale", DedupKey: "stale", Status: domain.EventHeld, NextAttempt: stale, ReceivedAt: stale,
	}
	if _, err := e.store.Events().InsertEvent(context.Background(), marker); err != nil {
		e.t.Fatal(err)
	}
	e.boot()
	deadline := time.Now().Add(testWait)
	for {
		var n int
		if err := e.db.QueryRow(`SELECT count(*) FROM events WHERE dedup_key = 'stale'`).Scan(&n); err != nil {
			e.t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			e.t.Fatal("the worker never dropped the stale event")
		}
		runtime.Gosched()
	}
	e.shutdown()
	e.boot()
}

func (e *env) addCollections(ids ...domain.ID) {
	names := map[domain.ID]string{inbox: "Unsorted", colOld: "Old", colTec: "Tech", colFoo: "Food", colRev: "Needs Review"}
	for _, id := range ids {
		e.p.AddCollection(domain.Collection{ID: id, Name: names[id]})
	}
}

func (e *env) add(local, title string, col domain.ID, tags []string) domain.ID {
	id := cid(local)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.p.Add(domain.Bookmark{ID: id, URL: "https://example.test/" + local, Title: title, Description: longText,
		Collection: col, Tags: tags, CreatedAt: at, UpdatedAt: at})
	return id
}

func (e *env) bookmark(id domain.ID) domain.Bookmark {
	e.t.Helper()
	b, ok := e.p.Bookmark(id)
	if !ok {
		e.t.Fatalf("bookmark %s is gone", id)
	}
	return b
}

func (e *env) wantIn(id, col domain.ID) {
	e.t.Helper()
	if got := e.bookmark(id).Collection; got != col {
		e.t.Fatalf("%s is in %s, want %s", id, got, col)
	}
}

func (e *env) pinOf(id domain.ID) domain.PinSource {
	e.t.Helper()
	st, ok, err := e.store.BookmarkStates().GetState(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	if !ok {
		e.t.Fatalf("no state for %s", id)
	}
	return st.PinSource
}

func (e *env) writesTo(id domain.ID) []pfake.Write {
	var out []pfake.Write
	for _, w := range e.p.Writes() {
		if w.ID == id {
			out = append(out, w)
		}
	}
	return out
}

func (e *env) moveWrites(id domain.ID) int {
	n := 0
	for _, w := range e.writesTo(id) {
		if w.Patch.Collection != nil {
			n++
		}
	}
	return n
}

func (e *env) wantNoSetTags() {
	e.t.Helper()
	for _, w := range e.p.Writes() {
		if w.Patch.SetTags != nil {
			e.t.Fatalf("a non-restore write used SetTags: %+v", w)
		}
	}
}

func TestDryRunThenApplyThenRestore(t *testing.T) {
	e := newEnv(t, true)
	// Food doesn't exist yet, so the applied job has to create it, and the restore
	// has to take it away again.
	e.addCollections(inbox, colOld, colTec, colRev)
	ids := []domain.ID{
		e.add("b1", "tech: nil tags", inbox, nil),
		e.add("b2", "food: two tags", colOld, []string{"keep", "Mixed Case"}),
		e.add("b3", "tech: empty tags", colOld, []string{}),
		e.add("b4", "food: one tag", inbox, []string{"keep"}),
	}
	before := map[domain.ID]domain.Bookmark{}
	for _, id := range ids {
		before[id] = e.bookmark(id)
	}
	wantAsBefore := func() {
		t.Helper()
		for _, id := range ids {
			got, want := e.bookmark(id), before[id]
			if got.Collection != want.Collection || !slices.Equal(got.Tags, want.Tags) {
				t.Fatalf("%s: %s %q, want %s %q", id, got.Collection, got.Tags, want.Collection, want.Tags)
			}
		}
	}

	// A dry run: a reviewable plan and no provider writes.
	plan := e.run(`{"type":"backfill","dry_run":true}`)
	if n := len(e.p.Writes()); n != 0 {
		t.Fatalf("the dry run wrote %d times", n)
	}
	wantAsBefore()
	cols, err := e.p.ListCollections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 {
		t.Fatalf("the dry run changed the collections: %+v", cols)
	}
	code, out := e.do(http.MethodGet, "/v1/jobs/"+plan.ID+"/decisions", "")
	if code != http.StatusOK {
		t.Fatalf("decisions = %d %s", code, out)
	}
	type decision struct {
		Bookmark string  `json:"bookmark"`
		Target   *string `json:"target_collection"`
	}
	dec := decode[struct {
		Items []decision `json:"items"`
	}](t, out)
	if len(dec.Items) != 4 {
		t.Fatalf("decisions = %s", out)
	}
	for _, d := range dec.Items {
		if d.Bookmark == "fake:b1" && (d.Target == nil || *d.Target != colTec.String()) {
			t.Fatalf("b1's plan = %+v", d)
		}
		if d.Bookmark == "fake:b2" && (d.Target == nil || !strings.Contains(*d.Target, "planned:food")) {
			t.Fatalf("b2's plan = %+v", d)
		}
	}
	code, out = e.do(http.MethodGet, "/v1/jobs/"+plan.ID+"/proposed-tags", "")
	if code != http.StatusOK || !strings.Contains(string(out), "about-tech") {
		t.Fatalf("proposed tags = %d %s", code, out)
	}

	// Apply: the library is sorted, adding only the approved tags.
	applied := e.run(fmt.Sprintf(`{"type":"backfill","from_dry_run":%q,"params":{"approved_new_tags":{"about-tech":"about-tech"}}}`, plan.ID))
	if applied.SnapshotID == nil {
		t.Fatal("a real backfill must take a snapshot")
	}
	created := map[string]domain.ID{}
	cols, err = e.p.ListCollections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cols {
		created[c.Name] = c.ID
	}
	food, ok := created["Food"]
	if !ok {
		t.Fatalf("Food was not provisioned: %+v", cols)
	}
	e.wantIn(ids[0], colTec)
	e.wantIn(ids[1], food)
	e.wantIn(ids[2], colTec)
	e.wantIn(ids[3], food)
	if got := e.bookmark(ids[0]).Tags; !slices.Equal(got, []string{"about-tech"}) {
		t.Fatalf("b1 tags = %q", got)
	}
	if got := e.bookmark(ids[1]).Tags; !slices.Equal(got, []string{"keep", "Mixed Case"}) {
		t.Fatalf("b2 tags = %q (about-food was not approved)", got)
	}
	e.wantNoSetTags()

	// The snapshot holds what the library held, through its trip via SQLite.
	items, err := e.store.Snapshots().ListSnapshotItems(context.Background(), *applied.SnapshotID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items.Items) != 4 {
		t.Fatalf("snapshot items = %+v", items.Items)
	}
	for _, it := range items.Items {
		if want := before[it.Bookmark]; it.Collection != want.Collection || !slices.Equal(it.Tags, want.Tags) {
			t.Fatalf("snapshot item %+v, want %+v", it, want)
		}
	}
	code, out = e.do(http.MethodGet, "/v1/snapshots/"+*applied.SnapshotID+"/export", "")
	if code != http.StatusOK || !bytes.Contains(out, []byte(`"b1"`)) {
		t.Fatalf("export = %d %s", code, out)
	}

	// A restore dry run shows the differences and changes nothing.
	writes := len(e.p.Writes())
	preview := e.run(fmt.Sprintf(`{"type":"restore","dry_run":true,"params":{"snapshot_id":%q}}`, *applied.SnapshotID))
	if n := len(e.p.Writes()); n != writes {
		t.Fatalf("the restore dry run wrote %d times", n-writes)
	}
	code, out = e.do(http.MethodGet, "/v1/jobs/"+preview.ID+"/decisions", "")
	if code != http.StatusOK {
		t.Fatalf("restore decisions = %d %s", code, out)
	}
	type diff struct {
		Bookmark string   `json:"bookmark"`
		ToTags   []string `json:"to_tags"`
		To       *string  `json:"to_collection"`
	}
	diffs := decode[struct {
		Items []diff `json:"items"`
	}](t, out)
	if len(diffs.Items) != 4 {
		t.Fatalf("restore diffs = %s", out)
	}
	for _, d := range diffs.Items {
		if d.Bookmark == "fake:b1" && (d.ToTags == nil || len(d.ToTags) != 0 || d.To == nil || *d.To != inbox.String()) {
			t.Fatalf("b1's diff = %+v in %s", d, out)
		}
	}

	// The real restore: the snapshot, exactly.
	restored := e.run(fmt.Sprintf(`{"type":"restore","params":{"snapshot_id":%q}}`, *applied.SnapshotID))
	wantAsBefore()
	if restored.SnapshotID == nil || *restored.SnapshotID == *applied.SnapshotID {
		t.Fatalf("a restore must take its own snapshot: %+v", restored)
	}
	cols, err = e.p.ListCollections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(cols, func(c domain.Collection) bool { return c.ID == food }) {
		t.Fatalf("the created collection should be gone: %+v", cols)
	}
	for _, w := range e.p.Writes()[writes:] {
		if w.Patch.SetTags == nil {
			t.Fatalf("restore wrote without exact tags: %+v", w)
		}
	}

	// The restore doesn't look like manual moves, and sync leaves it alone.
	writes, calls := len(e.p.Writes()), e.c.total()
	e.run(`{"type":"sync"}`)
	if n := len(e.p.Writes()); n != writes {
		t.Fatalf("sync after a restore wrote %d times", n-writes)
	}
	if n := e.c.total(); n != calls {
		t.Fatalf("sync after a restore classified %d times", n-calls)
	}
	wantAsBefore()
}

// A library with no collections to list, and tags only ever set from nothing: the
// nil slices must survive SQLite on the way to a restore.
func TestRestoreRoundTripsNilValues(t *testing.T) {
	e := newEnv(t, false)
	e.p.SetCapabilities(ports.ProviderCapabilities{InboxCollection: &inbox})
	id := e.add("b1", "tech: nil tags", inbox, nil)

	snap := e.run(`{"type":"snapshot"}`)
	if snap.SnapshotID == nil {
		t.Fatalf("no snapshot: %+v", snap)
	}
	s, err := e.store.Snapshots().GetSnapshot(context.Background(), *snap.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Collections) != 0 || len(s.CreatedCollections) != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
	b := e.bookmark(id)
	b.Tags = []string{"added-later"}
	e.p.Add(b)

	e.run(fmt.Sprintf(`{"type":"restore","params":{"snapshot_id":%q}}`, *snap.SnapshotID))
	if got := e.bookmark(id).Tags; len(got) != 0 {
		t.Fatalf("tags = %q, want none", got)
	}
	writes := e.writesTo(id)
	if len(writes) != 1 || writes[0].Patch.SetTags == nil {
		t.Fatalf("writes = %+v: empty tags must be set, not skipped", writes)
	}
}

func TestManualMoveSurvives(t *testing.T) {
	tests := []struct {
		name    string
		noticed func(e *env, id domain.ID)
	}{
		{"noticed by sync", func(e *env, _ domain.ID) { e.run(`{"type":"sync"}`) }},
		{"noticed by event", func(e *env, id domain.ID) { e.waitEvent(e.hook("bookmark.moved", id.Local)) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, true)
			e.addCollections(inbox, colTec, colFoo, colRev)
			id := e.add("b1", "tech: moved by hand", inbox, nil)
			other := e.add("b2", "food: left alone", inbox, nil)

			e.run(`{"type":"backfill"}`)
			e.wantIn(id, colTec)
			e.wantIn(other, colFoo)

			e.p.SetCollection(id, colFoo)
			tc.noticed(e, id)
			if pin := e.pinOf(id); pin != domain.PinUser {
				t.Fatalf("pin = %q, want user", pin)
			}

			moves := e.moveWrites(id)
			check := func(step string) {
				t.Helper()
				e.wantIn(id, colFoo)
				if n := e.moveWrites(id); n != moves {
					t.Fatalf("%s moved the bookmark back: %+v", step, e.writesTo(id))
				}
			}
			e.run(`{"type":"sync"}`)
			check("sync")
			e.waitEvent(e.hook("bookmark.created", id.Local))
			check("a created event")
			e.waitEvent(e.hook("bookmark.moved", id.Local))
			check("a moved event")
			// Forced, so the classifier really is asked and says tech: only the pin
			// can keep the bookmark where the user put it.
			calls := e.c.callsOf(id)
			e.run(`{"type":"backfill","params":{"force":true}}`)
			check("a backfill")
			if n := e.c.callsOf(id); n == calls {
				t.Fatal("the forced backfill never asked the classifier, so proves nothing")
			}
			e.run(`{"type":"sync"}`)
			check("a later sync")
			if pin := e.pinOf(id); pin != domain.PinUser {
				t.Fatalf("pin = %q at the end", pin)
			}
			e.wantNoSetTags()
		})
	}
}

func TestInboxFiledBySync(t *testing.T) {
	e := newEnv(t, true)
	e.addCollections(inbox, colOld, colTec, colFoo, colRev)
	old := e.add("old", "tech: already filed somewhere", colOld, []string{"mine"})
	first := e.add("first", "tech: in the inbox from the start", inbox, nil)

	// The first sync sets the baseline: only the inbox moves.
	e.run(`{"type":"sync"}`)
	e.wantIn(first, colTec)
	e.wantIn(old, colOld)
	if n := len(e.writesTo(old)); n != 0 {
		t.Fatalf("baseline bookmark written: %+v", e.writesTo(old))
	}
	if n := e.c.callsOf(old); n != 0 {
		t.Fatalf("baseline bookmark classified %d times", n)
	}

	fresh := e.add("fresh", "food: saved just now", inbox, nil)
	e.run(`{"type":"sync"}`)
	e.wantIn(fresh, colFoo)
	if got := e.bookmark(fresh).Tags; !slices.Equal(got, []string{"about-food"}) {
		t.Fatalf("tags = %q", got)
	}
	e.wantIn(old, colOld)

	// A sync with nothing new does nothing.
	writes, calls := len(e.p.Writes()), e.c.total()
	e.run(`{"type":"sync"}`)
	if n := len(e.p.Writes()); n != writes {
		t.Fatalf("an idle sync wrote %d times", n-writes)
	}
	if n := e.c.total(); n != calls {
		t.Fatalf("an idle sync classified %d times", n-calls)
	}
	e.wantNoSetTags()
}

func TestInboxFiledByEvent(t *testing.T) {
	e := newEnv(t, true)
	e.addCollections(inbox, colTec, colFoo, colRev)
	id := e.add("b1", "food: saved just now", inbox, nil)

	key := e.hook("bookmark.created", id.Local)
	e.waitEvent(key)
	e.wantIn(id, colFoo)
	if got := e.bookmark(id).Tags; !slices.Equal(got, []string{"about-food"}) {
		t.Fatalf("tags = %q", got)
	}
	if n := e.c.callsOf(id); n != 1 {
		t.Fatalf("classified %d times", n)
	}
	e.wantNoSetTags()

	// Without the secret, nothing gets in.
	code, _ := e.do(http.MethodPost, "/v1/webhooks/fake", `{"type":"bookmark.created","resource":{"id":"b1"}}`, "Authorization", "Bearer "+apiToken)
	if code != http.StatusUnauthorized {
		t.Fatalf("webhook with the API token = %d", code)
	}
}

func TestKillAndResume(t *testing.T) {
	const total = 12
	tests := []struct {
		name   string
		killed bool // Whether the call in flight is cancelled or left to finish.
	}{
		{"graceful stop", false},
		{"killed", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, false)
			e.addCollections(inbox, colTec, colFoo, colRev)
			var ids []domain.ID
			for i := range total {
				ids = append(ids, e.add(fmt.Sprintf("b%02d", i), "tech: one of many", inbox, nil))
			}

			inFlight := make(chan domain.ID, 1)
			release := make(chan struct{})
			var once sync.Once
			seen := 0
			e.c.setBlock(func(ctx context.Context, id domain.ID) error {
				// Concurrency is 1, so calls come one at a time.
				seen++
				if seen != 5 {
					return nil
				}
				once.Do(func() { inFlight <- id })
				if tc.killed {
					<-ctx.Done() // Like a process dying: the call never comes back.
					return ctx.Err()
				}
				<-release
				return nil
			})

			j := e.createJob(`{"type":"backfill","params":{"batch_size":2}}`)
			var interrupted domain.ID
			select {
			case interrupted = <-inFlight:
			case <-time.After(testWait):
				t.Fatal("the job never reached the fifth item")
			}
			if tc.killed {
				e.stopApp()
				e.shutdown()
			} else {
				stopped := make(chan error, 1)
				go func() { stopped <- e.stop() }()
				close(release)
				select {
				case err := <-stopped:
					if err != nil {
						t.Fatalf("Shutdown: %v", err)
					}
				case <-time.After(testWait):
					t.Fatal("Shutdown never returned")
				}
			}
			e.c.setBlock(nil)

			// The stopped job is left to resume, not marked as finished.
			stored, err := e.store.Jobs().GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.killed {
				if stored.Status != domain.JobRunning {
					t.Fatalf("job = %s, want running", stored.Status)
				}
				if n := len(e.p.Writes()); n != 4 {
					t.Fatalf("%d writes before the kill, want 4", n)
				}
				if stored.Checkpoint.PageToken == "" {
					t.Fatal("no checkpoint saved before the kill")
				}
			}

			// A new App on the same file finishes it.
			e.boot()
			fin := e.waitJob(j.ID)
			if fin.Status != domain.JobSucceeded {
				t.Fatalf("resumed job = %s, error %v, %+v", fin.Status, fin.Error, fin.Progress)
			}
			for _, id := range ids {
				e.wantIn(id, colTec)
				if n := e.moveWrites(id); n != 1 {
					t.Fatalf("%s moved %d times: %+v", id, n, e.writesTo(id))
				}
				want := 1
				if tc.killed && id == interrupted {
					want = 2 // Its first call was the one in flight.
				}
				if n := e.c.callsOf(id); n != want {
					t.Fatalf("%s classified %d times, want %d", id, n, want)
				}
			}
			code, out := e.do(http.MethodGet, "/v1/snapshots", "")
			if code != http.StatusOK {
				t.Fatalf("snapshots = %d %s", code, out)
			}
			snaps := decode[struct {
				Items []json.RawMessage `json:"items"`
			}](t, out)
			if len(snaps.Items) != 1 {
				t.Fatalf("resuming took another snapshot: %s", out)
			}
		})
	}
}

// One in-flight call blocks a job for as long as hold says, then records its return.
func (e *env) jobInFlight(hold func(context.Context)) (returned *atomic.Bool) {
	e.t.Helper()
	e.addCollections(inbox, colTec, colFoo, colRev)
	id := e.add("b1", "tech: slow to classify", inbox, nil)
	returned = &atomic.Bool{}
	inFlight := make(chan struct{})
	e.c.setBlock(func(ctx context.Context, _ domain.ID) error {
		close(inFlight)
		hold(ctx)
		returned.Store(true)
		return ctx.Err()
	})
	e.createJob(`{"type":"backfill"}`)
	select {
	case <-inFlight:
	case <-time.After(testWait):
		e.t.Fatalf("the job never classified %s", id)
	}
	return returned
}

// A drain that outlasts the wait for cancelled work is still a clean stop: that wait
// only starts once something has been cancelled.
func TestShutdownDrainLongerThanStopWait(t *testing.T) {
	const wait = 500 * time.Millisecond
	e := newEnv(t, false)
	e.shutdown()
	e.stopWait = wait
	e.boot()
	returned := e.jobInFlight(func(context.Context) { <-time.After(wait * 3 / 2) })

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := e.app.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after a clean drain: %v", err)
	}
	if !returned.Load() {
		t.Fatal("Shutdown returned with the call in flight")
	}
	e.c.setBlock(nil)
}

// Work cancelled when the deadline runs out still gets the whole wait to unwind, or
// the database closes under it, however long the deadline had been.
func TestShutdownWaitsForCancelledWork(t *testing.T) {
	const wait = 500 * time.Millisecond
	e := newEnv(t, false)
	e.shutdown()
	e.stopWait = wait
	e.boot()
	returned := e.jobInFlight(func(ctx context.Context) {
		<-ctx.Done()
		<-time.After(wait / 2)
	})

	ctx, cancel := context.WithTimeout(context.Background(), wait*3/2)
	defer cancel()
	if err := e.app.Shutdown(ctx); err == nil {
		t.Fatal("a missed deadline should be reported")
	}
	if !returned.Load() {
		t.Fatal("the database closed while a cancelled handler was still unwinding")
	}
	e.stopApp()
	e.c.setBlock(nil)
	e.boot() // For the cleanup's clean stop, and the file must still be usable.
}

// proposing adds what roles.proposer needs to the test classifier.
type proposing struct {
	*classifier
	caps ports.ClassifierCapabilities
}

func (p proposing) Capabilities() ports.ClassifierCapabilities { return p.caps }

func (p proposing) ProposeTaxonomy(context.Context, ports.ProposeRequest) (ports.TaxonomyProposal, error) {
	return ports.TaxonomyProposal{}, nil
}

func putBody(t *testing.T, tx domain.Taxonomy) string {
	t.Helper()
	b, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func fixedTaxonomy(n int) domain.Taxonomy {
	tx := domain.Taxonomy{Mode: domain.ModeFixed, Status: domain.TaxonomyActive, Rules: domain.TaxonomyRules{MaxTags: 5}}
	for i := range n - 1 {
		tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: fmt.Sprintf("c%d", i), Name: fmt.Sprintf("C%d", i)})
	}
	tx.Collections = append(tx.Collections, domain.TaxonomyCollection{Key: "review", Name: "Needs Review", Role: "review"})
	return tx
}

func dynamicTaxonomy(most int, autoCreate bool) domain.Taxonomy {
	return domain.Taxonomy{Mode: domain.ModeDynamic, Status: domain.TaxonomyDraft, Rules: domain.TaxonomyRules{MaxTags: 5},
		Dynamic: domain.DynamicSettings{MaxCollections: most, SampleSize: 10, AutoCreate: autoCreate}}
}

// A PUT must not store what would stop the next start (§6.4). Each answer is a 422
// that stores nothing, and the service still starts afterwards.
func TestPutTaxonomyKeepsStartupRules(t *testing.T) {
	canPropose := ports.ClassifierCapabilities{FixedChoice: true, ProposeCollections: true, ProposeTags: true, MaxChoices: 100}
	noCollections := ports.ClassifierCapabilities{FixedChoice: true, ProposeTags: true, MaxChoices: 100}
	tests := []struct {
		name string
		caps *ports.ClassifierCapabilities // nil: the test classifier, which can't propose.
		body domain.Taxonomy
		want string
	}{
		{"dynamic with no proposer", nil, dynamicTaxonomy(12, false), "roles.proposer"},
		{"more collections than MaxChoices", nil, fixedTaxonomy(101), "101"},
		{"max_collections above MaxChoices", &canPropose, dynamicTaxonomy(101, false), "max_collections"},
		{"dynamic with no cap on collections", &canPropose, dynamicTaxonomy(0, false), "max_collections"},
		{"auto_create with no collection proposals", &noCollections, dynamicTaxonomy(12, true), "auto_create"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, false)
			if tc.caps != nil {
				e.shutdown()
				e.classifierOverride = proposing{e.c, *tc.caps}
				e.boot()
			}
			code, out := e.do(http.MethodPut, "/v1/taxonomies/added", putBody(t, tc.body))
			if code != http.StatusUnprocessableEntity || !bytes.Contains(out, []byte("validation_failed")) || !bytes.Contains(out, []byte(tc.want)) {
				t.Fatalf("PUT = %d %s, want a 422 about %s", code, out, tc.want)
			}
			if code, out := e.do(http.MethodGet, "/v1/taxonomies/added", ""); code != http.StatusNotFound {
				t.Fatalf("GET after a refused PUT = %d %s", code, out)
			}
			e.shutdown()
			e.boot()
		})
	}
}

// What the rules allow still goes through, right up to the limit, and starts.
func TestPutTaxonomyAcceptsWhatStarts(t *testing.T) {
	e := newEnv(t, false)
	e.shutdown()
	e.classifierOverride = proposing{e.c, ports.ClassifierCapabilities{FixedChoice: true, ProposeCollections: true, ProposeTags: true, MaxChoices: 100}}
	e.boot()
	for name, body := range map[string]domain.Taxonomy{"big": fixedTaxonomy(100), "auto": dynamicTaxonomy(100, true)} {
		if code, out := e.do(http.MethodPut, "/v1/taxonomies/"+name, putBody(t, body)); code != http.StatusOK {
			t.Fatalf("PUT %s = %d %s", name, code, out)
		}
	}
	e.shutdown()
	e.boot()
}

func TestDeployChangesNothing(t *testing.T) {
	e := newEnv(t, false)
	e.addCollections(inbox, colTec, colFoo, colRev)
	id := e.add("b1", "tech: in the inbox", inbox, nil)

	code, out := e.do(http.MethodPost, "/v1/jobs", `{"type":"sync"}`)
	if code != http.StatusConflict || !bytes.Contains(out, []byte("automation_disabled")) {
		t.Fatalf("sync = %d %s", code, out)
	}

	key := e.hook("bookmark.created", id.Local)
	if s := e.eventStatus(key); s != domain.EventHeld {
		t.Fatalf("event = %s, want held", s)
	}

	e.restartAndWaitForWorker()
	if s := e.eventStatus(key); s != domain.EventHeld {
		t.Fatalf("event = %s after the worker ran, want held", s)
	}
	if n := len(e.p.Writes()); n != 0 {
		t.Fatalf("%d provider writes", n)
	}
	if n := e.c.total(); n != 0 {
		t.Fatalf("%d classifier calls", n)
	}

	// A dry-run sync is still allowed, and still writes nothing.
	e.run(`{"type":"sync","dry_run":true}`)
	if n := len(e.p.Writes()); n != 0 {
		t.Fatalf("%d provider writes after a dry run", n)
	}
	e.wantIn(id, inbox)
}

// Turning automation off is how to stop the service writing on its own, so work
// left over from when it was on must not carry on after a restart (§9.0).
func TestAutomationOffStopsLeftoverWork(t *testing.T) {
	e := newEnv(t, true)
	e.addCollections(inbox, colTec, colFoo, colRev)
	id := e.add("b1", "tech: in the inbox", inbox, nil)
	e.shutdown()

	ctx := context.Background()
	now := time.Now().UTC()
	if err := e.store.Jobs().CreateJob(ctx, domain.Job{ID: "job_left", Type: domain.JobSync, Status: domain.JobQueued,
		Provider: "fake", TaxonomyID: "default", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	left := domain.StoredEvent{
		Event: domain.Event{Provider: "fake", Type: domain.EventBookmarkCreated, Resource: domain.ResourceRef{ID: id.Local}},
		ID:    "left", DedupKey: "left", Status: domain.EventPending, NextAttempt: now, ReceivedAt: now,
	}
	if _, err := e.store.Events().InsertEvent(ctx, left); err != nil {
		t.Fatal(err)
	}

	e.vals["AUTOMATION"] = "false"
	e.boot()
	j := e.waitJob("job_left")
	if j.Status != domain.JobFailed || j.Error == nil || !strings.Contains(*j.Error, "automation") {
		t.Fatalf("leftover sync = %+v, want failed for automation", j)
	}
	e.restartAndWaitForWorker()
	if s := e.eventStatus("left"); s != domain.EventPending {
		t.Fatalf("leftover event = %s, want it left pending", s)
	}
	if n := len(e.p.Writes()); n != 0 {
		t.Fatalf("%d provider writes", n)
	}
	if n := e.c.total(); n != 0 {
		t.Fatalf("%d classifier calls", n)
	}
}

func TestWebhookLoop(t *testing.T) {
	e := newEnv(t, true)
	e.addCollections(inbox, colTec, colFoo, colRev)
	id := e.add("b1", "tech: filed by Rainwise", inbox, nil)

	e.run(`{"type":"sync"}`)
	e.wantIn(id, colTec)
	writes, calls := len(e.p.Writes()), e.c.total()
	if writes != 1 || calls != 1 {
		t.Fatalf("the sync made %d writes and %d classifier calls", writes, calls)
	}

	// Rainwise's own move comes back as events.
	e.waitEvent(e.hook("bookmark.moved", id.Local))
	e.waitEvent(e.hook("bookmark.created", id.Local))
	if n := len(e.p.Writes()); n != writes {
		t.Fatalf("the echo made %d extra writes: %+v", n-writes, e.p.Writes())
	}
	if n := e.c.total(); n != calls {
		t.Fatalf("the echo made %d extra classifier calls", n-calls)
	}
	if pin := e.pinOf(id); pin != domain.PinNone {
		t.Fatalf("our own move pinned the bookmark: %q", pin)
	}

	// The same for a move made by an event.
	fresh := e.add("b2", "food: filed by an event", inbox, nil)
	e.waitEvent(e.hook("bookmark.created", fresh.Local))
	e.wantIn(fresh, colFoo)
	writes, calls = len(e.p.Writes()), e.c.total()
	e.waitEvent(e.hook("bookmark.moved", fresh.Local))
	if n := len(e.p.Writes()); n != writes {
		t.Fatalf("the echo made %d extra writes", n-writes)
	}
	if n := e.c.total(); n != calls {
		t.Fatalf("the echo made %d extra classifier calls", n-calls)
	}
	if pin := e.pinOf(fresh); pin != domain.PinNone {
		t.Fatalf("our own move pinned the bookmark: %q", pin)
	}
}
