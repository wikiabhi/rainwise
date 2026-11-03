# Rainwise MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-binary Go service that sorts a Raindrop library into a configured set of collections and tags, through webhooks, scheduled sync, and resumable backfill, with dry run and restore.

**Architecture:** Modular monolith with ports and adapters. Core logic depends only on `internal/domain` and `internal/ports`. Raindrop, the LLM and Jev classifiers, and SQLite are adapters wired in `cmd/rainwise`. All triggers go through one `pipeline.Process`.

**Tech Stack:** Go 1.27, standard `net/http` and `log/slog`, `gopkg.in/yaml.v3`, `modernc.org/sqlite`, Litestream (Cloudflare deployment only).

**Spec:** `docs/superpowers/specs/2026-11-03-rainwise-design.md`. Read it before starting any task. Section references below (§) point to it.

## Global Constraints

- Module path: `github.com/wikiabhi/rainwise`.
- Only two third-party Go dependencies: `gopkg.in/yaml.v3`, `modernc.org/sqlite`. No web frameworks, queues, or ORMs. CGO stays off.
- `internal/core/...` may import only the standard library, `internal/domain`, `internal/ports`, and other `internal/core` packages. No vendor names (`raindrop`, `openrouter`, `jev`, `cloudflare`) appear in `internal/core`, `internal/domain`, or `internal/ports`.
- No author-specific collections or rules in code. They live only in `examples/config.yaml`.
- Secrets come only from environment variables, and are never logged.
- Pipeline tag writes only add. Only restore may use `SetTags`.
- Tests make no real network calls.
- Inject time (`func() time.Time`) and sleeping wherever behavior depends on them. Tests never really sleep.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Work on the current branch, `design/rainwise-hld`. Run `go vet ./... && go test ./...` before each commit.

## Review Focus

1. **Bookmark sent to Trash or deleted between read and write.** Rainwise must not pull it out of Trash or fail the job. Test in Task 5.
2. **Bookmark with no usable text** (bare link, PDF, image). No classifier call on nothing, no invented classification: it goes to Needs Review. Test in Task 5.
3. **Duplicate collection names** in the library, such as a root and a nested "Backend". Matching is deterministic and uses only root-level collections. Test in Task 11.
4. **Restore after the user deleted a snapshot's collection.** The collection is recreated by name, and the bookmark returns to it. Test in Task 12.
5. **Sloppy model output:** JSON in code fences, a collection given by name instead of key or in different case, or the classifier choosing the review collection. These are accepted when unambiguous and don't count as failures. Tests in Tasks 4 and 7.

---

## File Map

```
cmd/rainwise/main.go               serve, config validate
cmd/rainwise/wire.go               composition root
internal/domain/                   models, IDs, errors
internal/ports/                    provider, classifier, store interfaces
internal/archtest/                 import-rule test
internal/config/                   YAML load, env expansion, validation
internal/core/taxonomy/            tags, vocab, resolver, proposal
internal/core/pipeline/            hash, validator, move rules, Process
internal/core/jobs/                service, runner, sync, backfill, reclassify, restore, snapshot
internal/core/events/              inbox worker
internal/adapters/provider/raindrop/
internal/adapters/provider/fake/
internal/adapters/provider/providertest/
internal/adapters/classifier/llm/
internal/adapters/classifier/jev/
internal/adapters/classifier/fake/
internal/adapters/classifier/classifiertest/
internal/adapters/store/sqlite/
internal/adapters/store/memory/
internal/adapters/store/storetest/
internal/adapters/contentfetch/
internal/httpapi/
internal/scheduler/
internal/e2e/
examples/config.yaml
deploy/docker-compose/, deploy/cloudflare/
docs/live-checks.md, README.md, Dockerfile
```

---

### Task 1: Scaffold, domain models, ports, import rule

**Files:**
- Create: `go.mod`, `.gitignore`, `internal/domain/{id,models,errors,jobs,state}.go`, `internal/ports/{provider,classifier,store}.go`, `internal/archtest/imports_test.go`
- Test: `internal/domain/id_test.go`

**Interfaces:**
- Produces, in `domain`:
  - `ID{Provider, Local string}`, `ParseID(string) (ID, error)`, `(ID) String()`, written as `raindrop:12345`.
  - `Bookmark`, `Highlight`, `Collection`, `Event`, `ResourceRef{ID, URL string}`, `EventType` constants as in §5.1.
  - `Page[T]{Items []T; NextToken string}`, `TagCount{Name string; Count int}`.
  - Errors: `ErrNotFound`, `ErrConflict`, `ErrValidation`, `ErrRateLimited`, `ErrTemporary`, `ErrInvalidOutput`, `ErrCapabilityUnsupported`, `ErrAutomationDisabled`.
  - `JobType`: `sync|backfill|reclassify|snapshot|restore`. `JobStatus`: `queued|running|succeeded|succeeded_with_errors|failed|cancelling|canceled`.
  - `Job{ID, Type, Status, Provider, TaxonomyID string; DryRun bool; Params JobParams; Scope Scope; FromDryRun, SnapshotID, IdempotencyKey, Error string; Progress JobProgress; Checkpoint Checkpoint; CreatedAt, StartedAt, FinishedAt}`.
  - `JobParams{BatchSize int; ProposeOnly, Force, IncludePinned bool; SnapshotID string; ApprovedNewTags map[string]string}`.
  - `Scope{Collections, Tags, IDs []string}`. `JobProgress{Total, Processed, Changed, Skipped, NeedsReview, Failed int}`. `Checkpoint{PageToken string; PinCutoff *time.Time; Pauses int}`.
  - `PinSource`: `""|baseline|user`.
  - `BookmarkState{ID ID; KnownCollection ID; KnownTags []string; PinSource PinSource; PinnedAt *time.Time; InputHash string; TaxonomyVersion int; Model, Status string; PendingCollection *ID; PendingTags []string; FetchAttempts int; ProcessedAt, RestoredAt *time.Time}`.
  - `CollectionChoice{Key, ProposedName string}`, `TagSuggestion{Name string; Score *float64}`, `ModelInfo{Classifier, Model, Version string; InputTokens, OutputTokens int}`, `Decision` as in §5.3.
  - `DecisionRecord{JobID string; Bookmark ID; InputHash string; ObservedCollection ID; ObservedTags []string; TargetCollection *ID; AddTags []string; Decision Decision; Reason, Result string; AppliedAt *time.Time}`.
  - `TaxonomyCollection{Key, Name, Description, Role string}`; `TaxonomyRules{MinTags, MaxTags int; MinConfidence float64; AllowNewTags bool; TagAliases map[string]string; PromptExtra string}`; `DynamicSettings{MaxCollections, SampleSize int; AutoCreate bool; AutoCreateMinConfidence float64}`.
  - `Taxonomy{ID, Mode, Status string; Version int; Collections []TaxonomyCollection; Rules TaxonomyRules; ProvisionMissing bool; Dynamic DynamicSettings; Proposals []CollectionProposal}`. Modes `fixed|dynamic`, statuses `draft|active`.
  - `CollectionProposal{Name, Description string; Count int}`.
  - `StoredEvent{Event; ID, DedupKey, Status string; Attempts int; NextAttempt, ReceivedAt time.Time}`. Statuses `pending|held|done|failed`.
  - `Snapshot{ID, Provider, JobID string; CreatedAt; Collections []Collection; CreatedCollections []ID; ScopeAll bool}`, `SnapshotItem{Bookmark, Collection ID; Tags []string}`.
- Produces, in `ports`:
  - All provider interfaces, `ProviderCapabilities`, `ListQuery`, `BookmarkPatch` exactly as in §5.2. `ListCollections`, `CreateCollection`, `DeleteCollectionIfEmpty` on `CollectionManager`. `ListHighlights`, `ListAllHighlights` on `HighlightReader`.
  - `Classifier`, `TaxonomyProposer`, `ClassifierCapabilities`, `ClassifyRequest`, as in §5.3. `Subject{Bookmark domain.Bookmark; Highlights []domain.Highlight; FocusHighlight *domain.Highlight}`.
  - `ProposeRequest{Samples []Subject; ExistingCollections []string; MaxCollections int; Extra string}`; `TaxonomyProposal{Collections []domain.CollectionProposal; Model domain.ModelInfo}`.
  - `ContentFetcher{Fetch(ctx, url string) (string, error)}`: errors wrap `ErrTemporary` for retryable failures.
  - `Store` with `JobStore`, `EventStore`, `StateStore`, `SnapshotStore`, `TaxonomyStore`:
    - `JobStore`: `CreateJob(ctx, Job) error`, `GetJob(ctx, id) (Job, error)`, `FindJobByIdempotencyKey(ctx, key) (Job, bool, error)`, `FindActiveJob(ctx, provider string, t JobType) (Job, bool, error)`, `ListJobs(ctx, cursor string, limit int) (Page[Job], error)`, `UpdateJob(ctx, Job) error`, `ClaimNextQueued(ctx, provider string, dryRun bool) (Job, bool, error)`, `ListRunning(ctx) ([]Job, error)`, `SaveDecision(ctx, DecisionRecord) error`, `GetDecision(ctx, jobID string, b ID) (DecisionRecord, bool, error)`, `ListDecisions(ctx, jobID, cursor string, limit int) (Page[DecisionRecord], error)`.
    - `EventStore`: `InsertEvent(ctx, StoredEvent) (inserted bool, err error)`, `ClaimEvents(ctx, now time.Time, limit int) ([]StoredEvent, error)`, `UpdateEvent(ctx, StoredEvent) error`, `DropHeldBefore(ctx, t time.Time) (int, error)`.
    - `StateStore`: `GetState(ctx, ID) (BookmarkState, bool, error)`, `PutState(ctx, BookmarkState) error`, `GetSetting(ctx, key string) (string, bool, error)`, `PutSetting(ctx, key, value string) error`.
    - `SnapshotStore`: `CreateSnapshot(ctx, Snapshot) error`, `AddSnapshotItems(ctx, id string, items []SnapshotItem) error`, `AddCreatedCollection(ctx, id string, c ID) error`, `GetSnapshot(ctx, id) (Snapshot, error)`, `ListSnapshots(ctx, cursor string, limit int) (Page[Snapshot], error)`, `ListSnapshotItems(ctx, id, cursor string, limit int) (Page[SnapshotItem], error)`, `PruneSnapshots(ctx, keep int, protect []string) error`.
    - `TaxonomyStore`: `GetTaxonomy(ctx, id) (Taxonomy, error)`, `ListTaxonomies(ctx) ([]Taxonomy, error)`, `PutTaxonomy(ctx, t Taxonomy, ifVersion *int) (Taxonomy, error)` (bumps `Version`, returns `ErrConflict` on mismatch), `SeedTaxonomy(ctx, Taxonomy) (seeded bool, err error)`.

- [ ] **Step 1: Write the failing tests.** `TestParseID` table: `"raindrop:123"` gives `{raindrop,123}`; `"a:b:c"` gives `{a,"b:c"}`; `""`, `"x"`, `":1"`, `"x:"` return `ErrValidation`. `TestIDString` round-trips. `TestCoreImports` walks `internal/core`, `internal/domain`, `internal/ports` with `go/parser`, and fails on a disallowed import, or on a vendor name from Global Constraints in identifiers or string literals.
- [ ] **Step 2: Run `go test ./internal/...`.** Expected: FAIL, undefined `ParseID`.
- [ ] **Step 3: Implement** `go.mod`, the domain types, and the ports above.
- [ ] **Step 4: Run `go test ./internal/...`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add domain models, ports, and import rule`.

---

### Task 2: Config

**Files:**
- Create: `internal/config/config.go`, `internal/config/validate.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Consumes: `domain.Taxonomy` and friends.
- Produces: `Load(path string, getenv func(string) string) (*Config, error)`. `Config` matches the YAML in §8 and §12: `Server`, `State{SQLitePath string; DurableWait time.Duration; SnapshotRetention int}`, `Automation{Enabled bool}`, `Scheduler{Enabled bool; SyncEvery time.Duration}`, `Processing{Provider, Taxonomy string; Concurrency int; ContentFetch string; ContentFetchMinChars int; OnMoveRetag bool; TagVocabSize int}`, `Providers map[string]ProviderConfig`, `Classifiers map[string]ClassifierConfig`, `Roles{Classifier, Proposer string; Fallback *string}`, `Taxonomies map[string]domain.Taxonomy`.
- Defaults: `concurrency 4`, `content_fetch auto`, `content_fetch_min_chars 80`, `tag_vocab_size 200`, `snapshot_retention 10`, `requests_per_minute 100`, `max_tag_candidates 100`, `tag_threshold 0.7`, `min_tags 2`, `max_tags 5`, `min_confidence 0.6`, `max_collections 12`, `sample_size 1000`, `auto_create_min_confidence 0.85`, taxonomy `status active` when fixed, `draft` when dynamic.

- [ ] **Step 1: Write the failing tests.**
  - `TestLoadExpandsEnv`: `${RAINDROP_TOKEN}` resolves from `getenv`.
  - `TestLoadRejectsMissingEnv`: an unset variable fails, and the error names the variable.
  - `TestLoadRejectsUnknownKeys`: `servr:` fails (`KnownFields(true)`).
  - `TestLoadDefaults`: asserts the defaults above.
  - `TestValidate` table, each failing with a clear message: role naming an unknown classifier; `processing.provider` unknown; `concurrency: 0`; `min_tags > max_tags`; `min_confidence` outside 0..1; fixed taxonomy with no non-review collections; two collections with `role: review`; duplicate collection keys; no review collection.
  - `TestStringRedactsSecrets`: `fmt.Sprintf("%+v", cfg)` doesn't contain the token value.
- [ ] **Step 2: Run `go test ./internal/config/`.** Expected: FAIL, undefined `Load`.
- [ ] **Step 3: Implement.** Secrets use a `Secret` string type with a redacting `String()`, `GoString()`, and `Format`. Capability checks from §6.4 belong in Task 15, where adapters exist.
- [ ] **Step 4: Run `go test ./internal/config/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add YAML and env configuration`.

---

### Task 3: Stores

**Files:**
- Create: `internal/adapters/store/storetest/suite.go`, `internal/adapters/store/memory/memory.go`, `internal/adapters/store/sqlite/{sqlite,migrate,jobs,events,state,snapshots,taxonomies}.go`, `internal/adapters/store/sqlite/migrations/001_init.sql`
- Test: `internal/adapters/store/memory/memory_test.go`, `internal/adapters/store/sqlite/sqlite_test.go`

**Interfaces:**
- Consumes: `ports.Store`.
- Produces: `storetest.Run(t *testing.T, newStore func(t *testing.T) ports.Store)`, `memory.New() ports.Store`, `sqlite.Open(ctx, path string) (*sqlite.Store, error)` with `Close() error`. Tables per §10.1.

- [ ] **Step 1: Write the conformance suite and failing tests that call it.** Subtests:
  - `InsertEventDedups`: the second insert with the same `DedupKey` returns `inserted=false`.
  - `ClaimEventsSkipsHeldAndFuture`: `held` and future `NextAttempt` aren't claimed. A claimed event isn't claimed again until updated.
  - `ClaimNextQueuedOldestFirst`, respecting provider and `dryRun`. It sets `running`.
  - `FindActiveJob`: finds `queued` and `running`, not finished ones.
  - `IdempotencyKeyLookup`.
  - `JobRoundTrip`: `Params.ApprovedNewTags`, `Checkpoint`, `Progress`, and `Scope` survive.
  - `DecisionRoundTripAndPaging`: 120 decisions, limit 50, three pages with no repeats.
  - `StateRoundTrip`: nil vs set pointers survive.
  - `Settings`.
  - `SnapshotItemsPaging`, `AddCreatedCollection`, `PruneKeepsNewestAndProtected`.
  - `PutTaxonomyVersioning`: version bumps; stale `ifVersion` gives `ErrConflict`; `SeedTaxonomy` doesn't overwrite.
  - `NotFoundErrors`: wraps `ErrNotFound`.
- [ ] **Step 2: Add SQLite-only tests.** `TestReopenPersists`: data survives close and reopen. `TestMigrateIdempotent`. `TestConcurrentWrites`: 20 goroutines writing state, no `database is locked`.
- [ ] **Step 3: Run `go test ./internal/adapters/store/...`.** Expected: FAIL, undefined `memory.New`.
- [ ] **Step 4: Implement both stores.** SQLite uses WAL, `busy_timeout`, `foreign_keys=ON`, and `SetMaxOpenConns(1)`. Migrations use `embed`. Complex fields are JSON columns.
- [ ] **Step 5: Run `go test ./internal/adapters/store/...`.** Expected: PASS.
- [ ] **Step 6: Commit** `feat: add memory and SQLite stores`.

---

### Task 4: Tags, validator, input hash

**Files:**
- Create: `internal/core/taxonomy/tags.go`, `internal/core/pipeline/hash.go`, `internal/core/pipeline/validator.go`
- Test: `internal/core/taxonomy/tags_test.go`, `internal/core/pipeline/hash_test.go`, `internal/core/pipeline/validator_test.go`

**Interfaces:**
- Produces, in `taxonomy`:
  - `Normalize(tag string, aliases map[string]string) string`
  - `ComparisonKey(tag string) string`
  - `type RunVocab`, created by `NewRunVocab(existing []string, aliases map[string]string, allowNew bool)`, with `Resolve(tag string) (final string, isNew, ok bool)` and `ProposedCounts() map[string]int`. Safe for concurrent use.
- Produces, in `pipeline`:
  - `InputHash(b domain.Bookmark, highlights []domain.Highlight) string`: SHA-256 over URL, title, description, excerpt, and sorted highlight text. It excludes tags and collection.
  - `ValidateInput{Decision domain.Decision; Bookmark domain.Bookmark; Taxonomy domain.Taxonomy; Vocab *taxonomy.RunVocab; WantCollection bool; Resolved map[string]domain.ID; CanCreateCollections bool}`.
  - `Validated{Target *domain.ID; ToReview bool; AddTags, NewTags []string; ProposedCollection *domain.CollectionProposal; Reasons []string}`.
  - `Validate(in ValidateInput) Validated`.

- [ ] **Step 1: Write the failing tests.**

```go
// TestNormalize
{"#Postgres", "postgres"}, {"  System Design ", "system-design"},
{"K8s", "kubernetes"} /* aliases {k8s: kubernetes} */, {"#", ""},

// TestRunVocabResolve, existing {"database","Go"}
{"databases", "database", false, true}  // plural folds to existing
{"go", "Go", false, true}               // existing spelling wins
{"kubernetes", "kubernetes", true, true}// new tag keeps its form
{"llms", "llms", true, true}; {"llm", "llms", false, true} // joins the run's vocab
// allowNew=false: {"kubernetes", "", false, false}
```

  Folding: `ComparisonKey` removes a trailing `s` when the tag is longer than 3 characters and doesn't end in `ss`, `us`, or `is`. It's used only for comparison, never written to a tag.

  `TestInputHash`: tags and collection changes don't alter it; title or highlight changes do; highlight order doesn't.

  `TestValidate` table:
  - Allowed collection, confidence 0.9: target is that collection.
  - Key not allowed: `ToReview`, reason recorded.
  - Collection given as a name, in a different case: accepted.
  - Choosing the review collection: `ToReview`, not a failure.
  - Confidence 0.5 (threshold 0.6), bookmark in an unrelated collection: `ToReview`.
  - Confidence 0.5, bookmark already in an allowed collection: `Target` nil, tags still evaluated.
  - `WantCollection=false`: `Target` nil, `ToReview` false, whatever the confidence.
  - Bookmark already in target: `Target` nil.
  - Existing 3 tags, `MaxTags 5`, 4 suggested: 2 added.
  - Existing 5 or 7 tags: none added, none removed.
  - Suggested tag equal to an existing one in different case: dropped.
  - `allow_new_tags: false`: unknown tags dropped.
  - Proposed name with `AutoCreate` true, confidence ≥ 0.85, under cap, `CanCreateCollections` true: `ProposedCollection` set and not sent to review.
  - Otherwise a proposed name: `ToReview`, with `ProposedCollection` recorded.
  - Fewer than `MinTags`: reason recorded, no invented tags.
- [ ] **Step 2: Run `go test ./internal/core/...`.** Expected: FAIL, undefined `Normalize`.
- [ ] **Step 3: Implement** the signatures above. Empty normalized tags are dropped.
- [ ] **Step 4: Run `go test ./internal/core/...`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add tag rules, decision validator, and input hash`.

---

### Task 5: Fakes and the pipeline

**Files:**
- Create: `internal/adapters/provider/fake/fake.go`, `internal/adapters/classifier/fake/fake.go`, `internal/adapters/provider/providertest/suite.go`, `internal/adapters/classifier/classifiertest/suite.go`, `internal/core/pipeline/{pipeline,moves}.go`
- Test: `internal/adapters/provider/fake/fake_test.go`, `internal/core/pipeline/pipeline_test.go`, `internal/core/pipeline/moves_test.go`

**Interfaces:**
- Consumes: Task 1 ports, Task 3 `memory.New`, Task 4 `Validate`, `InputHash`, `RunVocab`.
- Produces:
  - `fake.Provider`: in-memory, with `Add(domain.Bookmark)`, `Writes() []Write`, `FailNext(err)`, `OnGet(func(domain.ID))` for changing state between reads, and capability overrides. `fake.Classifier`: scripted decisions, call count.
  - `providertest.Run(t, newProvider func(t) ports.Provider)`, checking: `AddTags` never drops a tag, `Get` of a missing ID wraps `ErrNotFound`, pagination returns everything exactly once, optional ports match declared capabilities. `classifiertest.Run(t, c ports.Classifier, stub)`, checking declared capabilities match behavior.
  - `pipeline.New(Deps) *Pipeline`, `Deps{Provider ports.Provider; Classifier, Fallback ports.Classifier; Fetcher ports.ContentFetcher; Store ports.Store; Now func() time.Time; Log *slog.Logger; ContentFetch string; MinChars int}`.
  - `WorkItem{Bookmark domain.ID; FocusHighlight *domain.ID}`.
  - `Trigger`: `TriggerBackfill|TriggerSync|TriggerWebhook|TriggerReclassify`.
  - `Options{Trigger Trigger; JobID string; DryRun, Force, IncludePinned, Baseline, TagsOnly bool; IsFirstBackfill bool; PinCutoff *time.Time; Taxonomy domain.Taxonomy; Resolved map[string]domain.ID; Vocab *taxonomy.RunVocab; Plan *domain.DecisionRecord; ApprovedNewTags map[string]string}`.
  - `Outcome{Status OutcomeStatus; Record domain.DecisionRecord}`. `OutcomeStatus`: `Changed|Unchanged|Skipped|NeedsReview|Deleted`.
  - `(*Pipeline).Process(ctx, WorkItem, Options) (Outcome, error)`. Returned errors mean item failure, and the caller retries later.
  - `EvaluateMove(state domain.BookmarkState, known bool, b domain.Bookmark, inbox *domain.ID, opts Options) (wantCollection bool)` in `moves.go`, as a pure function.
  - `DetectManualMove(state *domain.BookmarkState, b domain.Bookmark, now time.Time) bool`, also pure, per §10.3.

- [ ] **Step 1: Write failing tests for `EvaluateMove`, covering §9.1 and §9.1a as a table.**

```go
// trigger, location, pin, opts => wantCollection
{backfill, "col", "", first} => true
{backfill, "col", user, first, pinnedBeforeCutoff} => true
{backfill, "col", user, first, pinnedAfterCutoff} => false
{backfill, "col", user, later} => false
{backfill, "col", baseline, later} => true
{sync, inbox, "", newBookmark} => true
{sync, "col", "", newBookmark} => false  // and pins as user
{webhook, inbox, "", newBookmark} => true
{reclassify, "col", user, ""} => false
{reclassify, "col", user, includePinned} => true
{sync, needsReview, "", changedInput} => true
{sync, "col", "", changedInput} => false // hash recorded, no classifier call
{any, "col", "", tagsOnly} => false
{sync, nil-inbox provider, no collection, newBookmark} => true
```

  `TestDetectManualMove` table: same collection is not a move; different with no pending write pins `user`; different but matching a pending write clears the pending write and doesn't pin; out of Needs Review pins; to inbox pins.

- [ ] **Step 2: Write failing `pipeline_test.go` tests,** using `fake.Provider`, `fake.Classifier`, `memory.New()`:
  - `TestProcessNewInboxBookmark`: moved and tagged; existing tags kept; state records known collection, tags, hash, taxonomy version, model.
  - `TestProcessSkipsSameHash`: second run makes 0 classifier calls and 0 writes.
  - `TestProcessForceBypassesHash`.
  - `TestProcessDryRunNoWrites`: 0 provider writes, a decision saved, `ObservedCollection` recorded, and `KnownCollection` updated.
  - `TestCorrectionAfterDryRun`: dry run, then the user moves the bookmark, then apply with `Plan`: 0 moves, state pinned `user`, tags from the plan still added.
  - `TestPlanHashChangedReclassifies`.
  - `TestApprovedNewTagsMapping`: `{"ai-agents":"agents"}` writes `agents`; an unmapped new tag isn't written; caps apply again afterwards.
  - `TestUserMoveDuringClassify`: `OnGet` changes the collection on the re-read: no move, pinned `user`.
  - `TestTrashedBetweenReadAndWrite`: re-read shows Trash: no writes, no error, status `Unchanged`, pinned.
  - `TestDeletedBookmark`: `ErrNotFound` on the first read or on write: `Deleted` outcome, no error.
  - `TestNoUsableTextGoesToReview`: empty title, description, excerpt, failed fetch: 0 classifier calls, moved to review. Tag nothing.
  - `TestDomainOnlyNeverEnough`: a URL with no other text behaves the same way.
  - `TestClassifierErrorFailsItem`: returns an error, no move to review, hash not recorded.
  - `TestInvalidOutputRetriesOnce`: `ErrInvalidOutput` then success: 2 calls.
  - `TestFallbackOnError`: primary errors, fallback used, recorded on the decision. `TestFallbackNotUsedForLowConfidence`.
  - `TestContentFetch` table: `off` never fetches; `auto` only under `MinChars`; `always` always.
  - `TestFetchTemporaryFailure`: below `MinChars`, `ErrTemporary`: item error, `FetchAttempts` 1, no review; 3rd attempt treated as permanent.
  - `TestFetchPermanentFailure`: enough other text classifies; too little goes to review.
  - `TestBaselineRecordsOnly`: outside inbox pins `baseline`, 0 classifier calls.
  - `TestRestoredBookmarkSkippedBySync`: `RestoredAt` set: sync skips, backfill processes.
  - `TestHighlightTagsOnly`: focus highlight, tags added, no move; below `MinConfidence`, no tags.
  - `TestCrashAfterWrite`: pending write recorded and a matching collection on re-run: state finalized, no pin.
  - `TestPerBookmarkLock`: 2 concurrent `Process` calls on the same ID serialize (`-race`).
  - `TestAutoCreateCollection`: creates, records in the job's snapshot through `AddCreatedCollection` when a snapshot exists, and stops at `max_collections`.
- [ ] **Step 3: Run `go test -race ./internal/core/pipeline/`.** Expected: FAIL, undefined `EvaluateMove`.
- [ ] **Step 4: Implement** the fakes, suites, `moves.go`, and `Process`, following the steps in §9.1. Review collection key comes from `Taxonomy.Collections` with `Role == "review"`. Vocab reads from `Options.Vocab`.
- [ ] **Step 5: Run `go test -race ./internal/core/pipeline/ ./internal/adapters/provider/fake/`.** Expected: PASS.
- [ ] **Step 6: Commit** `feat: add shared processing pipeline`.

---

### Task 6: Content fetcher

**Files:**
- Create: `internal/adapters/contentfetch/fetch.go`
- Test: `internal/adapters/contentfetch/fetch_test.go`

**Interfaces:**
- Produces: `New(Options{Timeout time.Duration; MaxBytes int64; MaxChars int; AllowPrivate bool}) *Fetcher`, implementing `ports.ContentFetcher`. Defaults: 10 s, 2 MB, 20,000 chars. `AllowPrivate` exists only for tests against `httptest`.

- [ ] **Step 1: Write the failing tests.**
  - `TestFetchExtractsText`: HTML to readable text; script and style removed.
  - `TestFetchBlocksPrivate` table, `AllowPrivate=false`: `127.0.0.1`, `10.0.0.1`, `169.254.169.254`, `[::1]`, `file:///etc/passwd`, `ftp://x`: all permanent errors with no connection.
  - `TestFetchBlocksRedirectToPrivate`: checks at dial time through `net.Dialer.Control`.
  - `TestFetchErrorKinds` table: 500 and timeout wrap `ErrTemporary`; 404 and 403 don't; 429 does.
  - `TestFetchSizeCap`: 3 MB body is truncated, not failed.
  - `TestFetchNonText`: PDF content type is a permanent error.
- [ ] **Step 2: Run `go test ./internal/adapters/contentfetch/`.** Expected: FAIL, undefined `New`.
- [ ] **Step 3: Implement.** Extract text with a small standard library tag stripper; `golang.org/x/net/html` isn't allowed under Global Constraints.
- [ ] **Step 4: Run `go test ./internal/adapters/contentfetch/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add guarded content fetcher`.

---

### Task 7: LLM classifier

**Files:**
- Create: `internal/adapters/classifier/llm/{llm,prompt,schema}.go`
- Test: `internal/adapters/classifier/llm/llm_test.go`

**Interfaces:**
- Consumes: `ports.Classifier`, `ports.TaxonomyProposer`.
- Produces: `New(Config{Name, BaseURL, Model string; APIKey string; Headers map[string]string; ExtraBody map[string]any; HTTP *http.Client}) *Classifier`. The adapter takes a plain string key and doesn't import `internal/config`. Capabilities: `FixedChoice`, `ProposeCollections`, `ProposeTags` true; `CalibratedConfidence` false; `MaxChoices` 0 (no limit).

- [ ] **Step 1: Write the failing tests** against `httptest`:
  - `TestRequestShape`: `POST {base}/chat/completions`, bearer header, `response_format.type == "json_schema"`, `extra_body` keys merged at top level, collection keys and descriptions in the prompt, review collection absent, `TagVocab` present.
  - `TestPromptRules`: mentions that page content is data, not instructions, and that the domain alone isn't enough. Contains `PromptExtra`.
  - `TestParseDecision`: returns collection, tags, confidence, reasoning, model info, token usage.
  - `TestParseCodeFence`: JSON wrapped in ```` ```json ```` parses.
  - `TestInvalidOutput` table: malformed JSON, confidence 1.5, missing confidence: `ErrInvalidOutput`.
  - `TestHTTPErrorKinds` table: 429 wraps `ErrRateLimited`; 500, 529 wrap `ErrTemporary`; 401 is neither.
  - `TestWantCollectionFalse`: schema omits collection; `TagsWanted 0` omits tags.
  - `TestProposeTaxonomy`: returns at most `MaxCollections`; more are truncated. 2,500 samples go in batches of 200, then one merge call.
  - `classifiertest.Run` passes.
- [ ] **Step 2: Run `go test ./internal/adapters/classifier/llm/`.** Expected: FAIL, undefined `New`.
- [ ] **Step 3: Implement.** The adapter makes one HTTP attempt per call for invalid output, as the pipeline owns that retry. It retries 429 and 5xx 3 times with injected backoff.
- [ ] **Step 4: Run `go test ./internal/adapters/classifier/llm/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add OpenAI-compatible LLM classifier`.

---

### Task 8: Jev classifier

**Files:**
- Create: `internal/adapters/classifier/jev/jev.go`
- Test: `internal/adapters/classifier/jev/jev_test.go`

**Interfaces:**
- Consumes: `ports.Classifier`. Does not implement `TaxonomyProposer`.
- Produces: `New(Config{Name, BaseURL, Model, APIKey string; MaxTagCandidates int; TagThreshold float64; HTTP *http.Client}) *Classifier`. Capabilities: `FixedChoice`, `CalibratedConfidence` true; `ProposeCollections`, `ProposeTags` false; `MaxChoices` 255.

- [ ] **Step 1: Write the failing tests** against `httptest`, using the request and response shapes in §3.2:
  - `TestRequestShape`: `POST {base}/v1/systemone`, bearer, `state`, `model`, and a `collection` question of `type: choice` whose `criteria` map keys to descriptions.
  - `TestTagQuestions`: one `noul` per vocabulary tag, capped at `MaxTagCandidates`. Keys are `tag_0`, `tag_1`... and mapped back.
  - `TestParse`: `choice` becomes `Collection.Key`, `confidence` becomes `Confidence`, tags above 0.7 are sorted high to low and capped at `TagsWanted`, `model` in the response goes to `ModelInfo.Version`.
  - `TestWantCollectionFalse`: no collection question; `Confidence` is the highest tag's `|2p-1|`.
  - `TestTooManyChoices`: 256 collections gives `ErrValidation`.
  - `TestHTTPErrorKinds`: 429 and 529 as in Task 7; `400 max_tokens_exceeded` retries once with `state` truncated by half.
  - `classifiertest.Run` passes.
- [ ] **Step 2: Run `go test ./internal/adapters/classifier/jev/`.** Expected: FAIL, undefined `New`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run `go test ./internal/adapters/classifier/jev/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add Jev classifier`.

---

### Task 9: Raindrop provider

**Files:**
- Create: `internal/adapters/provider/raindrop/{client,bookmarks,collections,highlights,normalize,ratelimit}.go`
- Test: `internal/adapters/provider/raindrop/raindrop_test.go`, `normalize_test.go`

**Interfaces:**
- Consumes: Task 1 provider ports.
- Produces: `New(Config{Token string; BaseURL string; RequestsPerMinute int; IncrementalSince, TagListing bool; HTTP *http.Client; Sleep func(context.Context, time.Duration) error; Now func() time.Time}) *Provider`. Default base URL `https://api.raindrop.io/rest/v1`. Implements `Provider`, `HighlightReader`, `CollectionManager`, `EventNormalizer`, and `TagLister` when `TagListing`. Both flags default to false in config until `docs/live-checks.md` confirms them (§3.4 items 1, 4). Capabilities: `Highlights`, `CreateCollections` true; `NativeWebhooks`, `CursorPagination` false; `InboxCollection` `raindrop:-1`.
- Also produces `(*Provider) ResolveURL(ctx, url string) (domain.ID, error)`, through an optional `ports.URLResolver` interface, added here in `internal/ports/provider.go`.

- [ ] **Step 1: Write the failing tests** against `httptest`, with shapes from §3.1:
  - `TestListPaging`: `GET /raindrops/0?page=0&perpage=50&sort=created`, `PageToken` round trip; last short page has empty `NextToken`.
  - `TestListSince`: with `IncrementalSince`, `search=lastUpdate:>2026-11-01`. Without it, `Since` is ignored.
  - `TestListNeverTrash`: never requests `-99`. A `GetBookmark` of a trashed item maps collection to `raindrop:-99`.
  - `TestMapping`: Raindrop fields to `domain.Bookmark`, including `collection.$id`, `tags`, `excerpt`, `note` as `Metadata["note"]`.
  - `TestAddTagsMerges`: reads, merges case-insensitively, `PUT /raindrop/{id}` includes every existing tag.
  - `TestSetTagsReplaces`, `TestMoveOnly`: no `tags` field sent.
  - `TestUpdateNotFound`: 404 wraps `ErrNotFound`.
  - `TestRateLimitBackoff`: 429 with `X-RateLimit-Reset`: sleeps until then and retries. Also honors the `RateLimit-Remaining` and `X-RateLimit-Remaining` spellings.
  - `TestTokenBucket`: limit is applied across concurrent calls with an injected clock.
  - `Test5xxRetry`: 3 failures then success; 6 failures wraps `ErrTemporary`.
  - `TestCollections`: list merges `/collections` and `/collections/childrens`; `Parent` set for nested; create sends `title`.
  - `TestDeleteIfEmpty`: non-empty returns `false` with no `DELETE`.
  - `TestHighlights`: per-bookmark and global with paging.
  - `TestResolveURL`: search by URL, exact match only, `ErrNotFound` otherwise.
  - `providertest.Run` passes against a small stateful `httptest` server.
  - `TestNormalize` table, on the canonical payload in §7.2: valid events map; `resource.url` only is accepted; neither gives `ErrValidation`; unknown `type` gives `ErrValidation`; `highlight.created` without `bookmark` gives `ErrValidation`; dedup uses `event_id` when present, otherwise a hash, and two identical bodies give the same key.
- [ ] **Step 2: Run `go test -race ./internal/adapters/provider/raindrop/`.** Expected: FAIL, undefined `New`.
- [ ] **Step 3: Implement.** `AddTags` uses read-merge-write. The canonical normalizer lives in `internal/core/events/canonical.go` as `events.NormalizeCanonical(provider string, body []byte) ([]domain.Event, error)`, so future providers reuse it, and Raindrop delegates. Create that file in this step.
- [ ] **Step 4: Run `go test -race ./internal/adapters/provider/raindrop/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add Raindrop provider`.

---

### Task 10: Job service and runner

**Files:**
- Create: `internal/core/jobs/{service,runner,loop}.go`
- Test: `internal/core/jobs/service_test.go`, `runner_test.go`

**Interfaces:**
- Consumes: `ports.Store`, `domain.Job`.
- Produces:
  - `NewService(ServiceDeps{Store ports.Store; AutomationEnabled bool; ProviderNames []string; Now func() time.Time}) *Service`.
  - `CreateRequest{Type domain.JobType; Provider, Taxonomy string; DryRun bool; Params domain.JobParams; Scope domain.Scope; FromDryRun, IdempotencyKey string}`.
  - `(*Service) Create(ctx, CreateRequest) (job domain.Job, created bool, err error)`, `Get`, `List`, `Cancel(ctx, id) (domain.Job, error)`.
  - `Handler interface { Run(ctx context.Context, job *domain.Job, rc RunContext) error }`.
  - `RunContext{Checkpoint func(context.Context) error; Stopping <-chan struct{}}`. `Checkpoint` saves `job.Progress` and `job.Checkpoint`.
  - `ErrInterrupted`: a handler returns it when `Stopping` closes. The job stays resumable.
  - `NewRunner(RunnerDeps{Store ports.Store; Handlers map[domain.JobType]Handler; Provider string; Now func() time.Time; PollEvery time.Duration; Log *slog.Logger}) *Runner`, with `Start(ctx)` and `Shutdown(ctx) error`. `Wake()` is called by `Service.Create`, through a `Notifier` function field.
  - `ItemLoop` in `loop.go`: `RunItems(ctx, job *domain.Job, rc RunContext, cfg LoopConfig, list ListFunc, process ProcessFunc) error`. It pages, runs up to `Concurrency` items, checkpoints after each page, and implements the §9.4 pause rule. `LoopConfig{Concurrency, MaxConsecutiveFailures=10, MaxPauses=3; PauseFor=1*time.Minute; Sleep}`. `ListFunc(ctx, token string) (items []pipeline.WorkItem, next string, err error)`. `ProcessFunc(ctx, pipeline.WorkItem) (pipeline.Outcome, error)`.

- [ ] **Step 1: Write the failing tests.**
  - `TestCreateValidation` table: unknown type, provider, or taxonomy gives `ErrValidation`; reclassify with an empty scope gives `ErrValidation`; restore without `snapshot_id` gives `ErrValidation`; `from_dry_run` naming a real job, a running job, or a job of another type gives `ErrConflict`/`ErrValidation`.
  - `TestDraftTaxonomy`: real backfill on `draft` gives `ErrConflict`; dry run succeeds.
  - `TestAutomationGate` table, automation off: real sync gives `ErrAutomationDisabled`; dry-run sync, real backfill, reclassify, snapshot, restore all succeed.
  - `TestSyncDedup`: active sync returns the existing job with `created=false`.
  - `TestIdempotencyKey`: same key returns the same job with `created=false`.
  - `TestCancel` table: running goes to `cancelling` then `canceled`; queued goes to `canceled`; finished gives `ErrConflict`.
  - `TestRunnerOneWriteJobAtATime`: 2 real jobs never overlap; a dry run overlaps them.
  - `TestRunnerResumesOnStart`: a `running` job in the store restarts with its checkpoint.
  - `TestShutdownCheckpoints`: `Shutdown` closes `Stopping`, in-flight items finish, status remains `running`, and a new runner resumes it.
  - `TestStatusFromProgress`: `Failed > 0` gives `succeeded_with_errors`.
  - `TestRunItemsResume`, `TestRunItemsPauseRule`: 10 consecutive errors pauses; a success resets; 3 pauses gives `failed`, and it can be resumed by re-queuing.
  - `TestRunItemsCounters`: outcomes map to `Changed`, `Skipped`, `NeedsReview`, `Failed`.
- [ ] **Step 2: Run `go test -race ./internal/core/jobs/`.** Expected: FAIL, undefined `NewService`.
- [ ] **Step 3: Implement** with fake handlers in tests.
- [ ] **Step 4: Run `go test -race ./internal/core/jobs/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add job service and runner`.

---

### Task 11: Sync, backfill, reclassify, dynamic taxonomy

**Files:**
- Create: `internal/core/taxonomy/{resolver,service,propose}.go`, `internal/core/jobs/{sync,backfill,reclassify}.go`
- Test: `internal/core/taxonomy/resolver_test.go`, `propose_test.go`, `internal/core/jobs/sync_test.go`, `backfill_test.go`, `reclassify_test.go`

**Interfaces:**
- Consumes: Task 5 pipeline, Task 10 `Handler`, `RunItems`.
- Produces:
  - `taxonomy.Resolve(ctx, m ports.CollectionManager, t domain.Taxonomy, provision bool, onCreate func(domain.ID)) (map[string]domain.ID, []string, error)`, returning the resolved keys and missing names. Case-insensitive, root-level only. Among duplicate root names, the lowest ID in sorted order wins, with a warning.
  - `taxonomy.Service`: `New(ports.TaxonomyStore)`, `Get`, `List`, `Put(ctx, id, t, ifVersion)`, `Seed(ctx, []domain.Taxonomy, log)`. `Put` validates: `min_tags ≤ max_tags`; activating requires collections and one review collection.
  - `taxonomy.Propose(ctx, p ports.TaxonomyProposer, samples []ports.Subject, existing []string, t domain.Taxonomy) (domain.Taxonomy, error)`: stores proposed collections on the draft and adds a `needs_review` review collection if missing.
  - `jobs.Handlers(Deps) map[domain.JobType]Handler`, `Deps{Provider ports.Provider; Pipeline *pipeline.Pipeline; Taxonomies *taxonomy.Service; Proposer ports.TaxonomyProposer; Store ports.Store; Now func() time.Time; Concurrency, TagVocabSize int; DurableWait time.Duration; Sleep; Snapshots SnapshotTaker}`. `SnapshotTaker` is an interface here, implemented in Task 12: `Take(ctx, provider string, scope domain.Scope, jobID string) (string, error)`.
  - Setting key `first_backfill_done:<provider>`, whose value is an RFC 3339 time.
  - `GET /v1/jobs/{id}/proposed-tags` data: `jobs.ProposedTags(ctx, store, jobID) ([]ProposedTag, error)`, `ProposedTag{Name string; Count int}`.

- [ ] **Step 1: Write failing `taxonomy` tests.** `TestResolve`: case-insensitive match; nested same-name ignored; duplicate root names are deterministic across runs; missing names returned with `provision=false`; created with `provision=true`, calling `onCreate`. `TestPropose`: draft gets at most `MaxCollections`; a review collection is added; a provider's existing names are passed through. `TestSeedDoesNotOverwrite`: logs a warning on difference.
- [ ] **Step 2: Write failing job tests** with `fake.Provider`, `fake.Classifier`, `memory.New`.
  - `TestSyncFirstRunBaseline`: inbox item filed, others pinned `baseline`, classifier called once.
  - `TestSyncWatermark`: `Since = watermark - 48h`; advances to job start only on success. Failed items keep it unchanged.
  - `TestSyncFullScanWithoutCapability`.
  - `TestSyncHighlights`: new highlight queues a tags-only run for its parent. A provider without highlights skips it.
  - `TestBackfillDryRunNoChanges`: 0 writes, no snapshot, no collections created, decisions stored, first-backfill setting unset.
  - `TestBackfillSequence`: order is snapshot, durable wait, provision, process. A snapshot failure means 0 writes.
  - `TestBackfillMissingCollectionsNoProvision`: `provision_missing_collections: false` fails with `ErrValidation` before any write.
  - `TestBackfillResume`: interrupted after page 1, a new run starts from the checkpoint, and the classifier isn't called again for page 1.
  - `TestFromDryRun`: uses saved decisions; 0 classifier calls; `PinCutoff` is the dry run's start.
  - `TestProposedTagsAndApproval`: dry run reports `{name,count}`. Apply with `approved_new_tags` writes only mapped tags and existing ones.
  - `TestBackfillWithoutPlanNoNewTags`: real run without `from_dry_run` adds only existing tags.
  - `TestFirstBackfillIgnoresOldPins`, `TestLaterBackfillRespectsUserPins`, `TestFirstBackfillFlagSetOnSuccessOnly`.
  - `TestDynamicProposeOnly`: `propose_only` stores the proposal, 0 `Classify` calls.
  - `TestDynamicDryRunClassifiesAgainstProposal`.
  - `TestOldCollectionsNotDeleted`.
  - `TestReclassifyScope`: only matching collections, tags, IDs; `Force` true; takes a snapshot limited to the scope; `include_pinned` behavior.
  - `TestTaxonomyChangeDoesNotResortOnSync`.
- [ ] **Step 3: Run `go test -race ./internal/core/...`.** Expected: FAIL, undefined `Resolve`.
- [ ] **Step 4: Implement.** Vocabulary comes from `TagLister` when available; otherwise it's built from listed bookmarks with a first pass over pages. Samples for proposal are evenly spaced through the listing up to `sample_size`.
- [ ] **Step 5: Run `go test -race ./internal/core/...`.** Expected: PASS.
- [ ] **Step 6: Commit** `feat: add sync, backfill, reclassify, and dynamic taxonomy`.

---

### Task 12: Snapshot and restore

**Files:**
- Create: `internal/core/jobs/{snapshot,restore}.go`
- Test: `internal/core/jobs/snapshot_test.go`, `restore_test.go`

**Interfaces:**
- Consumes: `ports.SnapshotStore`, `ports.CollectionManager`, `BookmarkPatch.SetTags`.
- Produces: `SnapshotService` implementing `SnapshotTaker`, plus handlers for `snapshot` and `restore`, added to `Handlers`. `RestoreDiff{Bookmark domain.ID; FromCollection, ToCollection domain.ID; FromTags, ToTags []string}`, stored as decision records. `SnapshotService.Export(ctx, id string, w io.Writer) error` streams JSON.

- [ ] **Step 1: Write the failing tests.**
  - `TestSnapshotRecordsAll`: every in-scope bookmark's collection and tags, plus the collection list. Prunes to retention and keeps those in use.
  - `TestSnapshotWaitsDurable`: job waits `DurableWait` through the injected sleep before returning.
  - `TestRoundTrip`: snapshot, backfill, restore; every bookmark's collection and exact tags match the snapshot, and Rainwise-created empty collections are gone.
  - `TestRestoreDryRun`: 0 writes; a diff for each changed bookmark, including post-snapshot manual moves.
  - `TestRestoreOverwritesManualChanges`.
  - `TestRestoreSkipsDeleted`: reports `skipped`, not a failure. `TestRestoreLeavesNewerBookmarks`.
  - `TestRestoreRecreatesDeletedCollection`: a collection the user deleted is recreated by name, and the bookmark returns to it.
  - `TestRestoreKeepsNonEmptyCollection`: a created collection now holding a user's bookmark stays.
  - `TestRestoreTakesOwnSnapshot`, and undoing a restore with that snapshot works.
  - `TestRestoreUpdatesState`: no false pins on the next sync; sync skips restored bookmarks; `RestoredAt` set.
  - `TestRestoreResetsFirstBackfill`: a snapshot taken before the first backfill clears the setting; a later one doesn't.
  - `TestRestoreResume`: interrupted, then resumed, with the same result.
  - `TestExportJSON`: output parses and contains every item.
- [ ] **Step 2: Run `go test -race ./internal/core/jobs/ -run 'Snapshot|Restore|RoundTrip|Export'`.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the same command.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add snapshot and restore`.

---

### Task 13: Event inbox worker

**Files:**
- Create: `internal/core/events/worker.go`
- Test: `internal/core/events/worker_test.go`, `canonical_test.go`

**Interfaces:**
- Consumes: `ports.EventStore`, `Pipeline.Process`, `events.NormalizeCanonical` from Task 9, `ports.URLResolver`.
- Produces: `NewIngestor(store, automationEnabled bool, now) *Ingestor` with `Ingest(ctx, []domain.Event) error`, which stores events as `pending` or `held`. `NewWorker(WorkerDeps{Store; Pipeline; Provider ports.Provider; Taxonomies; OnMoveRetag bool; Now; Sleep; PollEvery time.Duration; Log}) *Worker`, with `Run(ctx)`.

- [ ] **Step 1: Write the failing tests.**
  - `TestIngestDedup`: the same event twice makes one row.
  - `TestAutomationOffHolds`: stored as `held`, 0 pipeline calls. `TestHeldDropped`: older than 7 days is dropped.
  - `TestBookmarkCreated`: runs with `TriggerWebhook`.
  - `TestHighlightCreated`: `TagsOnly`, with parent resolved.
  - `TestOwnMoveIgnored`: event collection equals `KnownCollection`: no pin, no writes.
  - `TestUserMovePins`: different: pinned `user`; retag only when `OnMoveRetag`.
  - `TestResolveByURL`: URL-only events resolve through `URLResolver`. A provider without it, or no match, marks the event `failed`, with no crash.
  - `TestRetryBackoff`: failure sets `NextAttempt`; after 5 attempts status is `failed`.
  - `TestRestartReclaims`: a claimed but unfinished event is claimable after restart.
  - `TestForgedPayloadIgnored`: fields beyond the reference, like a false title or tags in metadata, don't reach the classifier.
- [ ] **Step 2: Run `go test -race ./internal/core/events/`.** Expected: FAIL, undefined `NewWorker`.
- [ ] **Step 3: Implement.** Add to `EventStore`, in both stores and the suite: `ResetClaimed(ctx) error`, called at worker start.
- [ ] **Step 4: Run `go test -race ./internal/core/events/ ./internal/adapters/store/...`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add webhook event worker`.

---

### Task 14: REST API

**Files:**
- Create: `internal/httpapi/{server,auth,errors,webhooks,jobs,resources,snapshots}.go`
- Test: `internal/httpapi/server_test.go`

**Interfaces:**
- Consumes: `jobs.Service`, `jobs.ProposedTags`, `SnapshotService.Export`, `taxonomy.Service`, `events.Ingestor`, `ports.EventNormalizer`, stores.
- Produces: `New(Deps{APIToken string; WebhookSecrets map[string]string; Normalizers map[string]ports.EventNormalizer; Jobs *jobs.Service; Taxonomies *taxonomy.Service; Snapshots *jobs.SnapshotService; Ingestor *events.Ingestor; Store ports.Store; Providers []ProviderInfo; Classifiers []ClassifierInfo; Log *slog.Logger}) http.Handler`. Uses `net/http` method patterns. Routes and bodies follow §7.

- [ ] **Step 1: Write failing table-driven tests with `httptest`.**
  - `TestAuth`: no token or wrong token on `/v1/jobs` gives `401`; `/healthz` needs none; webhook needs its own secret, not the API token, as `Authorization: Bearer` or `X-Rainwise-Secret`.
  - `TestErrorShape`: every error body matches `{"error":{"code","message","details"}}`.
  - `TestStatusMapping` table: `ErrNotFound` 404; `ErrConflict` 409; `ErrValidation` 422; `ErrAutomationDisabled` 409 with code `automation_disabled`; malformed JSON 400; body over 1 MB 413; unknown error 500 with no internal detail.
  - `TestWebhook`: valid 202; duplicate 202 with no second row; unknown provider 404; malformed 400; unknown type 422; provider with no normalizer 404.
  - `TestCreateJob`: new 202 with `Location`; duplicate sync 200 with the existing job. Unknown fields 400.
  - `TestGetListCancelJob`: pagination returns `next_cursor`. Cancel returns 202, or 409 when finished.
  - `TestDecisionsAndProposedTags`.
  - `TestProviders`, `TestClassifiers`: capabilities and roles included, no secrets.
  - `TestTaxonomies`: `GET` list and item; `PUT` bumps version; stale `If-Match` 409; invalid body 422.
  - `TestProposals`, `TestSnapshotsAndExport`.
  - `TestHealthz`: 200 when store responds; 503 otherwise.
- [ ] **Step 2: Run `go test ./internal/httpapi/`.** Expected: FAIL, undefined `New`.
- [ ] **Step 3: Implement.** Token comparison uses `crypto/subtle`. Add a recover middleware and a structured request log with no authorization headers.
- [ ] **Step 4: Run `go test ./internal/httpapi/`.** Expected: PASS.
- [ ] **Step 5: Commit** `feat: add REST API`.

---

### Task 15: Scheduler, wiring, end-to-end

**Files:**
- Create: `internal/scheduler/scheduler.go`, `cmd/rainwise/main.go`, `cmd/rainwise/wire.go`, `internal/e2e/e2e_test.go`
- Test: `internal/scheduler/scheduler_test.go`, `cmd/rainwise/wire_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces: `scheduler.New(every, create func(ctx) error, tickerFn, log)`, `Run(ctx)`. `Build(cfg *config.Config, deps BuildDeps) (*App, error)` in package `main`, where `BuildDeps{Provider ports.Provider; Classifier ports.Classifier}` holds optional test overrides. `App` has `Handler http.Handler`, `Start(ctx)`, `Shutdown(ctx)`. CLI commands: `rainwise serve --config PATH`, `rainwise config validate --config PATH`. Exit code 0 on success, 1 on a config error, with the reason on stderr.

- [ ] **Step 1: Write failing wiring tests** for §6.4 startup rules.
  - `TestBuildRejects` table: dynamic taxonomy with Jev as `proposer`; `auto_create` with Jev as `classifier`; 256 fixed collections with Jev; unknown provider or classifier `type`. Each error names the setting.
  - `TestBuildAccepts`: fixed taxonomy with Jev; dynamic with LLM `proposer` and Jev `classifier`.
  - `TestSchedulerNeedsAutomation`: `scheduler.enabled` with automation off logs a warning and never creates jobs.
  - `TestSchedulerTick`: creates a sync on each tick; an error doesn't stop it.
- [ ] **Step 2: Write failing `e2e_test.go`,** using `httptest` with `App`, fake provider, fake classifier, and a real SQLite file in `t.TempDir()`. This is the definition of done from §16:
  - `TestDryRunThenApplyThenRestore`: dry run makes no provider writes; apply sorts; restore matches the snapshot exactly.
  - `TestManualMoveSurvives`: a move survives later sync, event, and backfill.
  - `TestInboxFiledBySync` and `TestInboxFiledByEvent`.
  - `TestKillAndResume`: stop mid-backfill, build a new `App` on the same file, and the job finishes without re-classifying finished items.
  - `TestDeployChangesNothing`: automation off, sync returns 409, a webhook is held, 0 writes.
  - `TestWebhookLoop`: Rainwise's own move, echoed back as an event, makes 0 extra writes and 0 classifier calls.
- [ ] **Step 3: Run `go test -race ./internal/scheduler/ ./cmd/... ./internal/e2e/`.** Expected: FAIL, undefined `Build`.
- [ ] **Step 4: Implement** wiring, scheduler, and signal handling (SIGTERM calls `Shutdown`).
- [ ] **Step 5: Run `go vet ./... && go test -race ./...`.** Expected: PASS.
- [ ] **Step 6: Commit** `feat: wire the service and add end-to-end tests`.

---

### Task 16: Packaging, deployment, docs

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `examples/config.yaml`, `deploy/docker-compose/{docker-compose.yml,config.yaml}`, `deploy/cloudflare/{wrangler.jsonc,worker.js,Dockerfile,entrypoint.sh,litestream.yml,README.md}`, `docs/live-checks.md`, `docs/ifttt.md`, `README.md`
- Test: `cmd/rainwise/examples_test.go`

**Interfaces:**
- Consumes: `config.Load`, `Build`.
- Produces: the deployment layout from §13. `deploy/cloudflare/worker.js` forwards traffic, and its 5-minute cron creates a sync job (a `409 automation_disabled` is fine) and keeps the container awake while `GET /v1/jobs` shows an active job.

- [ ] **Step 1: Write the failing test.** `TestExampleConfigsValid`: `examples/config.yaml` and `deploy/docker-compose/config.yaml` pass `config.Load` and `Build` with fake env values. `TestExampleFixedNineCollections`: the example has the 9 collections from §2, with Needs Review as `role: review`, `min_tags 2`, `max_tags 5`, and `automation.enabled: false`. `TestNoAuthorCollectionsInCode`: those names appear in no `.go` file outside tests.
- [ ] **Step 2: Run `go test ./cmd/rainwise/ -run Example`.** Expected: FAIL, config missing.
- [ ] **Step 3: Write the configs, images, and Worker.** Root `Dockerfile` is multi-stage: `golang:1.27-alpine` builds with `CGO_ENABLED=0`, and a distroless static final image runs as non-root. The Cloudflare image adds a pinned Litestream release and an `entrypoint.sh` that restores before starting and wraps the app. No Cloudflare file is referenced from Go code.
- [ ] **Step 4: Write `docs/live-checks.md`.** One checkbox section per item in §3.4, with the exact command and what each result means for config (such as setting `incremental_since: true`). Add live definition-of-done items 1, 2, 3, and 5 from §16, using a throwaway Raindrop account for writes. State plainly that none of these have been run.
- [ ] **Step 5: Write `README.md` and `docs/ifttt.md`.** README covers: what it does, quick start, the recommended sequence (dry run, review `/decisions` and `/proposed-tags`, apply with `from_dry_run` and `approved_new_tags`, inspect, turn on automation), restore, the privacy note, the prompt injection note, what happens if state is lost, and how to add a provider or classifier. `docs/ifttt.md` covers trigger setup and the §7.2 payload, and flags which fields are unverified.
- [ ] **Step 6: Run `go test ./... && docker build -t rainwise:dev . && docker run --rm rainwise:dev config validate --config /etc/rainwise/config.yaml`.** Expected: tests pass; image builds; validate exits non-zero only for missing env vars, naming the variable. If Docker isn't available, report that step as not run.
- [ ] **Step 7: Commit** `feat: add packaging, deployment config, and docs`.
