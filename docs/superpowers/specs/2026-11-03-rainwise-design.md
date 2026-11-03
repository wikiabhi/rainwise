# Rainwise: High-Level Design

**Status:** Approved
**Date:** 2026-11-03

## 1. Intent

Raindrop libraries get cluttered, and tending them by hand is too much effort. Lots of folders is bad, and a flat pile is bad too. Rainwise keeps a library organized with a small set of useful collections, plus tags that carry the finer detail, without ongoing manual work.

Rainwise is a generic, reusable repo. The author's own collections and rules are one example config. Someone else should be able to run it with their own taxonomy, classifier, or bookmark manager.

### Goals

- Sort every bookmark into exactly one collection and give it 2–5 meaningful tags (limits configurable).
- Share one processing pipeline across webhooks, scheduled sync, and one-time backfill.
- Swap bookmark providers and classifiers through adapters and config, with no core changes.
- Make the first run safe: dry run first, snapshot before writes, restore afterwards.
- Treat manual moves after the first backfill as final.

### Non-goals

- Multi-user or hosted SaaS use. Each deployment serves one account.
- A web UI. Dry-run results are read through the API.
- Deleting bookmarks or tags, or merging existing tags.
- Queues, microservices, or web frameworks.

### Anti-goals: failure even if it technically works

- Quietly removing a user's tags or undoing a manual move.
- Changing the library before the user has opted in.
- Growing into a new kind of clutter through tag synonyms or too many collections.
- An outage sending good bookmarks to Needs Review.
- Provider-specific or author-specific logic in core code.

### Decision record

Decisions from the user are in section 2. Everything marked **(design call)** elsewhere is the author's proposal and is open to overrule.

## 2. Decisions Made by the User

| Topic | Decision |
|---|---|
| Default taxonomy mode | Predefined collections. AI-generated collections and tags are optional. |
| AI mode start | A dry run proposes a collection list for approval. |
| First backfill | May move any bookmark, including ones already in a collection. |
| Safety | Backup and restore are required. |
| Rules | One collection, 2–5 tags, prefer existing tags, avoid synonyms and duplicates. Use title, description, URL, and optionally fetched content. Never classify solely by domain. Preserve user tags. Low confidence goes to Needs Review. |
| Genericity | The nine collections and rules are an example config, not product behavior. |
| Classifier | Configurable: an open-source model on OpenRouter, or a small model via Cloudflare AI Gateway. Jev is optional. The user has no direct Jev key. |
| Deployment | Small cloud container, with vendor-neutral code and deployment config kept separate. The user will deploy on Cloudflare. |

Example collections for `examples/config.yaml`: AI Engineering & Agents, System Design & Architecture, Backend & Software Engineering, Interview Preparation, Career & Opportunities, Developer Tools & Open Source, Learning & Reading Lists, Personal & Life, Needs Review.

## 3. Provider Facts That Shaped the Design

Checked against documentation on 2026-11-03. Items in section 3.4 are unconfirmed and must be tested first.

### 3.1 Raindrop

- **No native webhooks are documented.** Events can come from [IFTTT](https://ifttt.com/raindrop) triggers: new item, new highlight, and item moved to collection. These appear to be polled, so "realtime" may mean minutes.
- **Pagination:** page-based, [50 per page at most, no cursor](https://developer.raindrop.io/v1/raindrops/multiple). No sort by last update. Collection `0` is all except Trash, `-1` is Unsorted, `-99` is Trash.
- **Incremental sync:** only the `lastUpdate:` [search filter](https://help.raindrop.io/filters.md), with one-day granularity.
- **Deletions:** no deletion feed.
- **Highlights:** [global, per-collection, and per-raindrop reads](https://developer.raindrop.io/v1/highlights).
- **Writes:** bulk update appends tags and can move items, but does not support collection `0`. No way to attribute a change to an app.
- **Rate limit:** [120 requests per minute](https://developer.raindrop.io/llms-full.txt) per user.
- **Auth:** a [test token](https://developer.raindrop.io/v1/authentication/token) is enough for one's own account and does not expire.
- **Backups:** native backups are HTML/CSV exports. No restore API was found.

### 3.2 Jev

- [`POST /v1/systemone`](https://docs.typesafe.ai/api/system-one.md) with bearer auth. Not OpenAI-compatible. No Go SDK.
- Choice questions take up to 255 options and return one winner with calibrated confidence. There is no multi-select and no way to propose new labels.
- Yes/no (`noul`) questions return a probability, which can drive tag selection from a known list.
- Direct access is gated by early access. [OpenRouter lists it](https://openrouter.ai/typesafe/jev-1.13-20260917). Cloudflare access is reported by [one third-party page](https://opentweet.io/jev/api-access) only.

### 3.3 LLM gateways and Cloudflare Containers

- [OpenRouter](https://openrouter.ai/docs/guides/features/structured-outputs) supports `response_format` with JSON schema, per endpoint. `require_parameters: true` is needed, or [the schema can be silently ignored](https://openrouter.ai/blog/insights/confidence-thresholds-for-model-escalation-routing/).
- [Cloudflare AI Gateway](https://developers.cloudflare.com/ai-gateway/usage/rest-api/) has an OpenAI-compatible route. Workers AI (`@cf/`) models don't use that schema on this route.
- [Cloudflare Containers](https://developers.cloudflare.com/containers/faq/) have ephemeral disks, [sleep when idle](https://developers.cloudflare.com/containers/container-class/), and [can restart at any time](https://developers.cloudflare.com/containers/platform-details/architecture/).

### 3.4 Verify before building on these

Milestone 0 is a short spike against live services. Each item has a fallback, so none blocks the design.

| # | Unknown | Fallback |
|---|---|---|
| 1 | Does `lastUpdate:` work through the API `search` parameter? | Full scan each sync. 5,000 bookmarks is 100 requests. |
| 2 | Do API writes bump `lastUpdate`? | The input hash makes this harmless either way. |
| 3 | Which fields do IFTTT triggers expose? Is the bookmark ID included? | The canonical payload accepts a URL, which is resolved through search. |
| 4 | Is there a Raindrop tag-list endpoint, and is it usable? | Build the vocabulary from listed bookmarks. |
| 5 | How do OpenRouter's Jev listing and any Cloudflare route behave? | Jev direct when access arrives. The Jev adapter stays disabled until proven. |
| 6 | Does the chosen Cloudflare route enforce JSON schema? | Validate and retry, which happens anyway. |
| 7 | Can Litestream confirm replication on demand? | A configured wait after the snapshot (section 11). |
| 8 | How do rate-limit headers look with a test token? | Client-side limiter at 100 per minute. |

## 4. Architecture

A modular monolith, using ports and adapters.

```
 IFTTT / other sources      cron or scheduler      operator / scripts
          |                        |                       |
          v                        v                       v
 +---------------------------------------------------------------+
 | Trigger layer: net/http REST API, webhook ingest, scheduler   |
 +---------------------------------------------------------------+
          | events                 | jobs                  | config
          v                        v                       v
 +---------------------------------------------------------------+
 | Core (no vendor imports)                                      |
 |  Job runner --> Pipeline --> Decision validator --> Applier   |
 |  Taxonomy service | Snapshot/restore | Tag normalizer         |
 +---------------------------------------------------------------+
     |  ports            |  ports                |  port
     v                   v                       v
 +-------------+   +---------------------+   +------------------+
 | Providers   |   | Classifiers         |   | Store            |
 | - raindrop  |   | - llm (OpenAI API)  |   | - sqlite         |
 | - (future)  |   | - jev               |   | - memory (tests) |
 +-------------+   | - rules (post-MVP)  |   +------------------+
                   +---------------------+
```

**Dependency rule:** `internal/core` imports only the standard library and `internal/domain`. Adapters depend on ports, never the reverse. A single composition root in `cmd/rainwise` reads config and wires adapters. A test enforces the import rule.

## 5. Domain Models and Go Interfaces

The code below is illustrative. Names can change, but the boundaries should hold.

### 5.1 Models

```go
// ID is provider-qualified, written as "raindrop:12345".
type ID struct{ Provider, Local string }

type Bookmark struct {
    ID          ID
    URL         string
    Title       string
    Description string
    Excerpt     string
    Tags        []string
    Collection  ID
    CreatedAt   time.Time
    UpdatedAt   time.Time
    Metadata    map[string]string // opaque to core
}

type Highlight struct {
    ID        ID
    Bookmark  ID
    Text      string
    Note      string
    Tags      []string
    CreatedAt time.Time
}

type Collection struct {
    ID          ID
    Name        string
    Description string
    Parent      *ID
}

type EventType string // bookmark.created | highlight.created | bookmark.moved

type Event struct {
    Provider   string
    Type       EventType
    Resource   ResourceRef // an ID, or a URL to resolve
    Bookmark   *ResourceRef // parent, for highlight events
    OccurredAt time.Time
    DedupKey   string
    Metadata   map[string]string
}
```

### 5.2 Provider ports

```go
type BookmarkReader interface {
    GetBookmark(ctx context.Context, id ID) (Bookmark, error)
    ListBookmarks(ctx context.Context, q ListQuery) (Page[Bookmark], error)
}

type BookmarkWriter interface {
    UpdateBookmark(ctx context.Context, id ID, p BookmarkPatch) (Bookmark, error)
}

// BookmarkPatch makes additive intent explicit. AddTags is the only tag
// operation the pipeline uses. SetTags is reserved for restore.
type BookmarkPatch struct {
    Collection *ID
    AddTags    []string
    SetTags    []string
}

type Provider interface {
    Name() string
    Capabilities() ProviderCapabilities
    BookmarkReader
    BookmarkWriter
}

// Optional ports, discovered by type assertion and declared in capabilities.
type HighlightReader interface {
    ListHighlights(ctx context.Context, bookmark ID) ([]Highlight, error)
    ListAllHighlights(ctx context.Context, q ListQuery) (Page[Highlight], error)
}
type CollectionManager interface {
    ListCollections(ctx context.Context) ([]Collection, error)
    CreateCollection(ctx context.Context, c Collection) (Collection, error)
    DeleteCollectionIfEmpty(ctx context.Context, id ID) (bool, error)
}
type TagLister interface {
    ListTags(ctx context.Context) ([]TagCount, error)
}
type EventNormalizer interface {
    Normalize(ctx context.Context, headers http.Header, body []byte) ([]Event, error)
}

type ProviderCapabilities struct {
    Highlights, CreateCollections, NativeWebhooks bool
    IncrementalSince, CursorPagination, TagListing bool
    InboxCollection *ID // Raindrop: Unsorted. Nil if there's no such concept.
}

type ListQuery struct {
    Collection *ID
    Since      *time.Time // honored only with IncrementalSince
    PageToken  string     // opaque: a page number or a cursor
    Limit      int
}
```

Pagination and rate-limit quirks stay inside the adapter. Core treats `PageToken` as opaque and stores it in checkpoints.

### 5.3 Classifier ports

```go
type Classifier interface {
    Name() string
    Capabilities() ClassifierCapabilities
    Classify(ctx context.Context, r ClassifyRequest) (Decision, error)
}

// Optional: needed only for AI mode's proposal step.
type TaxonomyProposer interface {
    ProposeTaxonomy(ctx context.Context, r ProposeRequest) (TaxonomyProposal, error)
}

type ClassifierCapabilities struct {
    FixedChoice, ProposeCollections, ProposeTags, CalibratedConfidence bool
    MaxChoices int
}

type ClassifyRequest struct {
    Subject     Subject          // bookmark and highlights, or one highlight
    Content     string           // fetched text, untrusted, may be empty
    Taxonomy    TaxonomySnapshot // allowed collections and rules
    TagVocab    []string         // most-used tags, capped
    TagsWanted  int              // 0 means don't suggest tags
    WantCollection bool          // false for highlights and pinned bookmarks
}

type Decision struct {
    Collection CollectionChoice // an existing taxonomy key, or a proposed name
    Tags       []TagSuggestion  // each with an optional score
    Confidence float64          // 0..1
    Reasoning  string           // optional
    Model      ModelInfo        // classifier, model, version, token usage
}
```

Classifiers have no tools and make no writes. Only the pipeline's applier changes anything.

### 5.4 Store port

```go
type Store interface {
    Jobs() JobStore             // jobs, checkpoints, per-item decisions
    Events() EventStore         // inbox, dedup
    BookmarkStates() StateStore // section 10
    Snapshots() SnapshotStore
    Taxonomies() TaxonomyStore
}
```

## 6. Adapters

### 6.1 Raindrop provider

Maps Raindrop objects to domain models and owns retries, 429 handling, rate limiting, and the `0`/`-1`/`-99` collection IDs.

- `AddTags` must never lose an existing tag. The adapter may use the bulk endpoint scoped to one ID, or read, merge, then replace.
- Lists sort by `created` ascending for backfills, so inserts during paging land at the end.
- Trash is never listed. A 404 marks the bookmark deleted in local state.
- Auth is a token from an environment variable. OAuth is post-MVP.
- No native webhooks. `EventNormalizer` handles the canonical payload in section 7.2.

### 6.2 LLM classifier

One adapter for any OpenAI-compatible API, configured with base URL, model, API key env var, and extra headers or body fields. OpenRouter and Cloudflare AI Gateway are config entries rather than separate adapters.

- Requests JSON schema output. For OpenRouter, config sets `require_parameters: true`.
- Local validation is always on. Invalid output gets one retry, then an error.
- Confidence is self-reported, so it's uncalibrated. The threshold is set per classifier.
- The prompt is built from the taxonomy's rules and descriptions. It treats page content as data, and states that domain alone isn't enough.
- Implements `TaxonomyProposer`.

### 6.3 Jev classifier

Separate adapter that talks to the Jev question format, with base URL and auth configurable so a gateway can be used if it works (section 3.4).

- One `choice` question for the collection, with taxonomy descriptions as criteria.
- One `noul` question per candidate tag, for at most `max_tag_candidates` tags (default 100). Tags scoring above `tag_threshold` are chosen, highest first.
- `ProposeCollections` and `ProposeTags` are false. It doesn't implement `TaxonomyProposer`.

### 6.4 Selection and fallback

Each classifier has a role in config: `classifier`, `proposer`, `fallback`.

Startup fails with a clear message if:

- a dynamic taxonomy has no `proposer` implementing `TaxonomyProposer`;
- `auto_create` is on and the `classifier` can't propose collections;
- a taxonomy has more collections than the classifier's `MaxChoices`.

There is no silent downgrade. The fallback runs only after the primary errors, never for low confidence. Its use is recorded on the decision. A rule-based classifier is post-MVP, and the interface already allows it.

## 7. REST API

All JSON. All routes need `Authorization: Bearer <token>` except `/healthz` and webhooks. Errors look like:

```json
{"error": {"code": "capability_unsupported", "message": "...", "details": {}}}
```

Status codes: `200`, `201`, `202` (accepted, async), `400` (malformed), `401`, `404`, `409` (conflict), `422` (valid JSON, invalid meaning), `429`, `500`, `503`. Lists take `limit` and `cursor`, and return `next_cursor`.

### 7.1 Routes

| Method and path | Purpose |
|---|---|
| `GET /healthz` | Liveness and store check |
| `POST /v1/webhooks/{provider}` | Ingest events. `202` once saved. |
| `POST /v1/jobs` | Create a job. `202`. |
| `GET /v1/jobs`, `GET /v1/jobs/{id}` | List jobs, or get status and progress |
| `DELETE /v1/jobs/{id}` | Cancel. `202`, or `409` if already finished. |
| `GET /v1/jobs/{id}/decisions` | Per-bookmark plan or result |
| `GET /v1/jobs/{id}/proposed-tags` | A dry run's new tags, with counts |
| `GET /v1/providers` | Configured providers and capabilities |
| `GET /v1/classifiers` | Classifiers, roles, capabilities |
| `GET /v1/taxonomies`, `GET /v1/taxonomies/{id}` | Read taxonomies |
| `PUT /v1/taxonomies/{id}` | Replace a taxonomy. Bumps its version. `If-Match` is optional. |
| `GET /v1/taxonomies/{id}/proposals` | Collections proposed but not created |
| `GET /v1/snapshots`, `GET /v1/snapshots/{id}` | List or read snapshots |
| `GET /v1/snapshots/{id}/export` | Download a snapshot as JSON |

The decisions, proposals, and snapshot routes extend the original brief, because dry runs and restore need them **(design call)**.

### 7.2 Webhook payload

Auth uses a per-source secret from an env var, sent as `Authorization: Bearer` or `X-Rainwise-Secret`, compared in constant time. A provider with native webhooks can verify signatures in its own normalizer. `{provider}` is the bookmark provider, not the delivery tool.

```json
{
  "type": "bookmark.created",
  "resource": {"id": "12345", "url": "https://example.com/post"},
  "bookmark": {"id": "12345", "url": "https://example.com/post"},
  "occurred_at": "2026-11-03T10:00:00Z",
  "event_id": "optional-source-id",
  "metadata": {"source": "ifttt"}
}
```

- At least one of `resource.id` and `resource.url` is required. `bookmark` is required only for `highlight.created`.
- Events are hints. Content and state are always re-fetched, so a forged or stale event can't inject data. Webhook fields are never sent to a classifier.
- Unknown event types return `422`.
- When `event_id` is absent, the dedup key is a hash of provider, type, resource, and `occurred_at`.

### 7.3 Jobs

```json
POST /v1/jobs
{
  "type": "backfill",
  "provider": "raindrop",
  "taxonomy": "default",
  "dry_run": true,
  "params": {"batch_size": 50, "propose_only": false},
  "scope": {"collections": [], "tags": [], "ids": []},
  "from_dry_run": null
}
```

```json
{
  "id": "job_01J...",
  "type": "backfill",
  "status": "running",
  "dry_run": true,
  "progress": {"total": 4210, "processed": 1500, "changed": 1200,
               "skipped": 250, "needs_review": 40, "failed": 10},
  "snapshot_id": null,
  "created_at": "...", "started_at": "...", "finished_at": null,
  "error": null
}
```

- **Statuses:** `queued`, `running`, `succeeded`, `succeeded_with_errors`, `failed`, `cancelling`, `canceled`.
- **Types:** `sync`, `backfill`, `reclassify` from the brief, plus `snapshot` and `restore` **(design call)**.
- A `POST` for a `sync` while one is queued or running returns `200` with that job. This keeps wake-up retries harmless.
- At most one write job runs per provider at a time. Others wait in `queued`.
- Dry runs can run alongside anything, sharing the rate limit.
- `from_dry_run` applies a reviewed plan as a whole. There are no per-item edit routes. A bookmark whose input hash has changed since then is classified again.
- **To correct a plan, move the bookmark by hand in the bookmark manager.** A dry run makes no provider writes, but it records each bookmark's observed collection in local state. Moving a bookmark after that is detected as a manual move on apply, so the planned move is dropped and its tag decision stands **(design call)**.
- `Idempotency-Key` is honored on `POST /v1/jobs`.

## 8. Taxonomy Design

The taxonomy is independent of provider and classifier. Modes are `fixed` and `dynamic`.

```yaml
taxonomies:
  default:
    mode: fixed
    collections:
      - key: backend
        name: Backend & Software Engineering
        description: Server-side engineering, databases, APIs, languages.
      - key: needs_review
        name: Needs Review
        description: Reserved for low-confidence items.
        role: review       # never offered to the classifier
    rules:
      min_tags: 2
      max_tags: 5
      min_confidence: 0.6
      allow_new_tags: true
      tag_aliases: {k8s: kubernetes, golang: go}
      prompt_extra: ""
    provision_missing_collections: true
    dynamic:                   # used only when mode is dynamic
      max_collections: 12
      sample_size: 1000
      auto_create: false
      auto_create_min_confidence: 0.85
```

### 8.1 Storage

YAML seeds a taxonomy on first boot. After that, the database wins, since `PUT` must persist. A difference from YAML logs a warning and doesn't overwrite **(design call)**.

### 8.2 Fixed mode

- The classifier may only choose from non-review collections.
- `provision_missing_collections` creates the taxonomy's own declared collections on the first real run, because the example list won't exist in a fresh library. "Never creates new collections" is read as "never beyond the list". With it off, missing collections fail the job with `422` before any writes **(design call: please check)**.
- Collections are matched by case-insensitive name at the root level. Taxonomy collections are flat in the MVP.

### 8.3 Dynamic mode

1. A dynamic taxonomy starts in `draft` with no collections.
2. A dry-run backfill samples up to `sample_size` bookmarks (compact title, description, tags), along with existing collection names, and asks the `proposer` for at most `max_collections`. Large samples are batched and merged.
3. The proposal is stored on the taxonomy and the dry run classifies against it. `propose_only: true` stops after the proposal, avoiding the cost.
4. The user edits it if needed and `PUT`s with `status: active`. Real runs refuse a `draft` taxonomy with `409`.
5. From then on it behaves like fixed mode.

A later bookmark that doesn't fit goes to Needs Review, with the classifier's suggested name recorded under `/proposals`. Only with `auto_create: true` can a collection be created during processing, and only if confidence is at or above `auto_create_min_confidence` and the `max_collections` cap isn't reached. This satisfies "only when justified, with limits" while keeping the default of "approve first".

### 8.4 Tags

- Normalized: trim, drop a leading `#`, lowercase, spaces to hyphens, apply `tag_aliases`. Existing user tags keep their exact form and are compared case-insensitively.
- `TagVocab` is the 200 most-used tags (configurable) and seeds the prompt, which asks for reuse.
- `max_tags` applies to the total. `room = max(0, max_tags - existing)`. A bookmark with 5 or more tags gets none added, and none removed.
- With `allow_new_tags: false`, or with Jev, only tags in the vocabulary are kept.
- `min_tags` is a target. The validator never invents tags to reach it. A shortfall is recorded on the decision.
- Normalizing also folds simple plurals to one form, so `llms` and `llm` match. New tags accepted during a run join that run's vocabulary, so later bookmarks reuse them **(design call)**.
- Every dry run reports each new tag with its count, so tag sprawl is visible before any write.

**Approving new tags for a backfill (user's decision, on the author's recommendation):**

- `GET /v1/jobs/{id}/proposed-tags` lists a dry run's new tags with counts.
- The apply job passes `params.approved_new_tags`, a map from each proposed tag to its final form, such as `{"ai-agents": "agents", "postgres": "postgres"}`. Anything left out is rejected. The map is applied to the saved plan without classifying again, then each bookmark's tags are de-duplicated and capped again.
- A real backfill or reclassify adds only existing tags and approved ones. Without `from_dry_run`, it adds existing tags only.
- Sync and webhook bookmarks follow `allow_new_tags`.

### 8.5 Validator

Applied to every decision regardless of classifier:

| Condition | Result |
|---|---|
| Collection isn't allowed | Review collection, reason recorded |
| Confidence below `min_confidence` | Review collection |
| Bookmark already in an allowed collection, low confidence | Stays put, tags still evaluated |
| Bookmark is pinned or `WantCollection` is false | Collection ignored |
| Provider lacks a capability | That part is dropped and recorded |
| Too many, duplicate, or invalid tags | Trimmed to `room` after normalizing |

The third row is deliberate: Needs Review doesn't pull a bookmark out of a good home **(design call)**.

## 9. Flows

### 9.0 Opting in to automation

`automation.enabled` (default `false`) is the one switch for writes nobody explicitly requested. It's config, not an API setting **(design call)**.

- **While off:** real `sync` jobs return `409 automation_disabled`, including those from a Cloudflare Worker cron, so deploying changes nothing. Dry-run syncs are allowed. Webhook events are authenticated and saved as `held`, and nothing is processed. Held events older than 7 days are dropped, since sync covers them.
- **While on:** scheduled syncs and webhook events write without a dry run.
- **Explicit jobs:** real `backfill`, `reclassify`, `snapshot`, and `restore` jobs count as opt-in on their own, and work either way.
- **Sync-only use:** turning automation on without a backfill is allowed. Existing bookmarks stay where they are through the baseline rule.
- **Snapshots:** sync and webhook writes take no snapshot, because they only add tags and file inbox items. Previous collection and tags are stored with each decision, for inspection.

### 9.1 Shared pipeline

`pipeline.Process(ctx, WorkItem, Options)` is the one code path for all triggers. `Options` carries `MoveAllowed`, `DryRun`, `Force`, and taxonomy.

1. Resolve the provider. Fetch the bookmark and its highlights. If gone, mark it deleted and stop.
2. Load state and detect manual moves (section 10).
3. Compute the input hash. Skip if already handled.
4. Decide whether a collection decision is wanted. Fetch content if needed (section 9.6).
5. Classify, with retry and fallback.
6. Validate.
7. Dry run: record the decision and stop.
8. Re-read the bookmark. If its collection changed since step 1, treat that as a manual move and don't move it.
9. Record the intended write in state, then apply the patch.
10. Record the result: the bookmark's collection and tags as written, input hash, taxonomy version, model.

When to move:

| Trigger | Bookmark location | Action |
|---|---|---|
| Backfill | Anywhere, not pinned | Collection and tags. May move. |
| Sync or webhook, new | Inbox (Unsorted) | Collection and tags |
| Sync or webhook, new | Any other collection | Saved there on purpose: pinned, tags only |
| Reclassify | In scope, not pinned | Collection and tags. `include_pinned` overrides. |
| Highlight event or change | Anywhere | Tags only |
| Sync, existing bookmark with changed input | Needs Review, not pinned | Classify again. May leave Needs Review. |
| Sync, existing bookmark with changed input | Anywhere else | Record the new hash. No classification, no change. |

The last two rows are **(design calls)**: an edited title shouldn't re-sort an accepted bookmark or spend money on every edit, while an improved Needs Review item should get another chance.

For a provider with no inbox, new bookmarks are classified and moved only if they have no collection **(design call)**.

### 9.1a Which pins bind which job

Every pin has a source: `baseline`, or `user` for a detected manual move or a new bookmark saved directly into a collection.

- **First backfill:** the user said it may move anything, and that manual moves become final after it. So it ignores every pin created before its cutoff. The cutoff is the job's start, or the dry run's start when using `from_dry_run`, so corrections made after a dry run are honored. Moves made during the job are honored too.
- **Later backfills and reclassify:** respect `user` pins and ignore `baseline` pins. `include_pinned` overrides this for reclassify.
- **Tracking:** `settings` stores when the first real backfill for a provider finished. A restore to a snapshot taken before that clears it, so the next backfill is a first backfill again.

### 9.2 Webhook

1. Authenticate. `EventNormalizer` produces canonical events.
2. Save to the inbox table, ignoring duplicates by dedup key. Return `202`.
3. A background worker claims pending events, one per bookmark at a time:
   - `bookmark.created`: runs the pipeline.
   - `highlight.created`: runs the pipeline in tags-only mode. Requires a confidence at or above `min_confidence`.
   - `bookmark.moved`: if the current collection matches the one last written, it's our own move and is ignored. Otherwise the bookmark is pinned. Tags change only if `on_move_retag: true` (default false).
4. Failed events retry with backoff, up to 5 attempts, then stay as `failed`. The next sync covers them.

### 9.3 Sync

Scheduled and also the safety net for webhooks. Runs under the built-in scheduler or from an external `POST /v1/jobs`.

1. Read the watermark. List bookmarks with `Since = watermark - 48h`, or do a full list if `IncrementalSince` is false.
2. Run each through the pipeline. The input hash removes repeats, including those caused by our own writes.
3. Read recent highlights, if supported, and queue tags-only runs for parents with new highlights.
4. Advance the watermark to the job's start time only on success.

**Baseline:** a first sync with no watermark records bookmarks outside the inbox as known and pinned with source `baseline`, without classifying them. Only inbox items are processed. This prevents a surprise bill and tag changes **(design call)**. Section 9.1a covers how backfills treat these pins.

Sync doesn't re-sort on taxonomy changes. That's what `reclassify` is for, so costs stay predictable.

### 9.4 Backfill

1. Create a snapshot unless dry-run. Wait until it's durable (section 11).
2. Provision missing collections unless dry-run.
3. Page through all bookmarks in order, saving a checkpoint after each page: page token and counters.
4. Process with bounded concurrency (default 4). Item failures are recorded and the job continues.
5. If 10 consecutive items fail on classifier errors, the job pauses for backoff, and after 3 pauses it fails. It stays resumable.
6. Old collections are left in place, even when empty. Rainwise never deletes them, except on restore (section 11).

The recommended sequence, also in the README: dry run, review `/decisions`, apply with `from_dry_run`, inspect, then turn on the scheduler.

### 9.5 Reclassify and restore

- **Reclassify:** same as backfill, with a required `scope` and `force` to bypass the hash. Respects user pins unless `include_pinned`. Takes a snapshot of its scope.
- **Restore:** see section 11.

### 9.6 Content fetching

`content_fetch: off | auto | always`, default `auto`. In `auto`, it fetches when title, description, and excerpt together fall below a configured length (default 80 characters).

- Only `http` and `https`. Private, loopback, and link-local addresses are blocked, including after redirects.
- Size cap (2 MB), timeout (10 s), text only, truncated before classification.
- **Temporary failure** (timeout, DNS, 5xx, 429): the item fails without recording its hash, so a later run retries it. After 3 attempts it's treated as permanent. This keeps a brief outage from sending bookmarks to Needs Review **(design call)**.
- **Permanent failure** (404, 403, blocked address, unreadable content): classify on the text available. If there's too little text to judge, send it to Needs Review.

**Privacy:** bookmark data goes to the configured LLM endpoint. The README says so plainly, and a local Ollama-style endpoint works through the same adapter.

**Prompt injection:** page content is untrusted. A malicious page can at worst influence one bookmark's collection (within the allowed list) and tags (normalized and capped). Classifiers have no tools.

## 10. State, Idempotency, Loop Prevention

The brief preferred no database. SQLite is required, for reasons specific to Raindrop's API: no write attribution, no cursor, and no restore. Core depends only on the `Store` port. Marker tags in Raindrop were rejected, because they add clutter to the tags this project is trying to clean up **(design call)**.

### 10.1 Tables

| Table | Contents |
|---|---|
| `bookmark_state` | ID, known collection, known tags, pin source and time, input hash, taxonomy version, model, status, pending write, processed time, restored time |
| `events` | Dedup key (unique), payload, status, attempts, next attempt |
| `jobs` | Type, params, status, progress, checkpoint, snapshot ID |
| `job_decisions` | Job, bookmark, input hash, decision, applied result |
| `snapshots`, `snapshot_items` | Metadata, bookmark collection and tags, plus created collection IDs |
| `taxonomies` | ID, version, status, body, proposals |
| `settings` | Watermarks, schema version |

Migrations are embedded and run on start. Driver: `modernc.org/sqlite`, to keep static CGO-free builds **(design call)**.

### 10.2 Input hash

Hash of URL, title, description, excerpt, and highlight text. Tags and collection are excluded, so our own writes don't change it.

A bookmark is skipped when input hash and taxonomy version already match and `Force` is off. If only the taxonomy version differs, sync still skips; only backfill and reclassify act on it. Section 9.1 covers a changed input hash.

### 10.3 Manual move detection

The pipeline compares the bookmark's current collection with `known collection`, which is updated after every read and write.

- **Different, and no pending write:** a manual move. It gets pinned with `source: user`.
- **Different, with a pending write matching it:** a crash happened after the write, before recording. The result is recorded and no pin is set.
- **Moving a bookmark out of Needs Review:** counts as a manual move.
- **Moving a bookmark to the inbox:** counts as a manual move, and it stays pinned.

### 10.4 Loop prevention

Layered, all provider-neutral:

- A move event that matches `known collection` is ignored.
- Input hash ignores `lastUpdate` bumps from our writes.
- A bookmark needing no change produces no write at all.
- Tag writes only add, so reruns converge. Moves are idempotent.
- Per-bookmark locking prevents concurrent webhook and sync processing.

### 10.5 Failure and recovery

| Failure | Behavior |
|---|---|
| Crash or SIGTERM mid-job | On SIGTERM, finish in-flight items, save checkpoint, exit. On start, `running` jobs resume from the checkpoint; pending events are reclaimed. |
| Provider 429 | Honor reset headers. Shared token bucket across all triggers. |
| Provider 5xx or network | Exponential backoff with jitter, 5 attempts |
| Classifier error | Retry, fallback if configured, otherwise item fails and is retried later. It's never sent to Needs Review. |
| Invalid classifier output | One retry, then treated as an error |
| Low confidence | Needs Review. Not a failure. |
| Lost last seconds of state | Re-check a few bookmarks. Writes are idempotent. |
| Entire state lost | Sync's baseline rule prevents mass changes. Pins and snapshots are gone. README covers this; the JSON export is the mitigation. |

## 11. Snapshot and Restore

Rainwise keeps its own snapshots, since Raindrop has no restore API.

**Taking one:** an automatic step before any real `backfill` or `reclassify`, or manually as a `snapshot` job. Each records all in-scope bookmark IDs with collection and tags, along with the collection list. Later jobs record collections they create.

**Durability:** a real job doesn't start changing things until the snapshot is safe. `state.durable_wait` (default `0` for a normal disk volume, `30s` in the Cloudflare config) applies after the snapshot commits. If Litestream offers an on-demand confirmation (section 3.4), the Cloudflare entrypoint uses that instead. Either way the snapshot can be exported as JSON, and the README recommends doing this before a first backfill.

**Restore job:**

- `POST /v1/jobs {"type": "restore", "params": {"snapshot_id": "..."}, "dry_run": true}`. A dry run shows the differences, and is the recommended first step.
- Sets each bookmark's collection and exact tags using `SetTags`.
- Bookmarks deleted since are skipped. Bookmarks created since aren't touched.
- Manual changes made after the snapshot **are overwritten** for bookmarks in it. The dry run shows them.
- Collections Rainwise created are deleted only if empty.
- Updates `known collection` and `known tags` so this doesn't look like manual moves. Sets `restored time`, so sync leaves those bookmarks alone until a new backfill or reclassify.
- Resumable, and takes its own snapshot first, so a restore can be undone.

Retention: keep the last 10 snapshots, configurable. A snapshot referenced by a running job isn't deleted.

## 12. Configuration

YAML with `${ENV}` expansion. Secrets only ever come from the environment. Unknown keys fail startup.

```yaml
server: {addr: ":8080", api_token: ${RAINWISE_API_TOKEN}}
state: {sqlite_path: /data/rainwise.db, durable_wait: 0s}
automation: {enabled: false}                   # section 9.0
scheduler: {enabled: false, sync_every: 30m}   # timer only; needs automation
processing:
  provider: raindrop
  taxonomy: default
  concurrency: 4
  content_fetch: auto
  on_move_retag: false
providers:
  raindrop:
    type: raindrop
    token: ${RAINDROP_TOKEN}
    webhook_secret: ${RAINDROP_WEBHOOK_SECRET}
    requests_per_minute: 100
classifiers:
  openrouter:
    type: llm
    base_url: https://openrouter.ai/api/v1
    model: ${RAINWISE_MODEL}
    api_key: ${OPENROUTER_API_KEY}
    extra_body: {provider: {require_parameters: true}}
  cf_gateway:
    type: llm
    base_url: ${CF_AI_GATEWAY_URL}
    model: ${RAINWISE_MODEL}
    api_key: ${CF_AI_GATEWAY_KEY}
  jev:
    type: jev
    base_url: https://api.typesafe.ai
    model: jev-latest
    api_key: ${JEV_API_KEY}
    max_tag_candidates: 100
    tag_threshold: 0.7
roles:
  classifier: openrouter
  proposer: openrouter
  fallback: null
taxonomies: {}   # section 8
```

No specific default model is hardcoded. Examples name one chosen in Milestone 0 by testing it, because support for structured output varies by provider.

One process runs one provider in the MVP. The code keys everything by provider, so adding more doesn't require core changes.

## 13. Deployment

**The app:** a static Go binary in a small Docker image, running as one instance only. SQLite allows a single writer. Commands: `rainwise serve` and `rainwise config validate`. Logs are structured `slog` JSON to stdout, with job and bookmark IDs. Secrets are never logged.

**`deploy/docker-compose/`:** the image and a volume. No other parts needed.

**`deploy/cloudflare/`:** nothing here leaks into the Go code.

- A small Worker forwards API and webhook traffic to the container, and has a cron trigger for `POST /v1/jobs` (sync).
- The app's built-in scheduler stays off here, because a sleeping container can't fire timers.
- [Litestream](https://litestream.io) replicates SQLite to R2 over its S3 API and [restores at startup](https://docs.openclaw.ai/install/cloudflare), before the app starts. It's in the image's entrypoint for this deployment only.
- **A running job must keep the container awake.** The Worker's cron runs every 5 minutes, and the sleep timeout is longer than this while a job is active. When none is active, the container may sleep and the next cron or webhook wakes it. The exact mechanism is left to the builder and verified in a deployed test.
- Containers may be beta; that's a risk accepted by the user's choice of platform. The docker-compose path is the alternative.

## 14. Project Structure

```
cmd/rainwise/             main, composition root
internal/domain/          models, IDs, errors
internal/core/
  pipeline/               Process, validator, applier
  jobs/                   runner, sync, backfill, reclassify, restore
  events/                 inbox worker
  taxonomy/               fixed, dynamic, proposal, tag normalizing
  snapshot/
internal/ports/           provider, classifier, store interfaces
internal/adapters/
  provider/raindrop/
  provider/providertest/  conformance suite for any provider
  classifier/llm/
  classifier/jev/
  classifier/classifiertest/
  store/sqlite/
  store/memory/
  contentfetch/
internal/httpapi/         routes, auth, errors
internal/scheduler/
internal/config/
examples/config.yaml
deploy/docker-compose/
deploy/cloudflare/
docs/
Dockerfile
```

Adding a provider means a new package under `adapters/provider/`, passing `providertest`, plus one line in the composition root.

**Dependencies:** a YAML parser and an SQLite driver. HTTP clients and routing use the standard library.

**Testing:**

- Table-driven unit tests with fake providers and classifiers: validator, tag rules, move rules, manual-move detection.
- `httptest` servers for Raindrop, LLM, and Jev adapters, using recorded response shapes.
- End-to-end with fakes: backfill interrupted and resumed, restore round trip, webhook loop, duplicate events.
- No real network calls in tests. Live checks are opt-in behind an env var.

## 15. Trade-offs

| Choice | Benefit | Cost |
|---|---|---|
| SQLite required | Reliable pins, jobs, snapshots | State to protect; single instance |
| Polling as the backbone | Works without IFTTT; catches everything | Minutes of delay |
| Events as hints only | Simple; safe against forged data | An extra read per event |
| Own snapshots | Full restore | Stored locally; lost if state is lost, unless exported |
| One OpenAI-compatible LLM adapter | Many gateways through config | Provider-specific features unused |
| Self-reported LLM confidence | Works with any model | Needs threshold tuning. Jev's is calibrated. |
| Flat collections in MVP | Simple, matches the goal | Nested taxonomies wait |
| No web UI | Small scope | Reviewing dry runs means reading JSON |

## 16. MVP Scope

**Live testing (user's decision, on the author's recommendation):** the build runs against fakes, because live access isn't confirmed. The items in section 3.4 and definition-of-done items 1–3 and 5 become a live checklist in `docs/live-checks.md` for the user to run with their own credentials. Where an answer is unknown, the code uses the fallback in section 3.4.

Write tests use a throwaway Raindrop account. The user's real library gets read-only dry runs until restore has been proven on that account.

**Milestone 0, live checklist:** resolve section 3.4 against live services once credentials are available, then update this document.

**In the MVP:**

- Domain, ports, SQLite store, config.
- Raindrop provider, including highlights and collection creation.
- LLM classifier, tested with OpenRouter and Cloudflare AI Gateway.
- Jev classifier, enabled only if a working access route is confirmed. Tested against a fake regardless.
- Fixed and dynamic taxonomies.
- All jobs: sync, backfill, reclassify, snapshot, restore. Dry-run, resume, `from_dry_run`.
- Canonical webhook ingestion, with IFTTT setup documented.
- REST API from section 7.
- Docker image, docker-compose, Cloudflare deployment.
- Example config with the author's nine collections, and a README.

**Post-MVP:** rule-based classifier, other providers, OAuth, nested taxonomies, a review UI, tag merging, metrics.

**Definition of done:**

1. A dry-run backfill on a real library produces a reviewable plan with no changes in Raindrop.
2. An applied backfill sorts the library, and a restore returns collections and tags exactly to the snapshot.
3. A new Unsorted bookmark gets filed by the next sync, or by an IFTTT event if set up.
4. A manual move survives later syncs and backfills.
5. Killing the container mid-backfill loses no progress beyond in-flight items, and the job resumes, including on Cloudflare.
6. Replacing the provider and classifier with fakes requires no core changes, as the tests show.

## 17. Left to the Builder

- Exact prompts, JSON schemas, and internal package boundaries.
- ID formats, SQL schema details, and retry constants not stated above.
- Sampling and batching for taxonomy proposals.
- Whether `AddTags` uses Raindrop's bulk endpoint or read-merge-write, provided no tag can be lost and concurrent edits are handled.
- How the Worker keeps the container awake during jobs, provided the outcome in section 13 holds.
