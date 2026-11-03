# Rainwise

Rainwise keeps a bookmark library tidy. It sorts every bookmark into exactly one collection from a small list you define, and adds 2 to 5 tags that carry the finer detail. It reuses your existing tags, never removes tags, and treats manual moves as final.

It is one Go service with a REST API, a SQLite state file and no web UI. A bookmark provider (Raindrop today) and a classifier (any OpenAI-compatible LLM, or Jev) plug in through adapters and config. The collections and rules in `examples/config.yaml` are one person's example, not product behavior.

## Status: what has not been tested

Rainwise is built and tested against fakes and local `httptest` servers. **It has not been run against live Raindrop, any LLM provider, Jev, IFTTT, or Cloudflare.** Several facts about those services are unconfirmed, and the code falls back to safe behavior where they are. Work through [docs/live-checks.md](docs/live-checks.md) with a throwaway Raindrop account before pointing it at a library you care about. The Cloudflare deployment is additionally untested as a deployment (see `deploy/cloudflare/README.md`).

## Quick start

You need Go 1.27 or Docker, a Raindrop test token, and an API key for an LLM endpoint whose model supports strict JSON schema output.

```sh
cp examples/config.yaml config.yaml     # edit the taxonomy to suit you
export RAINWISE_API_TOKEN=$(openssl rand -hex 32)   # protects the REST API
export RAINDROP_TOKEN=...               # Raindrop test token
export RAINDROP_WEBHOOK_SECRET=$(openssl rand -hex 32)
export RAINWISE_MODEL=...               # a model id you have checked
export OPENROUTER_API_KEY=...

go run ./cmd/rainwise config validate --config config.yaml   # names any unset variable
go run ./cmd/rainwise serve --config config.yaml
```

Config values may reference `${ENV_VAR}`. Secrets come only from the environment, an unset variable stops startup, and unknown keys are errors. The sample config stores its database at `/data/rainwise.db`; change `state.sqlite_path` for a local run.

With Docker: `docker build -t rainwise:dev .`, then mount your config at `/etc/rainwise/config.yaml` and a volume at `/data`. See `deploy/docker-compose/` for a ready-made setup. Run exactly one instance, since SQLite allows a single writer.

All examples below assume:

```sh
H="Authorization: Bearer $RAINWISE_API_TOKEN"; U=http://localhost:8080
```

## Recommended sequence

Nothing in this sequence changes your library until step 3.

1. **Snapshot export (optional but wise).** Take a snapshot job and download it: `POST /v1/jobs {"type":"snapshot"}`, then `GET /v1/snapshots/{id}/export`. Keep the file off the machine.

2. **Dry run.** Plan the whole library without writing anything:

   ```sh
   curl -s -X POST $U/v1/jobs -H "$H" -H 'Content-Type: application/json' \
     -d '{"type":"backfill","dry_run":true}'
   ```

   It returns a job id. Poll `GET /v1/jobs/{id}` until it finishes.

3. **Review.** Read the plan with `GET /v1/jobs/{id}/decisions` (a collection, tags, confidence and reason per bookmark; `needs_review` in the job progress counts the doubtful ones). Then read `GET /v1/jobs/{id}/proposed-tags`, which lists every **new** tag the plan would create with its count. This is where tag sprawl shows up.

4. **Correct the plan by moving bookmarks by hand.** There are no per-item edit routes. A dry run records where each bookmark currently is. If you move one in Raindrop afterwards, the apply job detects a manual move, drops the planned move and keeps that bookmark where you put it (its tag decision still applies). Tags themselves are corrected in the next step.

5. **Apply.** Apply the reviewed plan as a whole, and say which new tags you approve. `approved_new_tags` maps each proposed tag to its final form; anything left out is rejected:

   ```sh
   curl -s -X POST $U/v1/jobs -H "$H" -H 'Content-Type: application/json' -d '{
     "type":"backfill","from_dry_run":"JOB_ID",
     "params":{"approved_new_tags":{"ai-agents":"agents","postgres":"postgres"}}
   }'
   ```

   The apply job takes a snapshot first and waits until it is durable, creates any missing taxonomy collections (`provision_missing_collections`), and then works from the saved plan without classifying again. **An apply job stays within the plan.** It adds only existing tags plus the approved ones. A bookmark whose title, description, URL or highlights have changed since the dry run is classified again, and a bookmark you moved after the dry run is left alone. Without `from_dry_run`, a real backfill classifies afresh and adds existing tags only.

6. **Inspect.** Look at the library, `GET /v1/jobs/{id}` for counts, and the decisions for what was applied. Old collections are left in place, even when empty. Rainwise never deletes collections or tags, except when a restore removes empty collections it created.

7. **Turn on automation.** See below.

### Automation

`automation.enabled` (default `false`) is the one switch for writes nobody explicitly asked for. It is config, not an API setting.

- **Off:** a real `sync` job returns `409 automation_disabled` (a Cloudflare cron receiving that is normal). Dry-run syncs are allowed. Webhook events are authenticated and saved as held, not processed; held events older than 7 days are dropped, since sync covers them.
- **On:** scheduled syncs and webhook events write without a dry run. Set `automation.enabled: true` and, if you want the built-in timer, `scheduler.enabled: true` with `sync_every`.
- Explicit `backfill`, `reclassify`, `snapshot` and `restore` jobs count as opt-in themselves and work either way.

A first sync with no watermark records bookmarks outside the inbox as known and pinned (source `baseline`) without classifying them, so turning it on does not reshuffle an existing library. Only inbox items (Unsorted) are filed. A bookmark you save straight into a collection is treated as filed on purpose, and gets tags only.

Sync never re-sorts after a taxonomy change; that is what `reclassify` (with a required `scope`) is for. Bookmarks in Needs Review whose content changes are classified again.

### Manual moves are final

Any move you make by hand is detected and pinned, including a move out of Needs Review or into Unsorted. The first backfill may move anything you had not pinned before it started; later backfills and reclassify respect your pins (reclassify can override with `include_pinned`). Low confidence sends a bookmark to Needs Review only if it is not already in an allowed collection.

## Restore and undo

Rainwise keeps its own snapshots (collection and exact tags per bookmark, plus collections it created), because Raindrop has no restore API. Real backfill and reclassify jobs take one automatically. Retention is `state.snapshot_retention` (default 10).

```sh
curl -s $U/v1/snapshots -H "$H"
curl -s -X POST $U/v1/jobs -H "$H" -H 'Content-Type: application/json' \
  -d '{"type":"restore","dry_run":true,"params":{"snapshot_id":"SNAPSHOT_ID"}}'
```

Run the dry run first and read its differences. Then repeat without `dry_run`. A restore sets each bookmark's collection and exact tag list, **overwrites manual changes made since the snapshot** for bookmarks in it, skips bookmarks deleted since and leaves ones created since alone. Empty collections Rainwise created are deleted. A restore takes its own snapshot first, so it can itself be undone by restoring that one.

## Privacy

Bookmark titles, descriptions, URLs, highlights and, depending on `content_fetch`, text fetched from the pages themselves are sent to the LLM endpoint you configure. That means a third party if you use a hosted gateway. If that is a concern, point a classifier at a local OpenAI-compatible server (Ollama and similar work through the same `type: llm` adapter) or set `content_fetch: off`. Rainwise has no telemetry. Tokens and keys are read from the environment and never logged.

Page fetching only follows `http` and `https`, blocks private, loopback and link-local addresses (including after redirects), and caps size (2 MB) and time (10 s).

## Prompt injection

Page content is untrusted, and so is every field of a webhook. Webhook fields are never sent to a classifier; events are hints and the bookmark is re-read from the provider. A hostile page can at worst steer one bookmark into a different allowed collection and suggest tags, which are normalized and capped. Classifiers have no tools and make no writes. Output is validated locally (allowed collection keys, tag limits, confidence range) and invalid output gets one retry and then an error. Domain alone is never treated as enough to classify.

## If state is lost

The SQLite file holds pins, jobs, snapshots and the watermark. If it is lost:

- Your bookmarks are untouched; Rainwise only ever changed collections and added tags.
- The next sync's baseline rule treats everything outside the inbox as known, so there is no mass re-sorting.
- Pins, snapshots and history are gone, so a past backfill can no longer be restored. The mitigation is the JSON export of a snapshot, kept elsewhere (step 1 above). Back up `/data`; the Cloudflare deployment replicates it with Litestream.

On SIGTERM the service lets in-flight items finish, saves the job's checkpoint and exits; running jobs resume on the next start.

## Taxonomy

Modes are `fixed` (default; a list you write) and `dynamic` (a dry run proposes collections for you to approve with `PUT /v1/taxonomies/{id}`, then it behaves like fixed). The config seeds a taxonomy on first start; afterwards the database wins and a difference only logs a warning. Tags are normalized (lowercase, hyphens, `tag_aliases`, simple plurals), existing tags are reused, and a bookmark that already has `max_tags` gets none added. `min_tags` is a target; the validator never invents tags to reach it.

See `examples/config.yaml` for every option. Two provider options are not in the design's YAML sketch: `incremental_since` and `tag_listing`. Both default to `false` and are turned on only after the live checks confirm the Raindrop feature.

## API

Everything except `GET /healthz` and `POST /v1/webhooks/{provider}` needs `Authorization: Bearer <api token>`. Lists take `limit` and `cursor`.

| Route | Purpose |
|---|---|
| `POST /v1/jobs`, `GET /v1/jobs`, `GET /v1/jobs/{id}`, `DELETE /v1/jobs/{id}` | Create (`sync`, `backfill`, `reclassify`, `snapshot`, `restore`), list, get, cancel. `Idempotency-Key` is honored. |
| `GET /v1/jobs/{id}/decisions`, `GET /v1/jobs/{id}/proposed-tags` | Plan or result; new tags with counts |
| `GET /v1/providers`, `GET /v1/classifiers` | Configuration and capabilities |
| `GET /v1/taxonomies[/{id}]`, `PUT /v1/taxonomies/{id}`, `GET /v1/taxonomies/{id}/proposals` | Read, replace, proposed collections |
| `GET /v1/snapshots[/{id}]`, `GET /v1/snapshots/{id}/export` | List, read, download |
| `POST /v1/webhooks/{provider}` | Event ingest, authenticated by the provider's webhook secret. See [docs/ifttt.md](docs/ifttt.md). |

## Deployment

- `deploy/docker-compose/`: the image plus a volume. `stop_grace_period` is 40s so the app's 30s shutdown grace is not cut short.
- `deploy/cloudflare/`: a Worker, cron and Litestream-to-R2 for Cloudflare Containers. Untested; read its README first.

## Adding a provider or classifier

Core code (`internal/core`) imports only the standard library, `internal/domain` and `internal/ports`, and a test enforces it. Vendor logic lives in adapters.

- **Provider:** add a package under `internal/adapters/provider/` implementing `ports.Provider` (read, write, capabilities) and any optional ports it supports (highlights, collection management, tag listing, event normalizing). Run the shared conformance suite in `providertest`. Keep pagination and rate-limit details inside the adapter. Register it where adapters are built from config types, `internal/app/adapters.go`.
- **Classifier:** add a package under `internal/adapters/classifier/` implementing `ports.Classifier` (and `TaxonomyProposer` if it can propose collections). Pass `classifiertest`, register it in `internal/app/adapters.go`, and assign it a role (`classifier`, `proposer`, `fallback`) in config. Most OpenAI-compatible services need no code at all, just a `type: llm` entry with a `base_url`.

Startup fails with a clear message if a role cannot do what the config needs, for example a dynamic taxonomy with no proposer, or more collections than a classifier can choose from. There is no silent downgrade.

## Development

```sh
go vet ./... && go test ./...
```

Tests make no real network calls. Dependencies are `gopkg.in/yaml.v3` and `modernc.org/sqlite`; builds are CGO-free. The design is in `docs/superpowers/specs/2026-11-03-rainwise-design.md`.
