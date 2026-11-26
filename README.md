# Rainwise

Rainwise keeps a bookmark library tidy. It files each bookmark into one collection from a short list you define and adds 2–5 tags. It reuses your existing tags, never deletes tags, and leaves alone any bookmark you move by hand.

- One Go service: REST API, SQLite state, no UI.
- Provider: [Raindrop.io](https://raindrop.io). Classifier: any OpenAI-compatible LLM (OpenRouter, Ollama, …).
- Plan first, review, then apply. Every write is preceded by a snapshot you can restore.

> **Status:** tested live on a ~100-bookmark Raindrop library (dry run, whole-library apply, tag top-up, small-scale restore). Webhooks, crash recovery and the Cloudflare deployment are not yet verified live. See [docs/live-checks.md](docs/live-checks.md).

## Quick start

Needs Go 1.27+ (or Docker), a Raindrop token, and an LLM endpoint that supports strict JSON-schema output.

```sh
cp examples/config.yaml config.yaml          # edit the collections to suit you
export RAINWISE_API_TOKEN=$(openssl rand -hex 32)
export RAINDROP_TOKEN=...
export RAINDROP_WEBHOOK_SECRET=$(openssl rand -hex 32)
export RAINWISE_MODEL=...                    # e.g. openai/gpt-oss-20b
export OPENROUTER_API_KEY=...

go run ./cmd/rainwise config validate --config config.yaml
go run ./cmd/rainwise serve --config config.yaml
```

Set `state.sqlite_path` to a local path for a non-Docker run. Docker: see `deploy/docker-compose/`. Run one instance only.

## Basic use

```sh
H="Authorization: Bearer $RAINWISE_API_TOKEN"; J="Content-Type: application/json"; U=http://localhost:8080

# 1. Dry run: plan the whole library, change nothing
curl -s -X POST $U/v1/jobs -H "$H" -H "$J" -d '{"type":"backfill","dry_run":true}'

# 2. Review the plan and the new tags it wants
curl -s $U/v1/jobs/JOB_ID/decisions -H "$H"
curl -s $U/v1/jobs/JOB_ID/proposed-tags -H "$H"

# 3. Apply it, approving the new tags you want (others are dropped)
curl -s -X POST $U/v1/jobs -H "$H" -H "$J" -d '{"type":"backfill","from_dry_run":"JOB_ID",
  "params":{"approved_new_tags":{"postgres":"postgres"}}}'

# Undo: restore the snapshot the apply took
curl -s $U/v1/snapshots -H "$H"
curl -s -X POST $U/v1/jobs -H "$H" -H "$J" -d '{"type":"restore","params":{"snapshot_id":"SNAP_ID"}}'
```

Set `automation.enabled: true` to file new Unsorted bookmarks automatically on sync or webhook.

## Privacy

Bookmark titles, URLs, descriptions, highlights and (optionally) page text go to the LLM you configure. Use a local model or `content_fetch: off` if that matters. No telemetry.

## Docs

- [docs/guide.md](docs/guide.md) — full usage guide: workflow, automation, restore, API, safety, extending
- [docs/live-checks.md](docs/live-checks.md) — what has been verified against real services
- [docs/ifttt.md](docs/ifttt.md) — webhook setup
- `examples/config.yaml` — every config option

## Development

```sh
go vet ./... && go test ./...
```

Tests make no network calls. Builds are CGO-free.
