# Cloudflare Containers deployment

**This deployment is untested.** Nothing here has been deployed, built by Wrangler, or run on Cloudflare. The files follow the Cloudflare Containers documentation as read when they were written (https://developers.cloudflare.com/containers/), and Containers may still be in beta. If you want something that is more likely to work as written, use `deploy/docker-compose/`.

Nothing in this folder is referenced by Go code. Delete the folder and the app is unchanged.

## What is here

| File | Purpose |
|---|---|
| `worker.js` | Forwards all traffic to one container. A 30-minute cron creates a `sync` job. The Container class keeps the container awake while a job is running. |
| `wrangler.jsonc` | Container, Durable Object binding, cron. |
| `Dockerfile` | The app plus a pinned Litestream release (v0.3.13). Build context is the repo root. |
| `entrypoint.sh` | Restores the database from R2 before the app starts, then runs the app under `litestream replicate -exec`. |
| `litestream.yml` | Replicates `/data/rainwise.db` to an R2 bucket over its S3 API. |
| `config.yaml` | Built-in scheduler off, `state.durable_wait: 30s`, `automation.enabled: false`. |
| `package.json` | Pulls in `@cloudflare/containers`. Pinned to 0.3.7, the latest `npm view @cloudflare/containers version` reported when this was written. Check it still matches the docs you follow. |

## How it works

- The built-in scheduler is off because a sleeping container cannot fire timers. The Worker cron does it instead.
- Every 30 minutes (matching `scheduler.sync_every`) the Worker posts `{"type":"sync"}` and does nothing else. `409 automation_disabled` is expected and fine until you set `automation.enabled: true`; it still wakes the container briefly, and it sleeps again after `sleepAfter`. `200` (a sync already queued or running) and `202` are fine too.
- Every request, including the cron's, renews the container's `sleepAfter` timer (`10m`). Because the cron is spaced wider than that, an idle container reaches `onActivityExpired` and sleeps. That hook (the Container class in `worker.js`) calls `GET /v1/jobs?active=true`, which returns the running jobs. If there is one, it renews the timer and the container stays up; if there is none it calls `stop()`. If the app cannot be reached it stays up, since stopping mid-job only costs a resume. Queued jobs are not checked because they are claimed promptly while the container is awake.
- Disk is ephemeral. Litestream restores before the app starts. `state.durable_wait: 30s` pauses a real backfill after its snapshot commits so replication can catch up.
- Run one instance only (`max_instances: 1`, one named container). SQLite allows a single writer.

## Setup

```sh
cd deploy/cloudflare
npm install
# R2: create a bucket and an API token with read/write on it.
for s in RAINWISE_API_TOKEN RAINDROP_TOKEN RAINDROP_WEBHOOK_SECRET RAINWISE_MODEL OPENROUTER_API_KEY \
         LITESTREAM_BUCKET LITESTREAM_ENDPOINT LITESTREAM_ACCESS_KEY_ID LITESTREAM_SECRET_ACCESS_KEY; do
  npx wrangler secret put "$s"
done
npx wrangler deploy
```

`LITESTREAM_ENDPOINT` is the R2 S3 endpoint, `https://<account-id>.r2.cloudflarestorage.com`.

## Verify first, in this order

1. **The wrangler config schema.** Field names such as `image_build_context`, `max_instances`, and whether `migrations` with `new_sqlite_classes` or an `exports` block is current. `npx wrangler deploy --dry-run` should catch some of it.
2. **`image_build_context: "../.."`** really makes the repo root the Docker context, so the `COPY cmd`, `COPY internal` and `COPY deploy/cloudflare/...` lines resolve.
3. **The Container class API** in `worker.js`: that `envVars` set in the constructor are applied on start, that `getContainer(env.RAINWISE).fetch(...)` takes a URL string and options, that `onActivityExpired()` is called when `sleepAfter` passes with no requests (it is called inside the class, so `this.containerFetch` and `this.renewActivityTimeout()` apply), and that `this.stop()` really lets the container sleep.
4. **Secrets reach the container.** `GET /healthz` through the Worker returns 200; `config validate` would name any missing variable in the container logs.
5. **Litestream.** The pinned release and download URL, that `-if-replica-exists` is accepted by `litestream restore`, and that R2 accepts the settings in `litestream.yml` (check `region: auto` and the endpoint). Deploy once, create some state, force the container to restart, and confirm the data is still there.
6. **The keep-awake behavior.** Start a long dry-run backfill and confirm the container is not stopped while it runs, past several `sleepAfter` periods, and does sleep afterwards and between cron ticks. This is definition-of-done item 5; follow `docs/live-checks.md` D5, which tests both halves.
7. **SIGTERM handling.** Confirm Cloudflare gives the container enough time after SIGTERM for the app's 30-second shutdown grace. If it gives less, in-flight items are lost but the job still resumes from its checkpoint.
8. **Cron outcome.** With automation off, the ticks log nothing alarming and return 409.
