# Live checks

**Most of these checks have not been run.** Boxes ticked below were run on 2026-11-04 with read-only requests to a real Raindrop library of 102 bookmarks, with a test prompt to OpenRouter, or as a Rainwise job in Docker. **Restore has been tested on 2 bookmarks only.** Run the rest before you trust the tool with a real library, and tick the boxes as you go.

Rules for running it:

- Use a **throwaway Raindrop account** for everything that writes (checks 2, D1 to D3, and the restore test). Point your real library at Rainwise only for read-only dry runs until restore has been proven on the throwaway one.
- Keep tokens in environment variables. The commands below use `$RAINDROP_TOKEN`, `$RAINWISE_URL` and `$RAINWISE_API_TOKEN`.
- Each section says what each outcome means for your config. Where an answer is unknown, the code already falls back to the safe behavior, so a failed check costs time or money, not correctness.

Raindrop API base: `https://api.raindrop.io/rest/v1`.

## Part 1: provider and gateway unknowns (design section 3.4)

### [x] 1. Does `lastUpdate:` work through the API `search` parameter?

**Result (2026-11-04): yes, with `>`.** `lastUpdate:>2026-06-06` returned 55 of 102 bookmarks, matching a local count of those updated on or after that date, so it includes the date itself. `lastUpdate:2026-06-06` and the JSON form matched only that one day (7). The adapter uses the `>` form, so `incremental_since: true` is safe. The commands below use the exact-day forms; add `>` to reproduce this.

```sh
curl -s -H "Authorization: Bearer $RAINDROP_TOKEN" \
  "https://api.raindrop.io/rest/v1/raindrops/0?perpage=50&search=%5B%7B%22key%22%3A%22lastUpdate%22%2C%22val%22%3A%222026-01-01%22%7D%5D" | head -c 600
# A plainer variant to try if that returns everything:
curl -s -H "Authorization: Bearer $RAINDROP_TOKEN" \
  "https://api.raindrop.io/rest/v1/raindrops/0?perpage=50&search=lastUpdate:2026-01-01" | head -c 600
```

Compare `count` and the `lastUpdate` fields against an unfiltered call, using a date after most of your bookmarks were edited.

- Fewer results, all newer than the date: it works. Set `providers.raindrop.incremental_since: true`. Sync then lists only what changed since the last run, minus 48 hours.
- Same results as unfiltered, or an error: leave it `false`. Every sync lists the whole library (about 100 requests per 5,000 bookmarks, if check F holds).

Note the filter has one-day granularity, which is why sync backs off 48 hours.

### [x] 2. Do API writes bump `lastUpdate`?

**Result (2026-11-04): yes.** The apply and the restore changed `lastUpdate` on the 2 bookmarks they wrote, and it stayed changed after the restore.

On the throwaway account: note a bookmark's `lastUpdate`, add a tag through `PUT /raindrop/{id}`, read it again.

- It changed: expected side effect. Your own writes will show up in the next incremental sync. The input hash makes that harmless, so no config change.
- It didn't change: also harmless.

### [ ] 3. Which fields do IFTTT triggers expose, and is the bookmark ID included?

Follow `docs/ifttt.md`, create the applet, trigger it once, and read what the receiving end got (a request inspector works). Record the real field names in `docs/ifttt.md`.

- ID present: map it to `resource.id`.
- Only a URL: map it to `resource.url`. Rainwise resolves URLs through search, which costs one extra request per event.
- Neither: IFTTT cannot drive Rainwise. Rely on scheduled sync.

### [x] 4. Is there a usable tag-list endpoint?

**Result (2026-11-04): yes.** Tags and counts matched a count over every listed bookmark. Set `tag_listing: true`.

```sh
curl -s -H "Authorization: Bearer $RAINDROP_TOKEN" https://api.raindrop.io/rest/v1/tags | head -c 600
```

Expect `{"items":[{"_id":"tag","count":N}, ...]}`. Rainwise's adapter calls `GET /tags`, so this check also answers whether `GET /tags` works.

- Works, counts look right: set `providers.raindrop.tag_listing: true`. Backfills, syncs and webhook items then take the tag vocabulary from this endpoint, for one request per sync.
- Fails or counts are missing: leave it `false`. The vocabulary is built from listed bookmarks.

### [ ] 5. Can Jev be reached through OpenRouter or a Cloudflare route?

Only matters if you want Jev. Send one tiny request through each route you have access to (OpenRouter model page for the Jev model, or your Cloudflare AI Gateway), and check it answers in Jev's question format.

- Works: add a `jev` classifier in your config and use it for a role.
- Doesn't, or no access: keep the `jev` entry out of your config. The adapter stays unused.

### [x] 6. Does your chosen route enforce JSON schema?

**Result (2026-11-04): yes** for `openai/gpt-oss-20b` on OpenRouter with `require_parameters`: it returned `{"confidence":0.5}`. One test prompt only, so it doesn't show how well the model classifies.

```sh
curl -s "$LLM_BASE_URL/chat/completions" -H "Authorization: Bearer $LLM_KEY" -H 'Content-Type: application/json' -d '{
  "model": "'"$RAINWISE_MODEL"'",
  "messages": [{"role":"user","content":"Reply with confidence 0.5"}],
  "response_format": {"type":"json_schema","json_schema":{"name":"t","strict":true,
    "schema":{"type":"object","properties":{"confidence":{"type":"number"}},"required":["confidence"],"additionalProperties":false}}}
}'
```

- Output matches the schema: good.
- Free text, or the parameter is ignored: Rainwise still validates every answer locally and retries once, so it stays correct, but expect more retries and errors. On OpenRouter, confirm `extra_body: {provider: {require_parameters: true}}` is set so unsupported endpoints are skipped.

### [ ] 7. Can Litestream confirm replication on demand?

Only for the Cloudflare deployment. Check whether the pinned Litestream version has a command or endpoint that reports the replica is caught up.

- Yes: change `deploy/cloudflare/entrypoint.sh` to use it.
- No: keep `state.durable_wait: 30s` and confirm in R2 that the snapshot's data appears within that window.

### [x] 8. How do rate-limit headers look with a test token?

**Result (2026-11-04):** `x-ratelimit-limit: 120`, as documented. The default of 100 is fine.

```sh
curl -si -H "Authorization: Bearer $RAINDROP_TOKEN" https://api.raindrop.io/rest/v1/user | grep -i 'ratelimit\|retry'
```

Raindrop documents 120 requests per minute. The client limits itself to `providers.raindrop.requests_per_minute` (default 100). If headers show a lower limit, lower that value.

## Part 2: additional checks

### [x] A. Does the chosen model accept `minimum` and `maximum` in strict JSON schema?

**Result (2026-11-04): accepted** by `openai/gpt-oss-20b` on OpenRouter.

The classifier's decision schema (`internal/adapters/classifier/llm/schema.go`) puts `"minimum": 0, "maximum": 1` on `confidence`. Some strict-mode implementations reject those keywords. Use the check 6 command with `"confidence":{"type":"number","minimum":0,"maximum":1}`.

- Accepted: nothing to do.
- Rejected with a 400: that model cannot be used as is. Either pick another model or remove the two keywords in `schema.go` (the validator already clamps and checks confidence locally).

### [x] B. Does `GET /tags` work?

**Result (2026-11-04): yes**, see check 4.

Same as check 4. Listed separately so you record the outcome for the adapter.

### [ ] C. Do nested Raindrop collections behave in listing?

**Partial result (2026-11-04):** on a library with no nested collections, `GET /collections/childrens` returned the same 9 collections as `GET /collections`, with no parent set. The adapter removes the duplicates, and the dry run was unaffected. Real nesting is still untested.

The adapter reads root collections (`GET /collections`) and merges in child collections (`GET /collections/childrens`). Taxonomy collections are flat and matched by case-insensitive name at the root level. In the throwaway account, create a child collection with a bookmark inside it, then run a dry-run backfill.

- The bookmark appears in the plan with its real (child) collection, and a root collection with the same name as a taxonomy entry is the one chosen for moves: good.
- The bookmark is missing, its collection is wrong, or a child collection is mistaken for a taxonomy collection: nested collections are not handled correctly. Keep the library flat until this is fixed.

### [ ] E. PUT with a bad collection id: what status and body come back?

On the throwaway account, with a real bookmark id and a collection id that doesn't exist:

```sh
curl -si -X PUT -H "Authorization: Bearer $RAINDROP_TOKEN" -H 'Content-Type: application/json' \
  -d '{"collection":{"$id":999999999}}' https://api.raindrop.io/rest/v1/raindrop/BOOKMARK_ID
```

Rainwise treats a 2xx with `"result":false` as a failed write, and as not found if `error` or `errorMessage` says "not found". A body with no `result` field counts as success.

- A 4xx, or `"result":false`: fine, the write is reported as failed.
- A 200 with `"result":true` and the bookmark left alone or sent elsewhere: the write looks successful when it isn't. Keep taxonomy collections from being deleted while Rainwise runs, and report it as a bug to fix in the adapter.

### [x] F. Do list responses include highlights the way a single read does?

**Result (2026-11-04): yes, on a small sample.** All 102 listed bookmarks had the field and a `created` date, and each highlight had `created` too. Only one bookmark had a highlight, and its list and single-read counts matched (1), with the feed also showing one. So an idle sync should need list requests only. Compare again if you have more highlighted bookmarks.

```sh
curl -s -H "Authorization: Bearer $RAINDROP_TOKEN" "https://api.raindrop.io/rest/v1/raindrops/0?perpage=50" \
  | jq '[.items[] | {id: ._id, highlights: (if has("highlights") then (.highlights | length) else "absent" end)}]'
curl -s -H "Authorization: Bearer $RAINDROP_TOKEN" https://api.raindrop.io/rest/v1/raindrop/BOOKMARK_ID | jq .item.highlights
```

Use a bookmark with highlights, one without, and compare the first output with the second.

- Every item has the field, and the highlights match: an idle sync makes only list requests, and new highlights are found in them without paging `/highlights`. Nothing to set.
- The field is absent on some or all items: still correct, because absent is read as "unknown". Each such bookmark costs one more GET per sync, and sync pages `/highlights`. If every item lacks it, 5,000 bookmarks take over 50 minutes at the default 100 requests per minute, which won't fit `sync_every: 30m`. Use a longer interval, or set `incremental_since` if check 1 passes.
- The field is present but incomplete, for example empty on a bookmark with highlights: new highlights would be missed, and their text left out of classification. Report it as a bug to fix in the adapter.

## Part 3: definition-of-done checks (design section 16)

Run these in order against the throwaway account, with a classifier configured. Start Rainwise with `automation.enabled: false` unless the step says otherwise.

### [x] D1. Dry-run backfill produces a reviewable plan and changes nothing

**Result (2026-11-04): pass** on the real 102-bookmark library with `openai/gpt-oss-20b` and `content_fetch: off`. All 102 were processed with no failures in about 35 seconds. Bookmarks, tags, `lastUpdate` values and collection counts were identical afterwards. Expect a long tail of one-use tags: 109 of 142 proposed tags were used once, so review `/proposed-tags` before approving.

Add 20 or so varied bookmarks to the throwaway account. Then:

```sh
curl -s -X POST "$RAINWISE_URL/v1/jobs" -H "Authorization: Bearer $RAINWISE_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{"type":"backfill","dry_run":true}'
curl -s -H "Authorization: Bearer $RAINWISE_API_TOKEN" "$RAINWISE_URL/v1/jobs/JOB_ID/decisions"
```

Pass: the job finishes, `/decisions` lists a collection and tags per bookmark, and Raindrop shows no changes (collections and tags as before).

### [ ] D2. Applied backfill sorts the library; restore returns it exactly

**Partial result (2026-11-04): pass at small scale.** On a real library, a backfill scoped to a 2-bookmark collection moved and tagged only those 2 and created the 9 taxonomy collections, leaving the other 100 unchanged. The restore returned all 102 to their original collection, tags (compared sorted), title and link, deleted the 9 collections and removed the 4 test tags. `lastUpdate` stays changed on the 2 written bookmarks. Manual sort order wasn't compared. A whole-library apply then ran on the 102 bookmarks with no failures: every bookmark landed in its planned collection, user tags were kept, and only approved tags or tags already in the library were added. A whole-library restore is still untested. A short approval list leaves many bookmarks below `min_tags`: approving 45 of 150 proposed tags left 66 of 102 with fewer than 2 tags, and 24 with none.

Apply the reviewed plan with `from_dry_run` and `approved_new_tags` (see the README). Then run a restore dry run and a real restore with the snapshot id from the backfill job.

Pass: after the backfill the bookmarks are in the planned collections with added tags. After the restore, every bookmark's collection and tag list matches the snapshot exactly, and collections Rainwise created are gone if empty.

### [ ] D3. A new Unsorted bookmark is filed by the next sync

Set `automation.enabled: true`, add a bookmark to Unsorted, then:

```sh
curl -s -X POST "$RAINWISE_URL/v1/jobs" -H "Authorization: Bearer $RAINWISE_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{"type":"sync"}'
```

Pass: the bookmark moves out of Unsorted with tags. With IFTTT set up, also pass if an event does it before the sync.

### [ ] D5. Killing the container mid-backfill loses no progress beyond in-flight items

Start a real backfill on a library of a few hundred bookmarks, then `docker kill` (or stop the Cloudflare container) part way through. Restart.

Pass: the job returns to `running` on its own, resumes from its checkpoint, and finishes with no bookmark processed twice in a harmful way. Litestream restores the database after a cold start.

Repeat on Cloudflare, and test both halves of the keep-awake design (`sleepAfter` is about 10 minutes, and the cron runs every 30):

- **A running job outlives `sleepAfter`.** Start a backfill that runs longer than `sleepAfter` and watch the container. At each expiry `onActivityExpired` asks `GET /v1/jobs?active=true`; the container must stay up until the job finishes, with no restart.
- **An idle container sleeps.** After the job finishes (or with none running), the container must stop within about one `sleepAfter` and not wake until a cron tick or webhook arrives. Check it stays asleep between ticks, since a container that never sleeps costs money.

Item 4 of the definition of done (a manual move survives later syncs and backfills) is covered by tests against fakes. Confirm it live by moving one bookmark by hand after a backfill and running another sync.
