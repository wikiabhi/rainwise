# Sending Raindrop events with IFTTT

Raindrop has no documented native webhooks. IFTTT can watch Raindrop and call a URL, so it can act as a delivery tool. Events are only hints: Rainwise re-reads the bookmark from Raindrop and never passes webhook fields to a classifier. Scheduled `sync` stays the safety net, and IFTTT triggers appear to be polled, so expect delays of minutes.

**Status: unverified.** The Rainwise side is tested against fakes. The IFTTT side has not been tried. Items marked `[unverified]` below need checking with `docs/live-checks.md` check 3.

## Endpoint

```
POST https://YOUR_HOST/v1/webhooks/raindrop
```

`raindrop` is the bookmark provider's name in your config, not the delivery tool. Authenticate with the webhook secret (`providers.raindrop.webhook_secret`), sent either way:

- `Authorization: Bearer <secret>`
- `X-Rainwise-Secret: <secret>`

The API token is not accepted here, and the secret is not accepted elsewhere. Unauthenticated calls are rejected. IFTTT's "Make a web request" action can set custom headers `[unverified]`; if it cannot, use a small relay that adds one.

## Payload

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

- `type` is one of `bookmark.created`, `highlight.created`, `bookmark.moved`. Others get `422`.
- At least one of `resource.id` and `resource.url` is required. A URL is resolved through search.
- `bookmark` is required only for `highlight.created`, and names the parent bookmark.
- `occurred_at` and `event_id` are optional. Without `event_id`, the dedup key is a hash of provider, type, resource and `occurred_at`, so send a stable `occurred_at` or an `event_id` to avoid duplicates counting as new.
- Content-Type is `application/json`. A good delivery returns `202`.

## Applets

Create one applet per trigger, each with a "Make a web request" action (method POST, content type `application/json`).

| IFTTT trigger | `type` | Body |
|---|---|---|
| New Raindrop item | `bookmark.created` | `{"type":"bookmark.created","resource":{"url":"{{Url}}"},"metadata":{"source":"ifttt"}}` |
| New highlight | `highlight.created` | `{"type":"highlight.created","resource":{"id":"{{HighlightId}}"},"bookmark":{"url":"{{Url}}"}}` |
| Item moved to collection | `bookmark.moved` | `{"type":"bookmark.moved","resource":{"url":"{{Url}}"}}` |

## What is unverified

- **Ingredient names** such as `{{Url}}`, `{{HighlightId}}`: the names above are guesses. Look at the ingredients IFTTT actually offers for each trigger.
- **Whether a bookmark ID is available** at all. Without one, the body uses `resource.url` only.
- **Whether the highlight trigger** exposes the highlight ID or only its text and the parent URL. Rainwise needs the parent bookmark; a highlight ID is optional.
- **Whether IFTTT can send custom headers**, and how it retries failed deliveries.
- **Delay.** If polling takes tens of minutes, the scheduled sync will usually get there first, which is fine.
- **Whether moves made by Rainwise itself fire the "moved" trigger.** If they do, Rainwise ignores them (the new collection matches what it last wrote).

While `automation.enabled` is `false`, events are authenticated and saved as `held`, and nothing is processed. Held events older than 7 days are dropped, since sync covers them.
