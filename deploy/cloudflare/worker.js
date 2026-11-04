// Forwards traffic to the Rainwise container. A 30-minute cron creates a sync job
// (matching scheduler.sync_every); when the container has been idle for sleepAfter,
// it asks the app whether a job is running and either stays up or sleeps.
// UNTESTED against Cloudflare; see README.md in this folder.
import { Container, getContainer } from "@cloudflare/containers";

const PASSTHROUGH = [
  "RAINWISE_API_TOKEN",
  "RAINDROP_TOKEN",
  "RAINDROP_WEBHOOK_SECRET",
  "RAINWISE_MODEL",
  "OPENROUTER_API_KEY",
  "LITESTREAM_BUCKET",
  "LITESTREAM_ENDPOINT",
  "LITESTREAM_ACCESS_KEY_ID",
  "LITESTREAM_SECRET_ACCESS_KEY",
];

export class RainwiseContainer extends Container {
  defaultPort = 8080;
  // Every request renews this timer, including the cron's. With the cron every
  // 30 minutes, an idle container outlives sleepAfter and reaches
  // onActivityExpired, which decides between staying up and sleeping.
  sleepAfter = "10m";

  constructor(ctx, env) {
    super(ctx, env);
    this.apiToken = env.RAINWISE_API_TOKEN;
    this.envVars = Object.fromEntries(
      PASSTHROUGH.filter((k) => env[k] !== undefined).map((k) => [k, env[k]]),
    );
  }

  // Runs when sleepAfter passes with no requests. A job in progress keeps the
  // container up for another period; otherwise it stops. If the app cannot be
  // asked, stay up: stopping mid-job costs a resume, staying up costs minutes.
  async onActivityExpired() {
    let busy = true;
    try {
      const res = await this.containerFetch("http://container/v1/jobs?active=true", {
        headers: { Authorization: `Bearer ${this.apiToken}` },
      });
      if (res.ok) {
        const { items = [] } = await res.json();
        busy = items.length > 0;
      }
    } catch (err) {
      console.error("active job check failed", err);
    }
    if (busy) {
      this.renewActivityTimeout();
    } else {
      await this.stop();
    }
  }
}

export default {
  async fetch(request, env) {
    return getContainer(env.RAINWISE).fetch(request);
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(createSync(env));
  },
};

// 200 (a sync is already queued or running) and 202 are fine. 409 automation_disabled
// is expected until automation.enabled is true; the request still wakes the
// container briefly, and it sleeps again after sleepAfter.
async function createSync(env) {
  const res = await getContainer(env.RAINWISE).fetch("http://container/v1/jobs", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RAINWISE_API_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ type: "sync" }),
  });
  if (![200, 202, 409].includes(res.status)) {
    console.error("sync job create failed", res.status, await res.text());
  } else {
    await res.body?.cancel();
  }
}
