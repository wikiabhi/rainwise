// Forwards traffic to the Rainwise container and, on a 5-minute cron, creates a
// sync job and keeps the container awake while any job is active.
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
  // Longer than the 5-minute cron. Each cron tick's requests renew the timer, so
  // while a job is active the container never idles for this long. With no
  // active job it may sleep, and the next cron or webhook wakes it.
  sleepAfter = "15m";

  constructor(ctx, env) {
    super(ctx, env);
    this.envVars = Object.fromEntries(
      PASSTHROUGH.filter((k) => env[k] !== undefined).map((k) => [k, env[k]]),
    );
  }
}

const isActive = (job) => job.status === "queued" || job.status === "running" || job.status === "cancelling";

export default {
  async fetch(request, env) {
    return getContainer(env.RAINWISE).fetch(request);
  },

  async scheduled(_event, env, ctx) {
    ctx.waitUntil(tick(env));
  },
};

async function tick(env) {
  const container = getContainer(env.RAINWISE);
  const headers = {
    Authorization: `Bearer ${env.RAINWISE_API_TOKEN}`,
    "Content-Type": "application/json",
  };
  const base = "http://container";

  // 200 (already queued or running) and 202 are both fine. 409 automation_disabled
  // is expected until automation.enabled is true. Anything else is logged.
  const created = await container.fetch(`${base}/v1/jobs`, {
    method: "POST",
    headers,
    body: JSON.stringify({ type: "sync" }),
  });
  if (![200, 202, 409].includes(created.status)) {
    console.error("sync job create failed", created.status, await created.text());
  } else {
    await created.body?.cancel();
  }

  // Requesting the job list is itself activity, which renews sleepAfter. Doing it
  // on every tick keeps the container up while a backfill or restore is running,
  // and lets it sleep once none is.
  const list = await container.fetch(`${base}/v1/jobs?limit=20`, { headers });
  if (!list.ok) {
    console.error("job list failed", list.status);
    return;
  }
  const { items = [] } = await list.json();
  if (items.some(isActive)) {
    await container.renewActivityTimeout();
  }
}
