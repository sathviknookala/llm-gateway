// Synchronous path: streamed chat completions at a constant arrival rate.
//   TARGET    base URL: http://gateway:8000 (through the gateway) or
//             http://mock-provider:8100 (baseline: the provider alone)
//   KEYS      comma-separated API keys, used round-robin (ignored by the mock)
//   RATE      requests per second; DURATION e.g. "60s"
//   TOKENS    output tokens per reply; DELAY_MS per-token delay at the mock
// k6 reads a streamed body whole, so `ttfb` is time to response headers
// (gateway admission + upstream headers), not time to first token.
import http from "k6/http";
import { check } from "k6";
import { Trend, Counter } from "k6/metrics";

const target = __ENV.TARGET || "http://gateway:8000";
const keys = (__ENV.KEYS || "").split(",").filter((k) => k);
const tokens = parseInt(__ENV.TOKENS || "64");
const delayMs = parseInt(__ENV.DELAY_MS || "0");

const ttfb = new Trend("ttfb", true);
const statuses = new Counter("status_total");

export const options = {
  discardResponseBodies: false,
  scenarios: {
    sync: {
      executor: "constant-arrival-rate",
      rate: parseInt(__ENV.RATE || "50"),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: parseInt(__ENV.VUS || "200"),
      maxVUs: parseInt(__ENV.MAX_VUS || "1000"),
    },
  },
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export default function () {
  const headers = { "content-type": "application/json" };
  if (keys.length) headers.authorization = `Bearer ${keys[__ITER % keys.length]}`;
  const body = JSON.stringify({
    model: "mock-small",
    messages: [{ role: "user", content: `load ${__VU}-${__ITER}` }],
    stream: true,
    // The gateway sets this upstream anyway; the baseline needs it to get usage too.
    stream_options: { include_usage: true },
    max_tokens: tokens,
    mock: { tokens: tokens, delay_ms: delayMs },
  });
  const res = http.post(`${target}/v1/chat/completions`, body, { headers, timeout: "60s" });
  ttfb.add(res.timings.waiting);
  statuses.add(1, { status: String(res.status) });
  check(res, {
    "status 200": (r) => r.status === 200,
    "stream has usage": (r) => typeof r.body === "string" && r.body.includes('"usage"'),
  });
}
