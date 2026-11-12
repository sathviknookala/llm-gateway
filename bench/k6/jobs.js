// Asynchronous path: job submissions at a constant arrival rate. Throughput and
// queue wait come from Prometheus once the queue drains (see bench/run_load.py).
//   TARGET, KEYS, RATE, DURATION, TOKENS, DELAY_MS as in sync.js
import http from "k6/http";
import { check } from "k6";

const target = __ENV.TARGET || "http://gateway:8000";
const keys = (__ENV.KEYS || "").split(",").filter((k) => k);
const tokens = parseInt(__ENV.TOKENS || "64");
const delayMs = parseInt(__ENV.DELAY_MS || "0");

export const options = {
  scenarios: {
    jobs: {
      executor: "constant-arrival-rate",
      rate: parseInt(__ENV.RATE || "50"),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  summaryTrendStats: ["avg", "med", "p(95)", "p(99)", "max"],
};

export default function () {
  const body = JSON.stringify({
    model: "mock-small",
    messages: [{ role: "user", content: `job ${__VU}-${__ITER}` }],
    max_tokens: tokens,
    mock: { tokens: tokens, delay_ms: delayMs },
  });
  const res = http.post(`${target}/v1/jobs`, body, {
    headers: { "content-type": "application/json", authorization: `Bearer ${keys[__ITER % keys.length]}` },
  });
  check(res, { "status 202": (r) => r.status === 202 });
}
