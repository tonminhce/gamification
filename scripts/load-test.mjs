import { execFileSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

const baseUrl = process.env.BASE_URL ?? "http://localhost:8080";
const total = Number(process.env.TOTAL ?? 10_000);
const concurrency = Number(process.env.CONCURRENCY ?? 100);
const companyCount = Number(process.env.COMPANIES ?? 20);
const duplicateCount = Number(process.env.DUPLICATES ?? 1_000);
const leaderboardSamples = Number(process.env.LATENCY_SAMPLES ?? 200);
const runId = process.env.RUN_ID ?? `LOAD-${Date.now()}`;

if (!Number.isInteger(total) || total < 1) throw new Error("TOTAL must be a positive integer");
if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("CONCURRENCY must be a positive integer");
if (!Number.isInteger(companyCount) || companyCount < 1 || companyCount > 100) {
  throw new Error("COMPANIES must be between 1 and 100");
}
if (!Number.isInteger(duplicateCount) || duplicateCount < 0 || duplicateCount > total) {
  throw new Error("DUPLICATES must be between 0 and TOTAL");
}

const events = Array.from({ length: total }, (_, index) => ({
  invoiceId: `${runId}-${index}`,
  companyId: `${runId}-COMPANY-${index % companyCount}`,
}));

async function postInvoice(event) {
  const response = await fetch(`${baseUrl}/api/invoices/simulate`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(event),
    signal: AbortSignal.timeout(10_000),
  });
  if (response.status !== 202) {
    throw new Error(`Expected 202, received ${response.status}: ${await response.text()}`);
  }
}

async function sendConcurrently(items) {
  let nextIndex = 0;
  const workerCount = Math.min(concurrency, items.length);
  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;
      await postInvoice(items[index]);
    }
  }));
}

async function totalPoints() {
  const response = await fetch(`${baseUrl}/api/leaderboard/top?limit=100`);
  if (!response.ok) throw new Error(`Leaderboard returned ${response.status}`);
  const body = await response.json();
  return body.entries.reduce((sum, entry) => sum + entry.points, 0);
}

async function waitForExpectedPoints(expectedPoints, timeoutMs = 60_000) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await totalPoints() === expectedPoints) return;
    await delay(50);
  }
  throw new Error(`Timed out waiting for ${expectedPoints} points; observed ${await totalPoints()}`);
}

function kafkaLag() {
  const output = execFileSync("docker", [
    "compose", "exec", "-T", "kafka",
    "/opt/kafka/bin/kafka-consumer-groups.sh",
    "--bootstrap-server", "localhost:9092",
    "--describe", "--group", "einvoice-gamification",
  ], { encoding: "utf8" });

  return output.trim().split("\n").slice(1).reduce((sum, line) => {
    const columns = line.trim().split(/\s+/);
    const lag = columns[5];
    return sum + (lag === undefined || lag === "-" ? 0 : Number(lag));
  }, 0);
}

async function waitForZeroLag(timeoutMs = 30_000) {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (kafkaLag() === 0) return;
    await delay(250);
  }
  throw new Error("Timed out waiting for Kafka consumer lag to reach zero");
}

function percentile(samples, percentileValue) {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * percentileValue) - 1];
}

const initialPoints = await totalPoints();
if (initialPoints !== 0) {
  throw new Error(`Expected an empty leaderboard; found ${initialPoints} points. Run redis-cli FLUSHDB first.`);
}
await waitForZeroLag();

const expectedPoints = total * 10;
const publishStart = performance.now();
await sendConcurrently(events);
const publishEnd = performance.now();
const pointsAtPublishEnd = await totalPoints();
const remainingEvents = (expectedPoints - pointsAtPublishEnd) / 10;
await waitForExpectedPoints(expectedPoints);
const drainEnd = performance.now();
await waitForZeroLag();

for (let i = 0; i < 20; i++) {
  await fetch(`${baseUrl}/api/leaderboard/top?limit=10`);
}

const latencySamples = [];
for (let i = 0; i < leaderboardSamples; i++) {
  const start = performance.now();
  const response = await fetch(`${baseUrl}/api/leaderboard/top?limit=10`);
  if (!response.ok) throw new Error(`Leaderboard returned ${response.status}`);
  await response.json();
  latencySamples.push(performance.now() - start);
}

await sendConcurrently(events.slice(0, duplicateCount));
await waitForZeroLag();
const pointsAfterDuplicates = await totalPoints();
if (pointsAfterDuplicates !== expectedPoints) {
  throw new Error(`Duplicate processing changed points: expected ${expectedPoints}, observed ${pointsAfterDuplicates}`);
}

const publishSeconds = (publishEnd - publishStart) / 1_000;
const endToEndSeconds = (drainEnd - publishStart) / 1_000;
const backlogDrainSeconds = (drainEnd - publishEnd) / 1_000;

console.log(JSON.stringify({
  runId,
  configuration: { total, concurrency, companyCount, duplicateCount, leaderboardSamples },
  results: {
    apiAcceptedEventsPerSecond: Number((total / publishSeconds).toFixed(2)),
    endToEndEventsPerSecond: Number((total / endToEndSeconds).toFixed(2)),
    backlogDrainEventsPerSecond: remainingEvents > 0
      ? Number((remainingEvents / backlogDrainSeconds).toFixed(2))
      : null,
    leaderboardP50Ms: Number(percentile(latencySamples, 0.50).toFixed(3)),
    leaderboardP95Ms: Number(percentile(latencySamples, 0.95).toFixed(3)),
    leaderboardMaxMs: Number(Math.max(...latencySamples).toFixed(3)),
    expectedPoints,
    pointsAfterDuplicates,
    duplicateProcessingCorrect: pointsAfterDuplicates === expectedPoints,
    kafkaLagAfterDuplicates: kafkaLag(),
  },
}, null, 2));
