# 14 · Batching: throughput against latency

**What you'll learn:** how `linger.ms`, `batch.size`, `compression.type` and `acks` change what a
producer can do, measured on this cluster instead of guessed, and which knob costs you latency.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe the *shape* of the numbers; the actual
> values depend on your PC.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Each measurement takes 10–60 seconds. Don't run other scenarios at the same time, or the numbers
  become meaningless.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 14`.

---

## Part 1 · Normal: measure before you tune

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic bench --partitions 3 --replication-factor 3 --config retention.bytes=67108864
```
✅ **Expected:** `Created topic bench.`

### Step 2 · Make a realistic payload (~150 bytes of JSON, like real order events)
```bash
for i in $(seq 1 200); do printf '{"order_id":%06d,"customer":"acme-corp","status":"CONFIRMED","currency":"EUR","amount":123.45,"warehouse":"eu-west-1","note":"standard shipping"}\n' $i; done > /tmp/s14-payload.txt; wc -lc /tmp/s14-payload.txt
```
✅ **Expected:** `200` lines, about `30000` bytes. The perf tool picks random lines from this file, so
the records look like real data — that matters for compression.

### Step 3 · Run (a): the defaults — send every record as soon as possible
```bash
kafka-producer-perf-test.sh --topic bench --num-records 200000 --payload-file /tmp/s14-payload.txt --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 linger.ms=0 batch.size=16384 2>&1 | tee /tmp/s14-a-linger0.out | tail -1
```
✅ **Expected:** a summary line like
`200000 records sent, 38000.0 records/sec (5.4 MB/sec), 12.0 ms avg latency, 350.0 ms max latency, 8 ms 50th, 30 ms 95th, 90 ms 99th, 200 ms 99.9th.`

---

## Part 2 · Break: the traffic doubles and the producer is the bottleneck

Marketing doubles the campaign volume. The producer above is already sending as fast as it can, with
one small batch after another — that's the ceiling, and it's the thing to fix.

### Step 4 · Run (b): wait 20 ms and send bigger batches
```bash
kafka-producer-perf-test.sh --topic bench --num-records 200000 --payload-file /tmp/s14-payload.txt --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 linger.ms=20 batch.size=131072 2>&1 | tee /tmp/s14-b-linger20.out | tail -1
```
✅ **Expected:** clearly more records/s than (a), and a higher `50th` percentile latency (the producer
now waits up to 20 ms before sending).

### Step 5 · Run (c): the same, with lz4 compression
```bash
kafka-producer-perf-test.sh --topic bench --num-records 200000 --payload-file /tmp/s14-payload.txt --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 linger.ms=20 batch.size=131072 compression.type=lz4 2>&1 | tee /tmp/s14-c-lz4.out | tail -1
```
✅ **Expected:** the best records/s of the three. JSON compresses well, so each batch carries far more
records over the same network and disk.

### Step 6 · Run (d): the same as (b), but with acks=all
```bash
kafka-producer-perf-test.sh --topic bench --num-records 200000 --payload-file /tmp/s14-payload.txt --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=all linger.ms=20 batch.size=131072 2>&1 | tee /tmp/s14-d-acksall.out | tail -1
```
✅ **Expected:** lower throughput and higher latency than (b): the leader now waits for the followers.

---

## Part 3 · Observe: the four runs side by side

### Step 7 · Build the table
```bash
bash /apps/perf-table.sh
```
✅ **Expected:** four rows, roughly like this (your numbers will differ):
```
run                           msg/s     MB/s   avg ms   p95 ms   p99 ms
a-linger0                   38000.0     5.44     12.0     30.0     90.0
b-linger20                  62000.0     8.88     22.0     45.0    110.0
c-lz4                       85000.0    12.17     20.0     40.0    100.0
d-acksall                   45000.0     6.44     35.0     70.0    160.0
```

### Step 8 · How much data actually reached the disks?
```bash
bash /apps/replica-sizes.sh bench
```
✅ **Expected:** three brokers with a similar size. The total is smaller than 4 × 30 MB because run (c)
stored its records compressed — the broker keeps the batch exactly as the producer sent it.

What the numbers say:
- **`linger.ms=0` isn't "fast", it's "eager".** Every batch leaves half empty, so the producer pays the
  per-request cost over and over. Waiting 20 ms fills batches and raises throughput a lot.
- **The cost of waiting is latency**, and it's bounded: at most `linger.ms` extra per record. The p50
  moves, the p99 usually moves less than people fear.
- **Compression is often the biggest win** for text/JSON, and it's paid in producer CPU. Binary or
  already-compressed payloads (images, protobuf blobs) gain nothing.
- **`acks=all` costs a round trip to the followers.** That's the price of not losing data (scenarios
  11 and 17) — and with big enough batches it's often affordable.

---

## Part 4 · Fix: pick settings for the requirement, not for a benchmark

### Step 9 · The settings a durable, high-volume pipeline would use
```bash
kafka-producer-perf-test.sh --topic bench --num-records 200000 --payload-file /tmp/s14-payload.txt --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=all linger.ms=20 batch.size=131072 compression.type=lz4 2>&1 | tee /tmp/s14-e-final.out | tail -1
```
✅ **Expected:** close to (c) in throughput while keeping the `acks=all` guarantee — much better than
the "safe" run (d), and far better than the defaults in (a).

---

## Part 5 · Back to normal

### Step 10 · The final comparison
```bash
bash /apps/perf-table.sh
```
✅ **Expected:** five rows now. `e-final` should be near the top for throughput, with a latency between
(b) and (d).

---

## Part 6 · Clean up

### Step 11 · Delete the topic and the files
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic bench
rm -f /tmp/s14-*
```
✅ **Expected:** no output.

---

## Why it works this way, and how to choose

- **Kafka's throughput comes from batching.** A produce request has a fixed cost (network round trip,
  broker handling, replication). The more records ride in one request, the cheaper each record gets.
  `batch.size` (bytes per partition) and `linger.ms` (how long to wait for a batch to fill) control it.
- **Rules of thumb:**
  - High volume, latency measured in tens of ms: `linger.ms=10..50`, `batch.size=64KB..256KB`,
    `compression.type=lz4` (or `zstd` for the best ratio, at more CPU).
  - Low latency for single events (a request/response path): keep `linger.ms=0` and accept lower
    throughput; batching can't help a producer that sends one record at a time anyway.
  - Durability: `acks=all` with RF 3 and `min.insync.replicas=2`. Pay for it with batching and
    compression, not by weakening `acks`.
- **Measure on your own cluster.** Record size, key distribution, partition count, disk and network all
  change the answer. `kafka-producer-perf-test.sh` with `--payload-file` is the honest way to do it;
  random bytes (`--record-size`) would make compression look useless.
- **Watch the p99, not the average.** The average hides the batches that waited. If the p99 matters to
  your users, tune `linger.ms` down and add partitions/producers instead.
