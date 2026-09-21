# 22 · Noisy neighbour: one client ruins it for everyone

**What you'll learn:** how one greedy client raises everyone else's latency on a shared cluster, and how
per-client quotas give the latency back without asking anyone to change their code.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; the actual
> latencies depend on your PC.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Don't run other scenarios at the same time: this one measures latency.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 22`.

---

## Part 1 · Normal: an online service on a shared cluster

Two teams share this cluster:
- **api** — the checkout service, writing ~100 small events/s with `acks=all`. It cares about latency.
- **batch-job** — a nightly export, reading a whole topic as fast as it can. It cares about finishing.

### Step 1 · Create both topics
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic api-events --partitions 3 --replication-factor 3 --config retention.bytes=33554432
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic batch-dump --partitions 3 --replication-factor 3 --config retention.bytes=134217728
```
✅ **Expected:** `Created topic api-events.` and `Created topic batch-dump.`

### Step 2 · Fill the export topic with ~200 MB
```bash
kafka-producer-perf-test.sh --topic batch-dump --num-records 1000000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP client.id=batch-job acks=1 | tail -1
```
✅ **Expected:** a summary line with 1,000,000 records sent. This takes 20–60 seconds.

### Step 3 · Measure the checkout service on a quiet cluster
```bash
kafka-producer-perf-test.sh --topic api-events --num-records 2000 --record-size 200 --throughput 100 --producer-props bootstrap.servers=$BOOTSTRAP client.id=api acks=all 2>&1 | tee /tmp/s22-api-quiet.out | tail -1
```
✅ **Expected:** after ~20 s, a summary like
`2000 records sent, 100.0 records/sec (0.02 MB/sec), 4.0 ms avg latency, …, 12 ms 99th, …`
**Write down the 99th percentile** — that's the number this scenario is about.

---

## Part 2 · Break: the export job starts

### Step 4 · Start the nightly export
```bash
nohup bash /apps/batch-job.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`. It reads `batch-dump` from the start, again and again.
**Wait ~20 seconds** so it gets going.

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · Measure the checkout service again — same command, same load
```bash
kafka-producer-perf-test.sh --topic api-events --num-records 2000 --record-size 200 --throughput 100 --producer-props bootstrap.servers=$BOOTSTRAP client.id=api acks=all 2>&1 | tee /tmp/s22-api-noisy.out | tail -1
```
✅ **Expected:** the same 100 records/s (the rate is fixed), but clearly worse latency — often several
times the 99th percentile of step 3. The checkout service didn't change a thing.
*If the numbers barely move, the cluster has spare capacity: start a second export with
`nohup bash /apps/batch-job.sh api-events >/dev/null 2>&1 &` and measure again.*

### Step 6 · How much is the export actually pulling?
```bash
tail -3 /tmp/s22-batch.log
```
✅ **Expected:** `kafka-consumer-perf-test.sh` result lines showing tens or hundreds of MB/s
(`MB.sec` / `nMsg.sec` columns). One client is using the brokers' disks, network threads and CPU as
hard as it can.

### Step 7 · Kafka treats both clients the same
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type clients --entity-default
```
✅ **Expected:** no default quota configured. Without quotas there is no notion of "important" traffic:
requests are handled first come, first served, and the greedy client simply asks more often.

---

## Part 4 · Fix: give the export job a budget

### Step 8 · Apply quotas to batch-job
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name batch-job --add-config 'consumer_byte_rate=10485760,producer_byte_rate=10485760,request_percentage=30'
```
✅ **Expected:** `Completed updating config for client batch-job.` That's 10 MB/s in each direction and
at most 30% of one request-handler thread's time. **Wait ~20 seconds** for it to take effect.

### Step 9 · Measure the checkout service one more time
```bash
kafka-producer-perf-test.sh --topic api-events --num-records 2000 --record-size 200 --throughput 100 --producer-props bootstrap.servers=$BOOTSTRAP client.id=api acks=all 2>&1 | tee /tmp/s22-api-quota.out | tail -1
```
✅ **Expected:** latency back close to the quiet run in step 3.

---

## Part 5 · Back to normal

### Step 10 · The three runs side by side
```bash
bash /apps/perf-table.sh /tmp/s22-api-*.out
```
✅ **Expected:** three rows — `api-noisy` with the worst `avg ms` / `p99 ms`, `api-quiet` and
`api-quota` close to each other.

### Step 11 · The export still runs, just slower
```bash
tail -3 /tmp/s22-batch.log
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type clients --entity-name batch-job
```
✅ **Expected:** the newest result lines are around 10 MB/s instead of the earlier number, and the quota
line shows `consumer_byte_rate=10485760.0`, `producer_byte_rate=10485760.0`, `request_percentage=30.0`.
The export takes longer and nobody else notices it.

---

## Part 6 · Clean up

### Step 12 · Stop the export and remove the quotas
```bash
pkill -f batch-job.sh; sleep 3
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name batch-job --delete-config 'consumer_byte_rate,producer_byte_rate,request_percentage'
```
✅ **Expected:** `Terminated`, then `Completed updating config for client batch-job.`

### Step 13 · Delete the topics, the group and the files
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group batch-job 2>/dev/null
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic api-events
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic batch-dump
rm -f /tmp/s22-* /tmp/batch-job.properties
```
✅ **Expected:** the group delete may say it doesn't exist (the export never commits offsets) — that's
fine — and nothing from the topic deletes.

---

## Why it happened, and how to prevent it

- **Why:** brokers share everything — request handler threads, network threads, page cache, disks. A
  client that asks for data as fast as it can gets served as fast as the broker can, and everyone
  else's requests queue behind it. Latency-sensitive traffic is the first to notice.
- **Quotas are the tool for this**, applied per `client.id`, per authenticated user, or per
  (user, client-id) pair:
  | Quota | What it limits |
  |---|---|
  | `producer_byte_rate` | bytes/s a client may write |
  | `consumer_byte_rate` | bytes/s a client may read |
  | `request_percentage` | share of request-handler + network-thread time (100 = one full thread) |
  - The broker doesn't reject anything: it **delays responses** until the client's average fits the
    quota (scenario 13 shows what that feels like from inside the producer).
  - Quotas are per broker, not cluster-wide: `producer_byte_rate=10485760` on a 3-broker cluster allows
    up to ~30 MB/s in total.
  - Set a sane **default** quota (`--entity-type clients --entity-default`) so new clients are
    limited from day one, and raise it per client where it's justified.
- **What to watch:** produce/fetch latency percentiles per client, `produce-throttle-time` and
  `fetch-throttle-time` on the clients, and request-handler idle ratio on the brokers
  (`RequestHandlerAvgIdlePercent`) — below ~30% means the brokers are saturated.
- **Also helps:** give batch and online workloads different topics (and, if it really matters,
  different clusters), and make batch jobs polite — smaller fetches, off-peak schedules, and
  `fetch.max.bytes` tuned down.
