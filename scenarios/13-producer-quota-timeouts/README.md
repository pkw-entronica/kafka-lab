# 13 · A throttled producer: blocked sends and timeouts

**What you'll learn:** how a broker-side **quota** slows one client down, what that looks like from
inside the producer (a full buffer, blocked `send()` calls, expired batches), and which producer
settings decide whether it fails fast or fails silently.

**Time:** about 15 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> messages may differ.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a lot between runs.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 13`.

---

## Part 1 · Normal: a batch job writes as fast as it can

The nightly export job (`client.id=batch-job`) writes to the topic `firehose`.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic firehose --partitions 3 --replication-factor 3 --config retention.bytes=33554432
```
✅ **Expected:** `Created topic firehose.` (The size limit keeps the 1 GiB lab disks safe.)

### Step 2 · Run the export job
```bash
kafka-producer-perf-test.sh --topic firehose --num-records 60000 --record-size 1000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP client.id=batch-job acks=1 | tail -2
```
✅ **Expected:** one summary line, something like
`60000 records sent, 25000.0 records/sec (23.84 MB/sec), 30.5 ms avg latency, …`
The job takes a few seconds and uses as much bandwidth as it can get.

---

## Part 2 · Break: an admin puts a quota on that client

The export job was crowding out the online services, so the platform team limited it to **100 KB/s**.

### Step 3 · Apply the quota
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name batch-job --add-config 'producer_byte_rate=102400'
```
✅ **Expected:** `Completed updating config for client batch-job.`

### Step 4 · Run the same job again, with a small buffer and short timeouts
```bash
kafka-producer-perf-test.sh --topic firehose --num-records 60000 --record-size 1000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP client.id=batch-job acks=1 buffer.memory=1048576 max.block.ms=5000 delivery.timeout.ms=10000 2>&1 | tee /tmp/s13-throttled.out | tail -25
```
✅ **Expected:** it crawls, then fails. Progress lines around `100 records/sec (0.10 MB/sec)`, error
traces such as
`org.apache.kafka.common.errors.TimeoutException: Expiring 24 record(s) for firehose-0:10000 ms has passed since batch creation`,
and finally the tool stops with
`org.apache.kafka.clients.producer.BufferExhaustedException: Failed to allocate memory within the configured max blocking time 5000 ms.`

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · The throughput matches the quota
Look at the `records/sec` in the output of step 4.

✅ **Expected:** about **100 records/s** = 100 KB/s = exactly the quota. The broker doesn't reject
anything: it **delays its answers** until the client's average rate fits the quota.

### Step 6 · Confirm the quota
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type clients --entity-name batch-job
```
✅ **Expected:** a line naming `batch-job` with `producer_byte_rate=102400.0`.

### Step 7 · Is the cluster slow, or just this client?
```bash
kafka-producer-perf-test.sh --topic firehose --num-records 20000 --record-size 1000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP client.id=api acks=1 | tail -1
```
✅ **Expected:** thousands of records/s again. The quota is attached to the **client.id**, not to the
topic or the cluster — a different client is untouched.

### Step 8 · Why the producer died instead of just being slow
The chain inside the producer:
1. The broker answers slowly, so batches stay in the producer's memory.
2. `buffer.memory=1048576` (1 MB) fills up.
3. `send()` blocks waiting for free memory, up to `max.block.ms=5000` — then it throws
   `BufferExhaustedException`.
4. Batches that wait longer than `delivery.timeout.ms=10000` are expired with a `TimeoutException`,
   which is reported to the send callback.

```bash
grep -c "Expiring" /tmp/s13-throttled.out; grep -c "BufferExhausted" /tmp/s13-throttled.out
```
✅ **Expected:** a number above 0 for expired batches, and at least `1` for the buffer exhaustion. With
Kafka's defaults (32 MB buffer, 60 s block, 2 min delivery timeout) the same job would have looked
*frozen* for minutes instead of failing.

---

## Part 4 · Fix: remove the quota (or give it room)

### Step 9 · Remove the quota
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name batch-job --delete-config producer_byte_rate
```
✅ **Expected:** `Completed updating config for client batch-job.`

### Step 10 · Check that it's gone
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type clients --entity-name batch-job
```
✅ **Expected:** no `producer_byte_rate` line any more (often no output at all).

---

## Part 5 · Back to normal

### Step 11 · Run the export job again
```bash
kafka-producer-perf-test.sh --topic firehose --num-records 60000 --record-size 1000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP client.id=batch-job acks=1 buffer.memory=1048576 max.block.ms=5000 delivery.timeout.ms=10000 | tail -2
```
✅ **Expected:** thousands of records/s, no timeouts, no `BufferExhaustedException` — the same small
buffer is fine when the broker answers quickly.

---

## Part 6 · Clean up

### Step 12 · Delete the topic and make sure no quota is left
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic firehose
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type clients --entity-name batch-job --delete-config producer_byte_rate 2>/dev/null; rm -f /tmp/s13-*; echo done
```
✅ **Expected:** `done` (the second command may print an error if the quota is already gone — that's fine).

---

## Why it happened, and how to prevent it

- **Quotas throttle, they don't reject.** The broker computes the client's rate and delays its
  *responses* by however long it takes to bring the average back under the limit. The client sees
  latency, not an error — until its own buffer runs out.
- **The producer settings that decide what happens then:**
  - `buffer.memory` (32 MB): how much unsent data may pile up.
  - `max.block.ms` (60 s): how long `send()` may block on a full buffer before throwing
    `BufferExhaustedException`. Setting it low turns a freeze into a clear error.
  - `delivery.timeout.ms` (2 min): the total budget per record, including retries. Expired batches come
    back as `TimeoutException` in the callback — count them, don't ignore them.
  - `linger.ms` / `batch.size`: bigger batches use the throttled bandwidth better (scenario 14).
- **Metrics that tell you it's a quota, not a broken broker:**
  - `produce-throttle-time-avg` / `-max` on the producer: above 0 means the broker is throttling you.
  - `record-error-rate` and `record-retry-rate`: records failing or being retried.
  - `buffer-available-bytes`: heading to 0 means `send()` is about to block.
- **Use quotas on purpose:** give each `client.id` (or authenticated user) a `producer_byte_rate`,
  `consumer_byte_rate` and `request_percentage` so one batch job can't starve the online services —
  that's scenario 22. And tell application teams: a throttled producer must handle slow sends and
  retriable errors instead of buffering forever.
