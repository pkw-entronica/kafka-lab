# 28 · Retries without idempotence: duplicates and reordering

**What you'll learn:** what a producer retry really does to your log when a leader changes mid-flight —
the same record twice, or records landing out of order — and how the idempotent producer (the default
since Kafka 3.0) makes both impossible.

**Time:** about 25 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do. Duplicates and
> reordering depend on timing: if a run shows none, repeat the disruption — that in itself is a useful
> lesson about how rare and how invisible these bugs are.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 28`.

---

## Part 1 · Normal: an old-style producer on a calm cluster

This producer is configured the way many applications were before Kafka 3.0: **no idempotence**,
unlimited retries, and up to 5 requests in flight at the same time.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic ordering --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic ordering.` One partition, so "in order" has a clear meaning.

### Step 2 · Write the producer config
```bash
printf 'enable.idempotence=false\nacks=all\nretries=2147483647\nmax.in.flight.requests.per.connection=5\ndelivery.timeout.ms=120000\n' > /tmp/s28-nonidem.properties; cat /tmp/s28-nonidem.properties
```
✅ **Expected:** the five settings printed back. `acks=all` and endless retries look safe — that's the
point of this scenario.

### Step 3 · Send 10,000 numbered records, no disruption
```bash
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic ordering --max-messages 10000 --throughput 500 --producer.config /tmp/s28-nonidem.properties > /tmp/s28-calm.json
grep -c producer_send_success /tmp/s28-calm.json
```
✅ **Expected:** `10000` after ~20 s.

### Step 4 · Read the partition back in order and check it
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic ordering --partition 0 --offset earliest --timeout-ms 20000 2>/dev/null > /tmp/s28-calm.txt
bash /apps/seq-check.sh /tmp/s28-calm.txt
```
✅ **Expected:**
```
10000 records read, 10000 different
0 duplicates
0 out of order
```
On a healthy cluster nothing is retried, so nothing goes wrong.

---

## Part 2 · Break: the leader changes while the producer is retrying

### Step 5 · Start a longer run in the background
```bash
nohup kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic ordering2 --max-messages 30000 --throughput 300 --producer.config /tmp/s28-nonidem.properties > /tmp/s28-chaos.json 2>/dev/null &
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic ordering2
```
✅ **Expected:** a job line, then the description of the auto-created topic `ordering2`. **Note the
`Leader:` number** — call it **L**. The run takes ~100 seconds.

### Step 6 · PowerShell: kill the leader while it writes
```powershell
kubectl -n kafka-lab delete pod kafka-controller-L
```
✅ **Expected:** `pod "kafka-controller-L" deleted`. The producer loses its connection, gets
`NotLeaderOrFollower`, and retries the batches that were in flight — possibly out of their original
order, and possibly ones the old leader had already written.

### Step 7 · Do it once more, on the new leader
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic ordering2
```
✅ **Expected:** a different `Leader:` now. Delete that pod too in PowerShell (same command with the new
number), then wait for the producer to finish (`grep -c producer_send_success /tmp/s28-chaos.json`
stops growing).

---

## Part 3 · Observe: what does the problem look like?

### Step 8 · What the producer thinks happened
```bash
grep -c producer_send_success /tmp/s28-chaos.json; grep -c producer_send_error /tmp/s28-chaos.json
```
✅ **Expected:** ~`30000` successes and `0` errors. From the application's point of view the run was
perfect: every record was acknowledged, nothing failed.

### Step 9 · What is actually in the log
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic ordering2 --partition 0 --offset earliest --timeout-ms 30000 2>/dev/null > /tmp/s28-chaos.txt
bash /apps/seq-check.sh /tmp/s28-chaos.txt
```
✅ **Expected:** more records read than different ones, and a non-zero count for **duplicates** and/or
**out of order**, with examples such as `1234 appears again` or `4001 came after 4050`.
*If both are 0, the retries happened to be clean: repeat steps 5–7 (use topic `ordering3`), or delete
the leader pod twice in quick succession.*

### Step 10 · Why retries can duplicate and reorder
- **Duplicate:** the broker wrote the batch, then the leader died before its response arrived. The
  producer never got an ack, so it sent the batch again — and the new leader appended it a second time.
- **Out of order:** with `max.in.flight.requests.per.connection=5`, batch 2 can still be in flight when
  batch 1 fails. Batch 2 succeeds, batch 1 is retried afterwards, and the partition now holds them
  swapped.
- Neither is visible to the application: it counted 30,000 successes.

---

## Part 4 · Fix: the idempotent producer

### Step 11 · The same config, with idempotence on
```bash
printf 'enable.idempotence=true\nacks=all\nretries=2147483647\nmax.in.flight.requests.per.connection=5\ndelivery.timeout.ms=120000\n' > /tmp/s28-idem.properties; cat /tmp/s28-idem.properties
```
✅ **Expected:** the same five lines with `enable.idempotence=true`. Each producer gets a producer id,
and every batch carries a sequence number per partition, so the broker can tell a retry from a new
batch — and refuses to append a batch that arrives out of sequence.

### Step 12 · Run it again, with the same disruption
```bash
nohup kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic ordering-idem --max-messages 30000 --throughput 300 --producer.config /tmp/s28-idem.properties > /tmp/s28-idem.json 2>/dev/null &
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic ordering-idem
```
✅ **Expected:** a job line and the topic description with its `Leader:`. In PowerShell, delete that
broker's pod while the run is going, exactly as in steps 6 and 7.

---

## Part 5 · Back to normal

### Step 13 · Check the log again
```bash
grep -c producer_send_success /tmp/s28-idem.json
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic ordering-idem --partition 0 --offset earliest --timeout-ms 30000 2>/dev/null > /tmp/s28-idem.txt
bash /apps/seq-check.sh /tmp/s28-idem.txt
```
✅ **Expected:** ~`30000` acknowledged, and
```
30000 records read, 30000 different
0 duplicates
0 out of order
```
Same disruption, same retries, clean log.

### Step 14 · The cluster is healthy again
```bash
bash /apps/isr-watch.sh "" 3 5
```
✅ **Expected:** `under-replicated 0   offline 0`.

---

## Part 6 · Clean up

### Step 15 · Delete the topics and files
```bash
for t in ordering ordering2 ordering3 ordering-idem; do kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic $t 2>/dev/null; done
rm -f /tmp/s28-*; echo done
```
✅ **Expected:** `done` (errors for topics you never created are fine).

---

## Why it happened, and how to prevent it

- **A retry is a second write.** The producer can't tell "the write failed" from "the write succeeded
  but the answer got lost", so it retries — and without idempotence the broker has no way to recognise
  the repeat. `acks=all` doesn't help: it's about durability, not about duplicates.
- **In-flight requests and order:** with more than one request in flight, a failed-and-retried batch
  lands after batches that were sent later. That's how a partition — the one place Kafka promises
  order — ends up out of order.
- **What idempotence does:** the producer gets a producer id (PID) and numbers its batches per
  partition. The broker keeps the last 5 sequence numbers per producer and partition, drops exact
  duplicates, and rejects out-of-sequence batches with `OutOfOrderSequenceException`. It costs
  nothing measurable and is **on by default since Kafka 3.0**.
- **The settings that must line up** (the client refuses invalid combinations):
  `enable.idempotence=true` requires `acks=all`, `retries > 0` and
  `max.in.flight.requests.per.connection <= 5`. Setting `acks=1` or `retries=0` in an old config file
  silently turns idempotence **off** — check your producer configs for exactly that.
- **What it does not cover:** duplicates created by the *application* (sending the same event twice
  after its own crash) and duplicates across a producer restart with a new producer id. For those you
  need transactions (scenario 27) or an idempotent consumer (scenario 09).
