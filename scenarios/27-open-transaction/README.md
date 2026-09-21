# 27 · An open transaction blocks read_committed consumers

**What you'll learn:** what the **last stable offset** is, why a single crashed transactional producer
can stop every `read_committed` consumer on that partition while the data keeps piling up, and how to
inspect and clear transactions with `kafka-transactions.sh`.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> messages will differ.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 27`.

---

## Part 1 · Normal: a transactional producer that commits

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic tx-topic --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic tx-topic.`

### Step 2 · Write 2,000 records in transactions of ~2 seconds
```bash
kafka-producer-perf-test.sh --topic tx-topic --num-records 2000 --record-size 200 --throughput 500 --producer-props bootstrap.servers=$BOOTSTRAP --transactional-id tx-demo --transaction-duration-ms 2000 | tail -1
```
✅ **Expected:** a summary line with 2000 records sent. Every ~2 s the producer committed a transaction.

### Step 3 · A read_committed consumer sees all of it
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --from-beginning --timeout-ms 15000 --consumer-property isolation.level=read_committed 2>/dev/null | wc -l
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --time -1
```
✅ **Expected:** `2000` records, but an end offset slightly **higher** than 2000 (e.g. `tx-topic:0:2005`).
Each commit writes a small transaction marker into the log, and markers take offsets too.

---

## Part 2 · Break: the producer dies in the middle of a transaction

### Step 4 · Start a long transaction, then kill the producer
```bash
nohup kafka-producer-perf-test.sh --topic tx-topic --num-records 1000000 --record-size 200 --throughput 200 --producer-props bootstrap.servers=$BOOTSTRAP transaction.timeout.ms=900000 --transactional-id tx-stuck --transaction-duration-ms 600000 >/dev/null 2>&1 &
sleep 30; pkill -9 -f "transactional-id tx-stuck"
```
✅ **Expected:** a job line, then after 30 seconds `Killed`. About 6,000 records were written inside a
transaction that will never be committed or aborted by its producer — and its timeout is 15 minutes.

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · Two consumers, two different answers
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --from-beginning --timeout-ms 15000 2>/dev/null | wc -l
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --from-beginning --timeout-ms 15000 --consumer-property isolation.level=read_committed 2>/dev/null | wc -l
```
✅ **Expected:** the first (the default, `read_uncommitted`) reads ~8,000 records; the second still only
`2000`. A `read_committed` consumer may not read past the **last stable offset** — the first record of
the oldest open transaction — because it doesn't yet know whether those records will be committed or
aborted.

### Step 6 · The data is there; it's just not readable
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --time -1
```
✅ **Expected:** an end offset around 8,000. For a `read_committed` consumer the partition looks frozen
at ~2,000 while its lag grows with every new record — the classic "consumer lag that never drains and
nothing is wrong with the consumer".

### Step 7 · Which transactions exist?
```bash
kafka-transactions.sh --bootstrap-server $BOOTSTRAP list
```
✅ **Expected:** a table with `tx-demo` in a finished state (`CompleteCommit` or `Empty`) and `tx-stuck`
in state **`Ongoing`**.

### Step 8 · Look at the stuck one
```bash
kafka-transactions.sh --bootstrap-server $BOOTSTRAP describe --transactional-id tx-stuck
```
✅ **Expected:** `TRANSACTION-STATE Ongoing`, `TRANSACTION-TIMEOUT-MS 900000`, a start time 30+ seconds
ago, a growing `TRANSACTION-DURATION-MS`, and `tx-topic-0` under `TOPIC-PARTITIONS`. The coordinator
will abort it by itself — in 15 minutes.

### Step 9 · Is it "hanging"?
```bash
kafka-transactions.sh --bootstrap-server $BOOTSTRAP find-hanging --topic tx-topic
```
✅ **Expected:** most likely `No hanging transactions found`. "Hanging" means something else: a
partition still holds an open transaction that the **coordinator** no longer knows about. Ours is
perfectly known — it's simply still running, from Kafka's point of view.

---

## Part 4 · Fix: start the producer again with the same transactional.id

A transactional application is supposed to use a **stable** `transactional.id`. When it restarts, the
coordinator bumps the producer epoch, **fences** the old producer and aborts whatever it left open.

### Step 10 · Restart the same producer
```bash
kafka-producer-perf-test.sh --topic tx-topic --num-records 200 --record-size 200 --throughput 100 --producer-props bootstrap.servers=$BOOTSTRAP --transactional-id tx-stuck --transaction-duration-ms 2000 | tail -1
```
✅ **Expected:** a normal summary line with 200 records sent. On `initTransactions()` the old
transaction was aborted, and its ~6,000 records are now marked as aborted for good.

---

## Part 5 · Back to normal

### Step 11 · read_committed can move again
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic tx-topic --from-beginning --timeout-ms 15000 --consumer-property isolation.level=read_committed 2>/dev/null | wc -l
```
✅ **Expected:** about `2200` — the original 2,000 plus the 200 from step 10. The aborted records are
skipped: they occupy offsets in the log but no `read_committed` consumer will ever deliver them.

### Step 12 · The transaction is finished
```bash
kafka-transactions.sh --bootstrap-server $BOOTSTRAP describe --transactional-id tx-stuck
```
✅ **Expected:** a finished state (`CompleteCommit` / `Empty`), not `Ongoing`.

---

## Part 6 · Clean up

### Step 13 · Delete the topic
```bash
pkill -f "topic tx-topic"; sleep 2
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic tx-topic
```
✅ **Expected:** no output. The transactional ids expire on their own
(`transactional.id.expiration.ms`, 7 days by default).

---

## Why it happened, and how to prevent it

- **The last stable offset (LSO)** is the boundary a `read_committed` consumer may read up to: the
  first offset of the **oldest open transaction** on that partition. Anything after it might still be
  aborted, so it stays invisible. `read_uncommitted` (the default) ignores all of this and delivers
  everything.
- **One stuck producer blocks one partition for everyone** reading it with `read_committed`, including
  Kafka Streams applications with `processing.guarantee=exactly_once_v2`. The lag graph looks like a
  dead consumer while the consumer is perfectly healthy.
- **`transaction.timeout.ms` is the blast radius** (default 60 s, capped by the broker's
  `transaction.max.timeout.ms`, 15 min). The coordinator aborts an open transaction after it, so a
  crashed producer clears itself eventually. Keep it as short as your longest legitimate transaction —
  minutes, not hours.
- **Fixing it faster:**
  - restart the app with the **same** `transactional.id` (step 10) — the clean, normal path;
  - for a truly hanging transaction, `kafka-transactions.sh find-hanging --broker-id <id>` locates it
    and `kafka-transactions.sh abort --topic <t> --partition <p> --start-offset <o>` forces it closed;
  - never "fix" it by switching consumers to `read_uncommitted` — that reads records that may be
    aborted.
- **Design notes:** one stable `transactional.id` per logical producer instance (for example per
  partition of the input, or per pod ordinal), transactions that cover a small batch, and monitoring on
  `read_committed` lag and on transactions in state `Ongoing` for longer than expected.
