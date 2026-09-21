# 09 · Duplicates after a crash

**What you'll learn:** why a consumer that crashes processes some records **again** after it restarts
(at-least-once delivery), how the commit interval decides how many, and why the real fix is to make
processing idempotent.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 09`.

---

## Part 1 · Normal: a ledger that applies each entry once

The topic `ledger` holds payment entries `entry-000001`, `entry-000002`, … The app `ledger-app` applies
each entry to the ledger file `/tmp/s09-applied.log`, about 150 entries/s. Its consumer commits offsets
automatically every 5 s (`auto.commit.interval.ms=5000`, the Kafka default).

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic ledger --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic ledger.`

### Step 2 · Write 5,000 entries
```bash
bash /apps/numbered-producer.sh --burst 5000 ledger entry 1000
```
✅ **Expected:** after a few seconds, `sent 5000 messages to ledger (entry-...)`.

### Step 3 · Start the app
```bash
nohup bash /apps/ledger-consumer.sh 5000 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 4 · Watch the ledger fill up
```bash
wc -l < /tmp/s09-applied.log
```
✅ **Expected:** a growing number. Run it again every ~10 s until it reaches **5000** (about 40 s).

### Step 5 · Stop the app cleanly
```bash
pkill -f "group ledger-app"
```
✅ **Expected:** after a few seconds, `Done` for the app's job. On a clean stop the consumer commits its
position, and the app finishes the entries it already has.

### Step 6 · Compare Kafka's bookmark with the ledger
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group ledger-app; bash /apps/gap-check.sh /tmp/s09-applied.log entry
```
✅ **Expected:**
- `CURRENT-OFFSET 5000`, `LAG 0`, and a note that the group has no active members;
- `5000 messages read (5000 different), from entry-000001 to entry-005000` and `0 missing in between`.

Kafka's bookmark and the ledger agree: every entry was applied exactly once.

---

## Part 2 · Break: release 1.1 crashes

To save load on Kafka, release 1.1 commits only every **30 s**. Shortly after it's deployed, the pod is
killed hard (`OOMKilled`): no clean shutdown, no final commit.

### Step 7 · 5,000 new entries arrive
```bash
bash /apps/numbered-producer.sh --burst 5000 ledger entry 1000
```
✅ **Expected:** `sent 5000 messages to ledger (entry-...)`. These are `entry-005001` … `entry-010000`.

### Step 8 · Start release 1.1, and 15 s later kill it hard
```bash
nohup bash /apps/ledger-consumer.sh 30000 >/dev/null 2>&1 & sleep 15; pkill -9 -f ledger-consumer.sh; pkill -9 -f "group ledger-app"
```
✅ **Expected:** a job line, then after 15 s `Killed` for the app.

---

## Part 3 · Observe: what does the problem look like?

### Step 9 · How far did the app get?
```bash
wc -l < /tmp/s09-applied.log
```
✅ **Expected:** about **7000**: the first 5,000, plus ~2,000 new entries applied before the crash.

### Step 10 · What does Kafka think?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group ledger-app
```
✅ **Expected:** `CURRENT-OFFSET` still **5000** and `LAG 5000`. The app applied ~2,000 entries but
crashed before its first 30 s commit, so Kafka doesn't know about them.

### Step 11 · The pod restarts
```bash
nohup bash /apps/ledger-consumer.sh 30000 >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~40 seconds** until `wc -l < /tmp/s09-applied.log` stops growing.

### Step 12 · Check the ledger
```bash
bash /apps/gap-check.sh /tmp/s09-applied.log entry; sort /tmp/s09-applied.log | uniq -d | head -3
```
✅ **Expected:**
- about `12000 messages read (10000 different)`, and `0 missing in between`;
- the first duplicates: `entry-005001`, `entry-005002`, `entry-005003`.

Nothing was lost, but **~2,000 payments were applied twice**. After the restart the app started again
from the last committed offset (5000), and re-did everything it had applied after it.

---

## Part 4 · Fix: make applying an entry idempotent

A shorter commit interval makes the window smaller, but a crash can always land between "applied" and
"committed". The real fix is **idempotent processing**: the app remembers which entry IDs it has already
applied (like a unique key in a database) and skips those that come again. That is release 1.2
(`--idempotent`).

### Step 13 · Stop the app cleanly
```bash
pkill -f "group ledger-app"
```
✅ **Expected:** `Done` for the app's job after a few seconds.

### Step 14 · Repair the ledger: remove the duplicates
```bash
sort -u /tmp/s09-applied.log -o /tmp/s09-applied.log; bash /apps/gap-check.sh /tmp/s09-applied.log entry
```
✅ **Expected:** `10000 messages read (10000 different)` and `0 missing in between`.

### Step 15 · 5,000 new entries arrive
```bash
bash /apps/numbered-producer.sh --burst 5000 ledger entry 1000
```
✅ **Expected:** `sent 5000 messages to ledger (entry-...)`. These are `entry-010001` … `entry-015000`.

### Step 16 · Start release 1.2, and 15 s later kill it hard, like in step 8
```bash
nohup bash /apps/ledger-consumer.sh 30000 --idempotent >/dev/null 2>&1 & sleep 15; pkill -9 -f ledger-consumer.sh; pkill -9 -f "group ledger-app"
```
✅ **Expected:** a job line, then `Killed`. The same crash as before, with the same 30 s commit interval.

---

## Part 5 · Back to normal

### Step 17 · The pod restarts (release 1.2)
```bash
nohup bash /apps/ledger-consumer.sh 30000 --idempotent >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~40 seconds.**

### Step 18 · Check the ledger
```bash
bash /apps/gap-check.sh /tmp/s09-applied.log entry; wc -l < /tmp/s09-skipped.log
```
✅ **Expected:**
- `15000 messages read (15000 different)` and `0 missing in between`: no duplicates this time;
- about **2000** skipped. Kafka delivered those entries again, as in step 12, but the app recognised and
  skipped them.

### Step 19 · Stop cleanly and compare Kafka's bookmark with the ledger
```bash
pkill -f "group ledger-app"; sleep 5; kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group ledger-app
```
✅ **Expected:** `CURRENT-OFFSET 15000` and `LAG 0`. It matches the 15,000 entries in the ledger, as in step 6.

---

## Part 6 · Clean up

### Step 20 · Make sure the app is stopped
```bash
pkill -f "ledger-consumer.sh|group ledger-app"; sleep 3
```
✅ **Expected:** no output (it's already stopped), or `Terminated`.

### Step 21 · Delete the consumer group, the topic and the files
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group ledger-app
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic ledger
rm -f /tmp/s09-* /tmp/numbered-ledger-*
```
✅ **Expected:** `Deletion of requested consumer groups ('ledger-app') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** a consumer's progress is its **committed offset**, and the commit happens *after*
  processing. Anything processed after the last commit is processed again after a crash. Kafka's
  default delivery is **at-least-once**: no loss, but duplicates are possible.
- **The commit interval sets the window, not the rule:** with `auto.commit.interval.ms=30000` up to
  30 s of work is repeated. With 5 s, up to 5 s. With a commit after every record it's still possible:
  a crash can land between processing and committing.
- **Beware of committing too early:** auto-commit commits what was *handed to* the app, not what it
  *finished*. If the app buffers records and crashes, records can be **lost** instead of duplicated
  (at-most-once). Commit manually (`commitSync()`) after the work is really done.
- **Prevent it:**
  - **Make processing idempotent:** a unique key or upsert in the database, or a "processed IDs" table
    updated in the same transaction as the work. Duplicates then do no harm.
  - For Kafka-to-Kafka pipelines, use **transactions / exactly-once** (`isolation.level=read_committed`,
    a transactional producer, or Kafka Streams with `processing.guarantee=exactly_once_v2`).
  - Stop apps gracefully (SIGTERM, then `close()`). The consumer commits on the way out, as in step 5.
- **Spot it early:** count duplicates downstream (the same ID applied twice), and after every crash or
  `OOMKilled`, check how much was re-processed.
