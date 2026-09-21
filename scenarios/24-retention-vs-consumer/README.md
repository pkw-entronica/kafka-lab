# 24 · Retention deletes what nobody read yet

**What you'll learn:** how `retention.ms` and `segment.ms` decide when data disappears, why a consumer
that is down for longer than the retention loses messages **and still reports lag 0**, and how to size
retention against the worst outage you can survive.

**Time:** about 25 minutes, including one unavoidable 5-minute wait.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Brokers check retention every `log.retention.check.interval.ms` — **5 minutes** by default — so one
  step in this scenario simply waits.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 24`.

---

## Part 1 · Normal: a topic with one minute of retention

The topic `short-lived` keeps messages for 60 seconds (`retention.ms=60000`) and starts a new segment
every 10 seconds (`segment.ms=10000`), because Kafka can only delete **whole closed segments**.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic short-lived --partitions 1 --replication-factor 3 --config retention.ms=60000 --config segment.ms=10000 --config segment.bytes=1048576
```
✅ **Expected:** `Created topic short-lived.`

### Step 2 · Write 10,000 messages
```bash
bash /apps/numbered-producer.sh --burst 10000 short-lived msg
```
✅ **Expected:** `sent 10000 messages to short-lived (msg-...)`.

### Step 3 · Keep a trickle of new messages coming
```bash
nohup bash /apps/numbered-producer.sh short-lived msg 5 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`. New messages keep arriving, which is also what makes
segments roll and old ones become deletable.

### Step 4 · The nightly report reads the first 2,000 messages, then stops
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic short-lived --group nightly-report --from-beginning --max-messages 2000 > /tmp/s24-report.log
tail -1 /tmp/s24-report.log
```
✅ **Expected:** `Processed a total of 2000 messages` on the console and `msg-002000` as the last line
in the file. The group's offset is committed when the consumer closes.

### Step 5 · Where the group stands
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group nightly-report
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -2
```
✅ **Expected:** `CURRENT-OFFSET 2000` with a `LAG` of several thousand, and `short-lived:0:0` — the
oldest message in the log is still offset 0, so everything the report hasn't read is still there.

---

## Part 2 · Break: the report job is down for ten minutes

Nothing dramatic: a failed deploy, a stuck pod, someone's laptop. Meanwhile Kafka keeps its promise —
it deletes anything older than 60 seconds.

### Step 6 · Watch the oldest available message move (this takes ~6 minutes)
```bash
for i in $(seq 1 12); do printf '%s  ' "$(date +%T)"; kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -2; sleep 30; done
```
✅ **Expected:** `short-lived:0:0` for the first few lines, then a jump to a number **above 10000** once
the brokers run their retention check. From then on it keeps creeping up: the log start offset is now
far past the report's committed offset of 2,000.

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · What Kafka still claims about the group
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group nightly-report
```
✅ **Expected:** `CURRENT-OFFSET 2000`, a `LOG-END-OFFSET` above 10,000 and a `LAG` of several
thousand — a lag made of messages that **no longer exist**. Lag alone can't tell the difference.

### Step 8 · What is actually left
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -2
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -1
bash /apps/replica-sizes.sh short-lived
```
✅ **Expected:** an earliest offset in the ten-thousands, an end offset only a few hundred higher, and
a tiny size per broker. Only the last ~60 seconds of messages survive.

### Step 9 · The report comes back
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic short-lived --group nightly-report --timeout-ms 20000 >> /tmp/s24-report.log 2>/dev/null
bash /apps/gap-check.sh /tmp/s24-report.log msg
```
✅ **Expected:** the consumer's stored offset (2,000) is below the log start, so Kafka applies
`auto.offset.reset` — `latest` for the console consumer — and it jumps to the end. `gap-check` shows one
big gap, something like `missing msg-002001 ... msg-010400 (8400 messages)`.

### Step 10 · …and everything looks healthy again
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group nightly-report
```
✅ **Expected:** `LAG 0`. No error was raised, no alert fired, and 8,000+ messages were never processed.

---

## Part 4 · Fix: retention longer than the worst outage, and a safer reset

### Step 11 · Give the topic an hour of retention
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name short-lived --alter --add-config retention.ms=3600000
```
✅ **Expected:** `Completed updating config for topic short-lived.` Retention is now far longer than any
deploy or restart of the report job.

### Step 12 · Restart the report so it never skips available data again
```bash
nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic short-lived --group nightly-report --consumer-property auto.offset.reset=earliest >> /tmp/s24-report.log 2>/dev/null &
```
✅ **Expected:** a job line. If its offset is ever invalid again, it restarts from the oldest message
that still exists instead of skipping to the end (scenario 10).

---

## Part 5 · Back to normal

### Step 13 · Nothing is being deleted any more
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -2; sleep 90; kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic short-lived --time -2
```
✅ **Expected:** the same earliest offset both times, 90 seconds apart — with one-hour retention, the
messages from a minute ago are still there.

### Step 14 · The report keeps up, and misses nothing new
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group nightly-report
bash /apps/gap-check.sh /tmp/s24-report.log msg
```
✅ **Expected:** `LAG` 0 or a handful, and `gap-check` still shows the one old gap from step 9 — those
messages are gone for good — but no new gaps.

---

## Part 6 · Clean up

### Step 15 · Stop everything and delete the topic
```bash
pkill -f "numbered-producer.sh short-lived"; pkill -f "group nightly-report"; sleep 3
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group nightly-report
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic short-lived
rm -f /tmp/s24-* /tmp/numbered-short-lived-*
```
✅ **Expected:** `Terminated` lines, then
`Deletion of requested consumer groups ('nightly-report') was successful.`

---

## Why it happened, and how to prevent it

- **Retention is a promise to the disk, not to the consumer.** Kafka deletes segments once every
  message in them is older than `retention.ms` (or the partition is bigger than `retention.bytes`),
  whether or not anyone read them. The check runs every `log.retention.check.interval.ms` (5 minutes),
  and only **closed** segments can go, which is what `segment.ms` / `segment.bytes` control.
- **Lag can't see deleted data.** `LAG = LOG-END-OFFSET − CURRENT-OFFSET` counts offsets, not existing
  messages, and after the reset it drops to 0 because the group jumped to the end. Both readings look
  fine; the data is gone.
- **Size retention against your worst case:** the longest outage a consumer may have (a bad weekend
  deploy, a holiday, a slow reprocessing job), plus a margin. A day is a common minimum for important
  topics; hours only for genuinely disposable data.
- **What to monitor instead of plain lag:**
  - **lag in time** (how old is the next unread message?) against `retention.ms` — alert when a
    consumer gets within, say, 50% of the retention window;
  - the log start offset moving past a group's committed offset;
  - consumer downtime itself.
- **Also:** `auto.offset.reset=earliest` turns "silently skip" into "reprocess" (make consumers
  idempotent, scenario 09), and for data you must not lose, archive it (tiered storage, a sink
  connector to object storage) instead of stretching retention forever.
