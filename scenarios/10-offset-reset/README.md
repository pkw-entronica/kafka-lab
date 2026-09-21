# 10 · Offset reset surprises

**What you'll learn:** what `auto.offset.reset` really does, how a consumer that was down longer than the
topic's retention silently skips data (while its lag says 0), and how to rewind a group safely with
`--reset-offsets`.

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
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 10`.

---

## Part 1 · Normal: an audit report that sees every event

The topic `audit` (1 partition) receives ~5 audit events per second, numbered `audit-000001`,
`audit-000002`, … The report service (group `audit-report`) writes every event it reads to
`/tmp/s10-report.log`. It uses `auto.offset.reset=latest`, which is the default in Kafka and in the
console consumer.

`auto.offset.reset` is only used when the group has **no valid committed offset**. That happens for a
brand-new group, or when the committed offset no longer exists in the topic:
- `latest`: start at the **end**, and read only what arrives from now on;
- `earliest`: start at the **oldest** record still in the topic.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic audit --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic audit.`

### Step 2 · Start the report service
```bash
nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic audit --group audit-report --consumer-property auto.offset.reset=latest >> /tmp/s10-report.log 2>/dev/null &
```
✅ **Expected:** a job line like `[1] 2345`. **Wait ~10 seconds** before the next step, so the service
is ready before the first event arrives.

### Step 3 · Start the audit events (5 per second)
```bash
nohup bash /apps/numbered-producer.sh audit audit 5 >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~20 seconds.**

### Step 4 · Did the report see every event?
```bash
bash /apps/gap-check.sh /tmp/s10-report.log audit
```
✅ **Expected:** `~100 messages read (~100 different), from audit-000001 to audit-0001xx` and
`0 missing in between`.

### Step 5 · Where does a brand-new group start?
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic audit --group audit-new --max-messages 3
```
✅ **Expected:** 3 events with **high** numbers, like `audit-000131`, then `Processed a total of 3
messages`. A new group with `latest` never sees the older events. The console consumer's
`--from-beginning` switches it to `earliest`.

---

## Part 2 · Break: the report service is down while retention cleans up

The report service goes down for a 1-minute maintenance. In the middle of it, retention deletes the old
audit data. In real life that happens when a consumer is down longer than `retention.ms`, or someone
lowers `retention.ms` to save disk. Here you trigger the deletion yourself instead of waiting.

### Step 6 · Stop the report service
```bash
pkill -f "group audit-report"
```
✅ **Expected:** `Done` or `Terminated` for the service's job. On a clean stop it commits its position.

### Step 7 · 30 s later, retention deletes everything written so far
```bash
sleep 30; echo '{"version":1,"partitions":[{"topic":"audit","partition":0,"offset":-1}]}' > /tmp/s10-delete.json; kafka-delete-records.sh --bootstrap-server $BOOTSTRAP --offset-json-file /tmp/s10-delete.json
```
✅ **Expected:** after 30 s:
```
Executing records delete operation
Records delete operation completed:
partition: audit-0	low_watermark: 312
```
Note your `low_watermark`: the oldest offset still in the topic. The event at offset N is `audit-(N+1)`,
so the oldest event left here is `audit-000313`.

### Step 8 · 30 s later, the service comes back
```bash
sleep 30; nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic audit --group audit-report --consumer-property auto.offset.reset=latest >> /tmp/s10-report.log 2>/dev/null &
```
✅ **Expected:** a job line after 30 s. **Wait ~15 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 9 · What is in the topic now?
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic audit --time -2; kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic audit --time -1
```
✅ **Expected:** `audit:0:312` (`-2` = the oldest offset, the low watermark from step 7) and something
like `audit:0:560` (`-1` = the end). Every event from `audit-000313` onwards is still in Kafka.

### Step 10 · What did the report get?
```bash
bash /apps/gap-check.sh /tmp/s10-report.log audit
```
✅ **Expected:** one gap, like `missing audit-000131 ... audit-000470 (340 messages)`. It ends **far
beyond** `audit-000312`:
- `audit-000131` … `audit-000312` were deleted by retention: gone for good;
- `audit-000313` … `audit-000470` were **still in Kafka**, but the report skipped them.

### Step 11 · What does the lag say?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group audit-report
```
✅ **Expected:** `LAG 0`. **Everything looks healthy**, so lag monitoring can't see this loss.

What happened when the service came back:
1. Its committed offset (~130) no longer existed: the topic now started at 312.
2. With no valid offset, the consumer applied `auto.offset.reset`.
3. `latest` means the **end** of the topic, so it jumped past everything still waiting for it.

---

## Part 4 · Fix: rewind the group, and start from the earliest when the offset is gone

### Step 12 · Stop the report service
```bash
pkill -f "group audit-report"
```
✅ **Expected:** `Done` or `Terminated`. The group must be empty to change its offsets.

### Step 13 · Preview rewinding to the oldest record still in Kafka
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --group audit-report --topic audit --reset-offsets --to-earliest --dry-run
```
✅ **Expected:** `audit-report  audit  0  312`: `NEW-OFFSET` is the low watermark. Nothing has changed yet.

### Step 14 · Rewind the group
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --group audit-report --topic audit --reset-offsets --to-earliest --execute
```
✅ **Expected:** the same table, now applied.

### Step 15 · Restart the service with auto.offset.reset=earliest
```bash
nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic audit --group audit-report --consumer-property auto.offset.reset=earliest >> /tmp/s10-report.log 2>/dev/null &
```
✅ **Expected:** a job line. **Wait ~15 seconds.**

### Step 16 · Check the report again
```bash
bash /apps/gap-check.sh /tmp/s10-report.log audit
```
✅ **Expected:** the gap now ends **exactly** at the low watermark: `missing audit-000131 ...
audit-000312`. Only what retention really deleted is missing. The report also shows more `read` than
`different`: the events it had already read after the jump were read again. That's the price of
rewinding; duplicates are easier to handle than gaps.

---

## Part 5 · Back to normal

The same outage again, this time with `auto.offset.reset=earliest`.

### Step 17 · Stop the report service
```bash
pkill -f "group audit-report"
```
✅ **Expected:** `Done` or `Terminated`.

### Step 18 · 30 s later, retention deletes everything written so far
```bash
sleep 30; kafka-delete-records.sh --bootstrap-server $BOOTSTRAP --offset-json-file /tmp/s10-delete.json
```
✅ **Expected:** `low_watermark:` with a new, higher number, e.g. `720`. Note it.

### Step 19 · 30 s later, the service comes back (with earliest)
```bash
sleep 30; nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic audit --group audit-report --consumer-property auto.offset.reset=earliest >> /tmp/s10-report.log 2>/dev/null &
```
✅ **Expected:** a job line after 30 s. **Wait ~15 seconds.**

### Step 20 · Check the report
```bash
bash /apps/gap-check.sh /tmp/s10-report.log audit
```
✅ **Expected:** the old gap from step 16, plus a new one that ends **exactly** at your new low
watermark (e.g. `... audit-000720`). Everything still in Kafka when the service came back was read.

### Step 21 · Check the lag
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group audit-report
```
✅ **Expected:** `LAG` 0 or close to it, as in step 11. This time it's the truth.

---

## Part 6 · Clean up

### Step 22 · Stop the report service and the events
```bash
pkill -f "group audit-report|numbered-producer.sh audit"; sleep 5
```
✅ **Expected:** two `Terminated` lines.

### Step 23 · Delete the consumer groups, the topic and the files
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group audit-report
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group audit-new
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic audit
rm -f /tmp/s10-* /tmp/numbered-audit-*
```
✅ **Expected:** `Deletion of requested consumer groups ('audit-report') was successful.` and the same
for `audit-new`.

---

## Why it happened, and how to prevent it

- **Why:** the committed offset pointed to data that retention had already deleted. The consumer fell
  back to `auto.offset.reset=latest` and jumped to the end, skipping records that were still there.
  Nothing failed and the lag showed 0, so it went unnoticed.
- **The same setting surprises people in other ways:**
  - A brand-new group with `latest` ignores all existing data (step 5).
  - Changing `group.id` in a deployment makes it a brand-new group.
  - A group that was idle longer than `offsets.retention.minutes` (7 days by default) loses its
    committed offsets and starts over.
- **Rewinding a group (it must be stopped first), always with `--dry-run` before `--execute`:**
  - `--to-earliest` / `--to-latest`
  - `--to-datetime 2026-09-19T10:00:00.000`: replay from a point in time
  - `--by-duration PT30M`: replay the last 30 minutes
  - `--shift-by -100`: step back 100 records
  - `--to-offset N`: an exact offset (scenario 08)
- **Prevent it:**
  - Use `auto.offset.reset=earliest` for any consumer that must not miss data, and make processing
    idempotent (scenario 09) so replays are harmless. Kafka 4.0 clients also accept `by_duration:PT1H`.
  - Keep `retention.ms` comfortably longer than the longest outage a consumer might have. Alert on lag
    **in time**: when a group's oldest unread record gets close to the retention limit, it's about to
    lose data.
  - Compare record counts end to end (produced vs. processed), since lag alone can't reveal skipped data.
