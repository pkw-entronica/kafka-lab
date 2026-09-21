# 07 · Slow processing: max.poll.interval.ms exceeded

**What you'll learn:** why a consumer that takes too long between two `poll()` calls gets thrown out of
its group, how that turns into "busy but going nowhere" (the same jobs over and over), and how to size
`max.poll.records` against `max.poll.interval.ms`.

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
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 07`.

---

## Part 1 · Normal: a job queue that keeps up

A web shop puts one job per second (for example "resize an image", ~2 KB each) on the topic `jobs`.
Two workers, `w1` and `w2`, share it. Each job takes a worker **0.5 s**. The workers are configured with:
- `max.poll.interval.ms=10000`: a worker must come back to `poll()` within 10 s, or Kafka considers it
  stuck and removes it from the group;
- `max.poll.records=500`: the Kafka default, so one `poll()` can return up to 500 jobs.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic jobs --partitions 3 --replication-factor 3
```
✅ **Expected:** `Created topic jobs.`

### Step 2 · Start the job traffic (1 job per second)
```bash
nohup bash /apps/numbered-producer.sh jobs job 1 2000 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start the two workers (max.poll.records=500)
```bash
for w in w1 w2; do nohup bash /apps/job-worker.sh $w 500 >/dev/null 2>&1 & done
```
✅ **Expected:** two job lines. **Wait ~20 seconds.**

### Step 4 · Check the group's state
```bash
bash /apps/group-state.sh job-workers 3 5
```
✅ **Expected:** `state Stable` and `members 2` on every line.

### Step 5 · Check the lag and the finished jobs
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group job-workers; tail -3 /tmp/s07-done.log
```
✅ **Expected:** `LAG` is 0 or 1 on each of the 3 partitions. The last lines of the log look like
`10:20:31 w2 job-000042`: jobs finished by both workers.

---

## Part 2 · Break: a big batch arrives at once

The nightly import drops 300 jobs into the queue in one go. Nothing about the workers changes.

### Step 6 · Send the batch
```bash
bash /apps/numbered-producer.sh --burst 300 jobs batch 2000
```
✅ **Expected:** after a few seconds, `sent 300 messages to jobs (batch-...)`. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · What is the group doing?
```bash
bash /apps/group-state.sh job-workers 10 3
```
✅ **Expected:** the state changes between `Stable`, `PreparingRebalance`, `CompletingRebalance` and
sometimes `Empty`. `members` goes up and down between 0 and 2.

### Step 8 · What do the workers' logs say?
```bash
grep -h "poll timeout" /tmp/s07-*.err | tail -2
```
✅ **Expected:** warnings like:
`WARN [Consumer clientId=w1, groupId=job-workers] consumer poll timeout has expired. This means the time
between subsequent calls to poll() was longer than the configured max.poll.interval.ms, …`

### Step 9 · Is the backlog going down?
```bash
bash /apps/lag-watch.sh job-workers 4
```
✅ **Expected:** the total lag stays around 300 or more: `flat` or `GROWING`, never really `draining`.

### Step 10 · …yet the workers are busy
```bash
wc -l < /tmp/s07-done.log; sleep 20; wc -l < /tmp/s07-done.log
```
✅ **Expected:** the second number is ~60–80 higher than the first. The workers finish ~4 jobs/s, but
the lag doesn't drop.

### Step 11 · Jobs done more than once
```bash
awk '{print $3}' /tmp/s07-done.log | sort | uniq -d | wc -l
```
✅ **Expected:** a number above 0 that keeps growing if you run it again. These jobs were done twice or more.

### Step 12 · Do the math
```bash
grep -n "max.poll\|sleep" /apps/job-worker.sh
```
✅ **Expected:** `max.poll.interval.ms=10000`, `max.poll.records="$M"` (500 here) and `sleep 0.5`.

A single `poll()` can return up to 500 jobs, which take 500 × 0.5 s = **250 s** to process. The limit is
**10 s**. So:
1. After 10 s Kafka removes the worker from the group (step 8) and gives its partitions to the other
   worker.
2. The first worker is still busy with its old jobs, so it can't commit them: it's no longer in the
   group. The other worker starts from the **last committed offset** and does the same jobs again
   (step 11).
3. The other worker gets a big batch too and is removed in turn. The committed offsets hardly move, so
   the lag doesn't drop (step 9).

---

## Part 4 · Fix: smaller batches per poll

Each `poll()` must be processed in less than `max.poll.interval.ms`. With `max.poll.records=10`, a batch
takes 10 × 0.5 s = **5 s**, safely under 10 s.

### Step 13 · Stop the workers and clear their logs
```bash
pkill -f job-worker.sh; sleep 3; rm -f /tmp/s07-*
```
✅ **Expected:** `Terminated` for both workers.

### Step 14 · Start the workers with max.poll.records=10
```bash
for w in w1 w2; do nohup bash /apps/job-worker.sh $w 10 >/dev/null 2>&1 & done
```
✅ **Expected:** two job lines. **Wait ~30 seconds.**

---

## Part 5 · Back to normal

### Step 15 · Is the backlog going down now?
```bash
bash /apps/lag-watch.sh job-workers 6
```
✅ **Expected:** `draining by ~3 msg/s` (2 workers × 2 jobs/s, minus the 1 job/s coming in). The backlog
of ~300 is gone in about 2 minutes. Run it again until the lag is close to 0.

### Step 16 · Check the group's state
```bash
bash /apps/group-state.sh job-workers 5 3
```
✅ **Expected:** `state Stable` and `members 2` on every line, as in step 4.

### Step 17 · Send the same batch again
```bash
bash /apps/numbered-producer.sh --burst 300 jobs batch 2000
```
✅ **Expected:** `sent 300 messages to jobs (batch-...)`. **Wait ~2 minutes** for the workers to finish it.

### Step 18 · No timeouts, no repeated jobs
```bash
grep -c "poll timeout" /tmp/s07-*.err; awk '{print $3}' /tmp/s07-done.log | sort | uniq -d | wc -l
```
✅ **Expected:** `0` for each worker's log, and `0` jobs done twice. The group stayed `Stable` through
the whole batch.

---

## Part 6 · Clean up

### Step 19 · Stop the workers and the traffic
```bash
pkill -f "job-worker.sh|numbered-producer.sh jobs"; sleep 5
```
✅ **Expected:** several `Terminated` lines.

### Step 20 · Delete the consumer group, the topic and the logs
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group job-workers
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic jobs
rm -f /tmp/s07-* /tmp/numbered-jobs-*
```
✅ **Expected:** `Deletion of requested consumer groups ('job-workers') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** `max.poll.interval.ms` is Kafka's check that a consumer is still making progress. The
  heartbeat keeps saying "I'm alive", but if `poll()` isn't called again in time, Kafka assumes the
  consumer is stuck and gives its partitions away. Work that was polled but not committed is then done
  again by someone else.
- **The rule:** `max.poll.records × worst-case time per record` must stay well **below**
  `max.poll.interval.ms` (the default is 300000, 5 minutes). Leave room for slow moments: retries, GC,
  a slow database.
- **Fix, most impactful first:**
  - Lower `max.poll.records`, so each poll returns only what you can safely finish.
  - Make processing faster, or move it off the poll loop (a worker pool), pausing the partitions while
    the pool is full.
  - Raise `max.poll.interval.ms` only if processing is truly that slow. It also delays detection of a
    worker that is really stuck.
- **Spot it early:** alert on the `consumer poll timeout has expired` warning, on frequent rebalances,
  and on lag that doesn't drop while the consumers are clearly busy. The same job done twice is often
  the first thing users notice.
- **Normal load hides this bug:** at 1 job per second each poll returned only 1–2 jobs. It only showed
  up when a burst filled a whole batch. Test consumers with a backlog, not just with live traffic.
