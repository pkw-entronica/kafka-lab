# 02 · More consumers than partitions

**What you'll learn:** traffic grows, the team adds consumers, and nothing improves. The extra
consumers sit idle, because the number of partitions caps how many consumers can work.

**Time:** about 15 minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 02`.

---

## Part 1 · Normal: a payment pipeline that works

About 100 payments/s arrive on the topic `payments`, which has **2 partitions**. Two consumers process
them, each spending ~10 ms per payment (at most ~95/s each).

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic payments --partitions 2 --replication-factor 3
```
✅ **Expected:** `Created topic payments.`

### Step 2 · Start the payment traffic (100 per second)
```bash
nohup kafka-producer-perf-test.sh --topic payments --num-records 100000000 --record-size 200 --throughput 100 --producer-props bootstrap.servers=$BOOTSTRAP acks=all metadata.max.age.ms=5000 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start 2 consumers
```bash
for i in 1 2; do nohup bash /apps/slow-consumer.sh payments payments-group c$i 0.01 >/dev/null 2>&1 & done
```
✅ **Expected:** two job lines.

### Step 4 · Check that the consumers keep up
```bash
bash /apps/in-out.sh payments payments-group
```
✅ **Expected** (after 10 s): out ≈ in, and a small lag.
```
in:  100 msg/s   (new messages written to payments)
out: 100 msg/s   (messages processed by payments-group)
lag: 40 -> 35
```

### Step 5 · Check who reads what
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group payments-group --members
```
✅ **Expected:** 2 members, each with `#PARTITIONS 1`.

---

## Part 2 · Break: peak season

Traffic jumps to ~250 payments/s. Lag starts growing, so the team adds 3 more consumers.

### Step 6 · Traffic rises to 250 per second
```bash
pkill -f ProducerPerformance; nohup kafka-producer-perf-test.sh --topic payments --num-records 100000000 --record-size 200 --throughput 250 --producer-props bootstrap.servers=$BOOTSTRAP acks=all metadata.max.age.ms=5000 >/dev/null 2>&1 &
```
✅ **Expected:** a new job line.

### Step 7 · The team scales from 2 to 5 consumers
```bash
for i in 3 4 5; do nohup bash /apps/slow-consumer.sh payments payments-group c$i 0.01 >/dev/null 2>&1 & done
```
✅ **Expected:** three job lines. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 8 · Did the new consumers get work?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group payments-group --members
```
✅ **Expected:** 5 members. Two have `#PARTITIONS 1`, and **three have `#PARTITIONS 0`**: they are
connected and healthy, but idle.

### Step 9 · Is the lag growing?
```bash
bash /apps/lag-watch.sh payments-group 4
```
✅ **Expected:** `GROWING by ~60 msg/s` on every line.

### Step 10 · How much comes in, and how much goes out?
```bash
bash /apps/in-out.sh payments payments-group
```
✅ **Expected:** `in: ~250 msg/s` but `out: ~190 msg/s`. The 2 working consumers are at their limit.

### Step 11 · How many partitions?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic payments | head -1
```
✅ **Expected:** `PartitionCount: 2`. At most 2 consumers can ever work, however many you start.

---

## Part 4 · Fix: add partitions

With 10 partitions, all 5 consumers get 2 each. Capacity becomes 5 × ~95 = ~475/s.

### Step 12 · Increase to 10 partitions
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --alter --topic payments --partitions 10
```
✅ **Expected:** no output.

### Step 13 · Check that every consumer now has work
Wait ~15 s, because clients notice new partitions at their next metadata refresh. Then:
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group payments-group --members
```
✅ **Expected:** 5 members, each with `#PARTITIONS 2`. Nobody is idle.

---

## Part 5 · Back to normal

### Step 14 · Watch the backlog drain
```bash
bash /apps/lag-watch.sh payments-group 6
```
✅ **Expected:** `draining` on every line, until the total lag is small and `flat`. Run it again if needed.

### Step 15 · Check that the consumers keep up
```bash
bash /apps/in-out.sh payments payments-group
```
✅ **Expected:** out ≈ in (~250 msg/s), and a small lag, as in step 4.

---

## Part 6 · Clean up

### Step 16 · Stop the traffic and the consumers
```bash
pkill -f "topic payments"; sleep 5
```
✅ **Expected:** several `Terminated` lines.

### Step 17 · Delete the consumer group and the topic
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group payments-group
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic payments
```
✅ **Expected:** `Deletion of requested consumer groups ('payments-group') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** each partition is read by exactly one member of a consumer group, so
  **useful consumers = min(consumers, partitions)**. With 2 partitions, consumer 3, 4 and 5 can only wait.
- **Things to know about adding partitions:**
  - Old records don't move. The backlog stays where it was and drains over time.
  - Clients notice new partitions only after a metadata refresh (`metadata.max.age.ms`, **5 minutes** by
    default; the lab uses 5 s).
  - Keys change partition, which can break per-key ordering (scenario 03).
  - Partitions can never be decreased.
- **Prevent it:** create topics with `partitions ≥ peak msg/s ÷ msg/s one consumer can handle`, plus
  headroom. Also make consumers faster before scaling them out.
