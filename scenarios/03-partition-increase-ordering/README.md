# 03 · Adding partitions breaks per-key ordering

**What you'll learn:** Kafka keeps events of one key in order only while the key stays in one
partition. Adding partitions moves about half of the keys. If older events are still waiting, newer
ones can be processed first.

**Time:** about 15 minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 03`.

---

## Part 1 · Normal: every account's events in order

20 bank accounts (`acct-1` … `acct-20`) each send their next event every second: `seq=1`, `seq=2`,
`seq=3` … The key is the account id. Two consumers (`s03-a`, `s03-b`) process the events and write each
one to a log, `/tmp/s03-processed.log`. An **order checker** reads that log.

### Step 1 · Start fresh and create the topic
```bash
rm -f /tmp/s03-*
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic accounts --partitions 3 --replication-factor 3
```
✅ **Expected:** `Created topic accounts.`

### Step 2 · Start the event producer
```bash
nohup bash /apps/account-producer.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`. After a few seconds this shows 3 partitions filling up (run it twice to see the numbers grow):
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic accounts
```

### Step 3 · Start the application (the 2 consumers)
```bash
nohup bash /apps/account-consumers.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line. After ~15 s, `tail -3 /tmp/s03-processed.log` shows lines like
```
10:01:02.123 s03-a 1 acct-7 seq=12
```
The fields are: time, consumer, partition, account, event.

### Step 4 · Check the order
```bash
bash /apps/order-check.sh
```
✅ **Expected:** `… events checked, 0 out of order`

---

## Part 2 · Break: partitions added during a deploy

A deploy stops the consumers for ~30 s while events keep coming. Meanwhile an operator increases the
partitions from 3 to 6 "to be ready for more load". Then the deploy finishes.

### Step 5 · The deploy stops the consumers
```bash
pkill -f account-consumers.sh
```
✅ **Expected:** `Terminated`. The log stops growing: `wc -l /tmp/s03-processed.log` prints the same
number twice in a row. **Wait ~20 seconds**, so events pile up.

### Step 6 · Meanwhile: increase the partitions to 6
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --alter --topic accounts --partitions 6
```
✅ **Expected:** no output. `kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic accounts | head -1`
shows `PartitionCount: 6`. **Wait ~10 seconds.**

### Step 7 · The deploy finishes: start the consumers again
```bash
nohup bash /apps/account-consumers.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~30 seconds** while they work through the backlog.

---

## Part 3 · Observe: what does the problem look like?

### Step 8 · Run the order checker again
```bash
bash /apps/order-check.sh
```
✅ **Expected:** about 150 events out of order, spread over 8 of the 20 accounts. Each affected account
was read from **two** partitions, *p* and *p+3*:
```
  acct-2     19 events out of order   (read from partitions 2 5)
  acct-6     20 events out of order   (read from partitions 0 3)
  ...
6200 events checked, 152 out of order
```

### Step 9 · Look at one affected account
```bash
grep " acct-6 " /tmp/s03-processed.log | tail -40 | head -20
```
✅ **Expected:** `s03-b` processes acct-6's newest events from partition 3 **before** `s03-a` reaches its
older events in partition 0. The `seq` numbers go backwards.

### Step 10 · Why did acct-6 end up in two places?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group accounts-app --members --verbose
```
✅ **Expected:**
- `s03-a` owns partitions `0,1,2`, which is where the deploy's backlog waited.
- `s03-b` owns `3,4,5`, the new partitions, which held only the newest events.

With 6 partitions, `murmur2(key) % 6` sends acct-6 to partition 3 instead of 0.

---

## Part 4 · Fix: add partitions the safe way (6 → 12)

What already went wrong can't be undone by Kafka: the affected accounts must be repaired by replaying
their events in `seq` order. The fix is the procedure for the next increase:
**pause producers → drain → add partitions → resume.**

### Step 11 · Pause the producer
```bash
touch /tmp/s03-paused
```
✅ **Expected:** no output. In real life, this is stopping the producing service.

### Step 12 · Drain: wait until nothing is waiting
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group accounts-app
```
✅ **Expected:** `LAG` is `0` on every partition. If not, wait a few seconds and run it again.

### Step 13 · Add partitions
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --alter --topic accounts --partitions 12
```
✅ **Expected:** no output.

### Step 14 · Wait until the consumers own the new partitions
Wait ~10 s, then:
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group accounts-app --members
```
✅ **Expected:** 2 members with `#PARTITIONS 6` each, 12 in total.

### Step 15 · Resume the producer
```bash
rm /tmp/s03-paused
```
✅ **Expected:** no output. **Wait ~30 seconds.**

---

## Part 5 · Back to normal

### Step 16 · Check the order of the latest events
```bash
bash /apps/order-check.sh 600
```
✅ **Expected:** `600 events checked, 0 out of order`. The last ~30 s are all in order, even though many
accounts moved again when going from 6 to 12 partitions.

---

## Part 6 · Clean up

### Step 17 · Stop the producer and the consumers
```bash
pkill -f "topic accounts"; sleep 5
```
✅ **Expected:** `Terminated` lines.

### Step 18 · Delete the group, the topic and the log
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group accounts-app
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic accounts
rm -f /tmp/s03-*
```
✅ **Expected:** `Deletion of requested consumer groups ('accounts-app') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** the partition is `murmur2(key) % partitions`. Going from 3 to 6 moves every key with
  `hash % 6` of 3, 4 or 5, which is about half. Their old events were still unprocessed in the old
  partitions, and their new events landed in new partitions read by another consumer.
- **Prevent it:**
  - pause → drain → expand → resume (Part 4);
  - or migrate to a new topic with more partitions;
  - or make consumers keep the last applied `seq` per key and park older events.
  - Choose the partition count up front, because it can never be decreased.
- **Related trap:** a group with `auto.offset.reset=latest` can **silently skip** records written to new
  partitions before it was assigned them. The lab's consumers read from the earliest offset.
