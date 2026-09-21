# 25 · Log compaction and the delete that vanished

**What you'll learn:** how a compacted topic keeps the latest value per key, how a **tombstone** deletes
a key, and why a consumer that is offline too long can miss the delete and keep a stale record forever.

**Time:** about 25 minutes (compaction runs in the background, so a few steps wait for it).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; compaction timing
> in particular will differ — if a check still shows the old data, produce one more record and try again
> a minute later.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 25`.

---

## Part 1 · Normal: a compacted topic as a key-value store

`user-profile` is a **compacted** topic: instead of deleting old messages by age, Kafka keeps the
**latest value for every key** forever. Services rebuild their caches by reading it from the start.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic user-profile --partitions 1 --replication-factor 3 --config cleanup.policy=compact --config segment.ms=10000 --config segment.bytes=1048576 --config min.cleanable.dirty.ratio=0.01 --config min.compaction.lag.ms=0 --config delete.retention.ms=30000
```
✅ **Expected:** `Created topic user-profile.` The extra settings make compaction run within a minute
instead of hours: roll segments every 10 s, compact as soon as 1% is "dirty", and keep tombstones for
only 30 s.

### Step 2 · Write three versions of five profiles
```bash
for v in 1 2 3; do for k in u1 u2 u3 u4 u5; do echo "$k:{\"user\":\"$k\",\"version\":$v}"; done; sleep 1; done | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --property parse.key=true --property key.separator=:
```
✅ **Expected:** no output. 15 records: 3 versions of 5 keys.

### Step 3 · Read the whole topic
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --from-beginning --timeout-ms 10000 --property print.key=true 2>/dev/null | wc -l
```
✅ **Expected:** `15`. Compaction hasn't run yet — and the newest segment is never compacted, so a
compacted topic always holds some history.

### Step 4 · A service builds its cache from the topic
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --group profile-cache --from-beginning --timeout-ms 10000 --property print.key=true > /tmp/s25-cache.log 2>/dev/null
grep -c u3 /tmp/s25-cache.log
```
✅ **Expected:** `3` — the cache service has seen u3 and stored its latest version. Its group offset is
committed, so next time it will only read what's new.

---

## Part 2 · Break: u3 is deleted while the cache service is down

### Step 5 · Delete the user with a tombstone (a record with a null value)
```bash
echo "u3:NULL" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --property parse.key=true --property key.separator=: --property null.marker=NULL
```
✅ **Expected:** no output. A tombstone is an ordinary record with key `u3` and **no value**; it is how
you delete a key from a compacted topic.

### Step 6 · The tombstone is there for anyone reading now
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --from-beginning --timeout-ms 10000 --property print.key=true 2>/dev/null | tail -3
```
✅ **Expected:** the last line is `u3` followed by `null` — the delete, visible in the log.

### Step 7 · Time passes: segments roll, the cleaner runs, the cache service is still down
```bash
for i in 1 2 3 4 5 6; do echo "u$((i % 5 + 1)):{\"user\":\"u$((i % 5 + 1))\",\"version\":9}" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --property parse.key=true --property key.separator=: ; sleep 15; done
```
✅ **Expected:** no output, and about 90 seconds pass. Each write after 10 s closes the current segment,
which lets the log cleaner compact it — and tombstones older than `delete.retention.ms` (30 s) are
removed for good.

---

## Part 3 · Observe: what does the problem look like?

### Step 8 · What is left in the topic
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --from-beginning --timeout-ms 10000 --property print.key=true 2>/dev/null
```
✅ **Expected:** far fewer than 22 records — roughly the latest value per key plus the newest records.
**And no `u3` line at all**: both u3's values and its tombstone are gone. *(If you still see old
versions, the cleaner hasn't caught up: repeat step 7 once and look again.)*

### Step 9 · PowerShell: look inside a segment
```powershell
kubectl -n kafka-lab exec kafka-controller-0 -- sh -c 'ls /bitnami/kafka/data/user-profile-0/; kafka-dump-log.sh --files $(ls /bitnami/kafka/data/user-profile-0/*.log | head -1) --print-data-log | tail -15'
```
✅ **Expected:** the segment files (`.log`, `.index`, `.timeindex`, and a `.snapshot`/`leader-epoch`
file), then record lines with `offset:`, `keySize:` and `valueSize:`. A tombstone has
`valueSize: -1` — if the cleaner already removed it, you won't find one. Note that offsets have
**gaps**: compaction removes records, it never renumbers them.

### Step 10 · The cache service comes back and resumes
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --group profile-cache --timeout-ms 10000 --property print.key=true >> /tmp/s25-cache.log 2>/dev/null
grep u3 /tmp/s25-cache.log | tail -2
```
✅ **Expected:** the last u3 lines are still the **old values** from step 4 — nothing about a deletion.
The service resumed from its committed offset, and by then the tombstone was already compacted away. Its
cache still holds a user who no longer exists, and nothing will ever correct it.

---

## Part 4 · Fix: keep tombstones longer than any consumer is away

### Step 11 · Raise delete.retention.ms to a day
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name user-profile --alter --add-config delete.retention.ms=86400000
```
✅ **Expected:** `Completed updating config for topic user-profile.` Now a consumer has 24 hours to see
a delete, however long compaction takes.

### Step 12 · Repair the stale cache: rebuild it from the beginning
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group profile-cache
rm -f /tmp/s25-cache.log
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --group profile-cache --from-beginning --timeout-ms 10000 --property print.key=true > /tmp/s25-cache.log 2>/dev/null
grep -c u3 /tmp/s25-cache.log
```
✅ **Expected:** `0` — a full rebuild is the only way to get rid of a missed delete, and it works
because the compacted topic still holds the latest value of every key that exists.

---

## Part 5 · Back to normal

### Step 13 · A new delete stays visible
```bash
echo "u4:NULL" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --property parse.key=true --property key.separator=: --property null.marker=NULL
sleep 60
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --from-beginning --timeout-ms 10000 --property print.key=true 2>/dev/null | grep u4 | tail -2
```
✅ **Expected:** a `u4  null` line even a minute later — with `delete.retention.ms=86400000` the
tombstone stays for a day, so a consumer that was briefly offline still learns about the delete.

### Step 14 · The topic still behaves like a key-value store
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic user-profile --from-beginning --timeout-ms 10000 --property print.key=true 2>/dev/null | awk '{print $1}' | sort -u
```
✅ **Expected:** the surviving keys (`u1`, `u2`, `u5`, and `u4` with its tombstone) — the latest state
of every user, without u3.

---

## Part 6 · Clean up

### Step 15 · Delete the group, the topic and the files
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group profile-cache
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic user-profile
rm -f /tmp/s25-*
```
✅ **Expected:** `Deletion of requested consumer groups ('profile-cache') was successful.`

---

## Why it works this way, and how to use it safely

- **Compaction keeps the last value per key**, so a compacted topic is a changelog you can replay into a
  cache, a database or Kafka Streams state. `cleanup.policy=compact` replaces age-based deletion;
  `compact,delete` does both.
- **What the cleaner will and won't touch:**
  - never the **active** segment, so recent history is always visible — `segment.ms` / `segment.bytes`
    decide how quickly records become compactable;
  - only when the "dirty" part is big enough (`min.cleanable.dirty.ratio`, default 0.5) and old enough
    (`min.compaction.lag.ms`), and at the latest after `max.compaction.lag.ms`;
  - offsets are never reused: after compaction the log has holes, which is normal.
- **Tombstones are the delete**, and `delete.retention.ms` (default **24 hours**) is how long they stay
  after a compaction pass. Any consumer that is offline longer than that can miss the delete
  permanently — exactly what happened in step 10. Keep it comfortably above your worst consumer outage,
  and rebuild caches from the beginning after a long downtime.
- **Operational notes:**
  - a compacted topic's size is driven by the number of **distinct keys**, not by the message rate;
  - keys are mandatory — a record without a key can never be compacted away;
  - `__consumer_offsets` is itself a compacted topic, which is why group offsets survive restarts;
  - watch the log cleaner (`LogCleanerManager`, `max-dirty-percent`, cleaner thread deaths). A dead
    cleaner thread silently stops compaction and the topic grows forever.
