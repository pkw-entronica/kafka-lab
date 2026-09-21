# 12 · RecordTooLargeException: the three limits

**What you'll learn:** a big message can be rejected by the **broker** or by the **producer itself**, with
the same exception name but a different cause, and which settings have to line up before a large record
can travel all the way from producer to consumer.

**Time:** about 15 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> messages may differ.

## How to follow this guide

- Every command runs in the **lab shell**. Open it once from PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 12`.

---

## Part 1 · Normal: a document topic with a size limit

The topic `docs` carries documents. The team capped a single document at ~100 KB
(`max.message.bytes=100000`) so one upload can't hurt the cluster.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic docs --partitions 1 --replication-factor 3 --config max.message.bytes=100000
```
✅ **Expected:** `Created topic docs.`

### Step 2 · Send a normal document
```bash
echo "a normal document" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic docs
```
✅ **Expected:** no output at all. No news is good news for the console producer.

### Step 3 · Read it back
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic docs --from-beginning --max-messages 1
```
✅ **Expected:** `a normal document` and `Processed a total of 1 messages`.

---

## Part 2 · Break: a 500 KB document

A user uploads a scanned contract. Nothing in the app changes; only the document is bigger.

### Step 4 · Make a 500 KB document
```bash
{ head -c 500000 /dev/zero | tr '\0' x; echo; } > /tmp/s12-500k.txt; ls -l /tmp/s12-500k.txt
```
✅ **Expected:** a file of `500001` bytes (500,000 x's and a newline, which the producer uses as the
record separator).

### Step 5 · Send it
```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic docs < /tmp/s12-500k.txt
```
✅ **Expected:** an error:
```
ERROR Error when sending message to topic docs with key: null, value: 500000 bytes with error:
org.apache.kafka.common.errors.RecordTooLargeException: The request included a message larger than the max message size the server will accept.
```

---

## Part 3 · Observe: which limit said no?

### Step 6 · The topic's limit
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type topics --entity-name docs
```
✅ **Expected:** `max.message.bytes=100000` — the limit this topic was created with. The **broker**
rejected the record; the message never reached the log.

### Step 7 · The cluster-wide default behind it
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep -E "^ *message.max.bytes"
```
✅ **Expected:** `message.max.bytes=1048588 …` — the default for topics that don't set their own limit
(about 1 MB). The topic config wins when both are set.

### Step 8 · Now a 2 MB document: a different "too large"
```bash
{ head -c 2000000 /dev/zero | tr '\0' y; echo; } > /tmp/s12-2m.txt
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic docs < /tmp/s12-2m.txt
```
✅ **Expected:** the same exception name, a different sentence:
```
org.apache.kafka.common.errors.RecordTooLargeException: The message is 2000009 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.
```
This one never left the **client**: the producer's own `max.request.size` (1 MB) stopped it. Raising the
topic limit alone would not have helped.

---

## Part 4 · Fix: raise the limits that are in the way

To carry 2 MB documents, the limit has to be raised **on the topic** and **in the producer**.

### Step 9 · Raise the topic's limit
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name docs --alter --add-config max.message.bytes=3000000
```
✅ **Expected:** `Completed updating config for topic docs.`

### Step 10 · Send the 2 MB document with a bigger producer limit
```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic docs --producer-property max.request.size=3000000 < /tmp/s12-2m.txt
```
✅ **Expected:** no output. It worked.

### Step 11 · Did it really land?
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic docs --time -1
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic docs
```
✅ **Expected:** `docs:0:2` (two records: the small one and the big one), and `Isr: 0,1,2` — the 2 MB
record was replicated to all three brokers without any change to the brokers' `replica.fetch.max.bytes`.

---

## Part 5 · Back to normal

### Step 12 · Can a consumer read it?
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic docs --from-beginning --max-messages 2 --consumer-property max.partition.fetch.bytes=65536 | wc -c
```
✅ **Expected:** about `2000020` bytes — both documents, even though the consumer's per-partition fetch
limit is only 64 KB. Since Kafka 0.10.1 the broker returns an oversized record anyway, so a consumer
can never get stuck on one.

### Step 13 · Small documents still work
```bash
echo "another normal document" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic docs
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic docs --time -1
```
✅ **Expected:** no output from the producer, then `docs:0:3`.

---

## Part 6 · Clean up

### Step 14 · Delete the topic and the test files
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic docs
rm -f /tmp/s12-*
```
✅ **Expected:** no output.

---

## Why it happened, and how to prevent it

- **The limits, in the order a record meets them:**
  | Where | Setting | Default | What it does |
  |---|---|---|---|
  | Producer | `max.request.size` | 1 MB | the client refuses before sending (step 8) |
  | Producer | `buffer.memory` | 32 MB | total buffer; a few large records fill it fast (scenario 13) |
  | Topic | `max.message.bytes` | from `message.max.bytes` | the broker refuses the batch (step 5) |
  | Broker | `message.max.bytes` | ~1 MB | the default for topics that don't override it |
  | Consumer | `max.partition.fetch.bytes` / `fetch.max.bytes` | 1 MB / 50 MB | how much a fetch may buffer — **not** a hard limit on one record any more |
- **Compression matters:** the broker checks the size of the **compressed** batch, so
  `compression.type=lz4` or `zstd` in the producer often solves a "record too large" without changing
  any limit. Text and JSON compress very well.
- **The real advice for big payloads: don't put them in Kafka.** Store the file in object storage (S3,
  MinIO, a database) and send a small message with the URL and a checksum — the "claim check" pattern.
  Large records cost memory on every broker, follower and consumer, make GC pauses worse, and slow
  replication down for everyone sharing the cluster.
- **If you do raise the limits,** raise them everywhere and keep them in sync: topic, producer, and any
  consumer that needs bigger buffers. Mirrors, connectors and stream apps are producers and consumers
  too — they need the same settings.
