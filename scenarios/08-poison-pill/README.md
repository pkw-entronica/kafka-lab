# 08 · Poison pill: one bad record stops a consumer

**What you'll learn:** how a single record that the app can't handle makes it crash on the same offset
forever, how to find that record, how to move the group past it, and why apps need a dead letter topic.

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
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 08`.

---

## Part 1 · Normal: temperature readings flow in

Five sensors send ~5 readings per second to the topic `sensors`, as JSON like `{"sensor":"s3","temp":24}`.
The app `sensor-app` (group `sensor-app`) reads each temperature and stores it. Like a Kubernetes pod,
it is restarted automatically whenever it crashes.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic sensors --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic sensors.`

### Step 2 · Start the sensors
```bash
nohup bash /apps/sensor-producer.sh 5 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start the app
```bash
nohup bash /apps/sensor-app.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~15 seconds.**

### Step 4 · Is it storing readings?
```bash
tail -3 /tmp/s08-readings.log
```
✅ **Expected:** lines like `10:30:12 offset 57 temp 24`, with the current time and growing offsets.

### Step 5 · Check that the lag stays small
```bash
bash /apps/lag-watch.sh sensor-app 3 5
```
✅ **Expected:** a total lag of 0 to ~25, not growing.

---

## Part 2 · Break: a sensor sends something unexpected

A firmware update on sensor `s4` makes it send `"N/A"` instead of a number when its thermometer fails.

### Step 6 · Sensor s4 sends a bad reading
```bash
echo '{"sensor":"s4","temp":"N/A"}' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic sensors
```
✅ **Expected:** no output. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · What is the app doing?
```bash
tail -6 /tmp/s08-app.log
```
✅ **Expected:** the same pair of lines, again and again, **always with the same offset**:
```
10:31:05 CRASH: cannot parse the record at offset 1234: {"sensor":"s4","temp":"N/A"}
10:31:05 app restarting in 5 s
```

### Step 8 · Is the lag growing?
```bash
bash /apps/lag-watch.sh sensor-app 3
```
✅ **Expected:** `GROWING by ~5 msg/s`. Nothing gets processed any more, while the sensors keep sending.

### Step 9 · Where does the app stop?
```bash
tail -2 /tmp/s08-readings.log
```
✅ **Expected:** the last reading has the offset just **before** the bad one, e.g. `offset 1233` if the
crash was at 1234. The app never gets past the bad record.

### Step 10 · Remember the bad offset and look at the record
```bash
BAD=$(grep -o 'offset [0-9]*' /tmp/s08-app.log | tail -1 | cut -d' ' -f2); echo "bad offset: $BAD"
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic sensors --partition 0 --offset $BAD --max-messages 1
```
✅ **Expected:** `bad offset: 1234` (your number), then the record `{"sensor":"s4","temp":"N/A"}` and
`Processed a total of 1 messages`. The next steps use `$BAD`. If you open a new lab shell, run the
first line again.

### Step 11 · What does Kafka know about the group?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group sensor-app
```
✅ **Expected:** `CURRENT-OFFSET` is at or a little below `$BAD`, and `LAG` keeps growing. You may also
see `has no active members` or `is rebalancing`, depending on where the crash loop is.

Why it never recovers:
- The app crashes **before** it can commit the bad record's offset.
- After each restart it continues from the last committed offset, reads the same record, and crashes
  again.
- Restarting the app can't fix a bad **record**. Something has to move the group past it.

---

## Part 4 · Fix: park the record, skip it, and deploy a safer release

### Step 12 · Stop the app
```bash
pkill -f sensor-app.sh
```
✅ **Expected:** `Terminated`. The group needs to be empty before you can change its offsets.

### Step 13 · Create a dead letter topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic sensors-dlq --partitions 1 --replication-factor 3
```
✅ **Expected:** `Created topic sensors-dlq.` This is where records the app can't handle will go, so
nobody loses them.

### Step 14 · Copy the bad record to the dead letter topic
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic sensors --partition 0 --offset $BAD --max-messages 1 2>/dev/null | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic sensors-dlq
```
✅ **Expected:** no output. The record is now saved for someone to look at later.

### Step 15 · Preview moving the group past the bad record
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --group sensor-app --topic sensors:0 --reset-offsets --to-offset $((BAD + 1)) --dry-run
```
✅ **Expected:** a table with `sensor-app  sensors  0  1235` (your `$BAD` + 1) under `NEW-OFFSET`. Nothing
has changed yet. If it says the group is not inactive, wait 10 s and run it again.

### Step 16 · Move the group
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --group sensor-app --topic sensors:0 --reset-offsets --to-offset $((BAD + 1)) --execute
```
✅ **Expected:** the same table, now applied.

### Step 17 · Deploy release 2: bad records go to the dead letter topic
```bash
nohup bash /apps/sensor-app.sh --dlq >/dev/null 2>&1 &
```
✅ **Expected:** a job line. Release 2 doesn't crash on a bad record: it copies it to `sensors-dlq` and
carries on. **Wait ~20 seconds.**

---

## Part 5 · Back to normal

### Step 18 · Check that the lag drains and stays small
```bash
bash /apps/lag-watch.sh sensor-app 3 5
```
✅ **Expected:** the backlog from the crash loop drains within seconds, then the lag stays small, as in
step 5.

### Step 19 · Send another bad reading
```bash
echo '{"sensor":"s4","temp":"N/A"}' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic sensors; sleep 10; tail -2 /tmp/s08-app.log
```
✅ **Expected:** the last line is `bad record at offset … sent to sensors-dlq: {"sensor":"s4","temp":"N/A"}`,
with no `CRASH` after it. `tail -2 /tmp/s08-readings.log` shows fresh readings.

### Step 20 · What's in the dead letter topic?
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic sensors-dlq --from-beginning --max-messages 2
```
✅ **Expected:** the two bad readings, then `Processed a total of 2 messages`.

---

## Part 6 · Clean up

### Step 21 · Stop the app and the sensors
```bash
pkill -f "sensor-app.sh|sensor-producer.sh"; sleep 5
```
✅ **Expected:** two `Terminated` lines.

### Step 22 · Delete the consumer group, the topics and the logs
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group sensor-app
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic sensors
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic sensors-dlq
rm -f /tmp/s08-*
```
✅ **Expected:** `Deletion of requested consumer groups ('sensor-app') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** Kafka delivers records in order within a partition, and a consumer only moves on once its
  offset is committed. A record that always crashes the app gets delivered again and again, and it
  blocks everything behind it in that partition. That's a **poison pill**.
- **In real apps** the bad record is often one the deserializer rejects: wrong format, a schema change,
  a byte-level mix-up. Java consumers then throw a `RecordDeserializationException` that names the
  partition and offset, the same information as `/tmp/s08-app.log` here.
- **Fix, right now:**
  1. Find the partition and offset (from the app's error).
  2. Save the record (a dead letter topic or a file).
  3. Stop the group.
  4. Preview with `--reset-offsets --to-offset N+1 --dry-run`, then run it with `--execute`.
  - `--shift-by 1` also works when the group sits exactly on the bad record.
- **Prevent it:**
  - Catch deserialization and processing errors per record. Send bad records to a **dead letter topic**
    with the error in a header, then carry on. Kafka Streams and Kafka Connect (`errors.tolerance=all`
    plus a DLQ topic) have this built in.
  - Validate at the producer side (a schema registry with compatibility rules), so bad records don't
    reach the topic.
  - Alert on a consumer that restarts repeatedly **and** has growing lag. That combination almost
    always means a poison pill.
