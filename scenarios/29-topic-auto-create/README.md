# 29 · A typo creates a topic

**What you'll learn:** what `auto.create.topics.enable=true` really does when someone misspells a topic
name — a new topic with default settings, data that goes nowhere, and no error anywhere — and what the
error looks like once you turn it off.

**Time:** about 20 minutes (one Helm upgrade each way).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact messages
> will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 29` (it also puts
  auto-creation back, because the rest of the lab expects it).

---

## Part 1 · Normal: a topic created on purpose

### Step 1 · Create the real topic, with the settings the team decided on
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic orders-app --partitions 6 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000
```
✅ **Expected:** `Created topic orders-app.`

### Step 2 · Look at what that gave you
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders-app
```
✅ **Expected:** `PartitionCount: 6`, `ReplicationFactor: 3`, and a `Configs:` list with
`min.insync.replicas=2` and `retention.ms=604800000`.

### Step 3 · The app writes to it
```bash
bash /apps/produce-check.sh orders-app 5 all
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic orders-app --time -1 | awk -F: '{ s += $3 } END { print "messages in orders-app:", s }'
```
✅ **Expected:** `5 accepted, 0 rejected (acks=all)` and `messages in orders-app: 5`.

---

## Part 2 · Break: a deploy ships a typo

Someone wrote `ordres` in a config file. The deploy goes out; no test catches it.

### Step 4 · The app writes to the misspelled topic
```bash
bash /apps/produce-check.sh ordres 5 all
```
✅ **Expected:** perhaps one `ERROR` on the very first attempt (the topic is being created at that
moment), then `ok` — and a summary with most or all messages **accepted**. Nothing warned anybody.

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · A new topic exists
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep ord
```
✅ **Expected:** both `orders-app` and `ordres`.

### Step 6 · And it is nothing like the real one
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic ordres
```
✅ **Expected:** `PartitionCount: 1` and an empty `Configs:` — it was created from the broker defaults
(`num.partitions`, `default.replication.factor`), not from your topic definition. No
`min.insync.replicas`, no retention policy, one sixth of the parallelism.

### Step 7 · The real pipeline never saw those messages
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic orders-app --time -1 | awk -F: '{ s += $3 } END { print "orders-app:", s }'
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic ordres --time -1
```
✅ **Expected:** `orders-app: 5` (unchanged) and `ordres:0:5`. Five orders are sitting in a topic no
consumer subscribes to, with a retention nobody chose.

### Step 8 · Consumers can create topics too
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic odrers --timeout-ms 8000 2>/dev/null; kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep odrers
```
✅ **Expected:** no messages, then `odrers` in the list — subscribing to a missing topic created it, empty.
(`allow.auto.create.topics=false` on the consumer prevents that half.)

---

## Part 4 · Fix: only create topics on purpose

`auto.create.topics.enable` is a **read-only** broker setting, so it takes a restart — through Helm.

### Step 9 · PowerShell: turn auto-creation off
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/29-topic-auto-create/values-no-auto-create.yaml --wait --timeout 10m
```
✅ **Expected:** after ~2–3 minutes, `Release "kafka" has been upgraded. Happy Helming!` The brokers
restart one at a time.

### Step 10 · Check the setting
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep auto.create.topics.enable
```
✅ **Expected:** `auto.create.topics.enable=false …`

### Step 11 · Delete the topics the typos created
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic ordres
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic odrers
```
✅ **Expected:** no output.

---

## Part 5 · Back to normal

### Step 12 · Now the typo fails loudly
```bash
bash /apps/produce-check.sh ordrs 3 all
```
✅ **Expected:** 3 × `ERROR TimeoutException` and `0 accepted, 3 rejected`. The broker answered
`UNKNOWN_TOPIC_OR_PARTITION`, the producer waited for the topic to appear in the metadata, and gave up.
That's an error a deploy test or an alert can catch.

### Step 13 · And nothing was created
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep ordrs; echo "(nothing above = no topic created)"
```
✅ **Expected:** only the `(nothing above = no topic created)` line.

### Step 14 · The real topic still works
```bash
bash /apps/produce-check.sh orders-app 5 all
```
✅ **Expected:** `5 accepted, 0 rejected (acks=all)`.

---

## Part 6 · Clean up

### Step 15 · Delete the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic orders-app
```
✅ **Expected:** no output.

### Step 16 · PowerShell: put the lab's default back
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml --wait --timeout 10m
```
✅ **Expected:** `Release "kafka" has been upgraded.` Auto-creation is on again, which several other
scenarios in this lab rely on.

---

## Why it happened, and how to prevent it

- **Why:** with `auto.create.topics.enable=true`, any produce, fetch or metadata request for an unknown
  topic creates it, using `num.partitions` (default 1) and `default.replication.factor`. The client
  gets no warning — from its side everything worked.
- **What that costs you:** data in a topic nobody reads; a partition count you can't change without
  breaking key ordering (scenario 03); no `min.insync.replicas`, no retention; and in bigger clusters,
  a slow drift towards thousands of stray partitions (scenario 23).
- **Prevent it:**
  - Turn auto-creation **off** on production brokers, and create topics from code review — Terraform,
    a topic-definition repo, a CI job, or a Kubernetes operator.
  - Where ACLs are enabled, deny `CREATE` on the cluster to application users: even with auto-creation
    on, they then get an authorization error instead of a new topic.
  - Set `allow.auto.create.topics=false` in consumers (it's a client-side setting).
  - Alert on `UNKNOWN_TOPIC_OR_PARTITION` errors from clients, and on the total topic count changing
    outside a deployment.
- **Watch out when you disable it:** clients that relied on auto-creation start failing, and the
  failure is slow — a producer waits `max.block.ms` (60 s by default) for the topic to appear before it
  throws. Internal topics (`__consumer_offsets`, `__transaction_state`) are created by the cluster
  itself and are not affected.
