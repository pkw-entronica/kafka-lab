# 20 · Memory pressure: GC pauses and OOMKilled brokers

**What you'll learn:** how a broker that is short of memory fails — long GC pauses, ISR flapping, moving
leaders, containers killed by the kernel — and why "the JVM heap" and "the container limit" are two
different settings you have to size together.

**Time:** about 30 minutes (two rolling restarts of the cluster).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

> ⚠️ This scenario makes the **whole cluster** unhealthy on purpose, and needs two Helm upgrades to get
> back. Don't run it together with other scenarios.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 20` — it puts the
  normal Helm values back.

---

## Part 1 · Normal: 300 partitions on a properly sized broker

### Step 1 · PowerShell: what the brokers have today
```powershell
kubectl -n kafka-lab get sts kafka-controller -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='KAFKA_HEAP_OPTS')].value}{'\n'}{.spec.template.spec.containers[0].resources}{'\n'}"
```
✅ **Expected:** `-Xms512m -Xmx512m` and a resources block with `"memory":"1536Mi"` as the limit. The
heap is half the container limit: the rest is for the JVM itself, page cache and network buffers.

### Step 2 · Create a topic with 300 partitions
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic manyparts --partitions 300 --replication-factor 3 --config retention.bytes=1048576
```
✅ **Expected:** `Created topic manyparts.` That's 900 replicas, 300 per broker — a realistic number for
a small cluster, and no problem at all when the brokers have memory.

### Step 3 · Start producer and consumer load
```bash
nohup kafka-producer-perf-test.sh --topic manyparts --num-records 1000000000 --record-size 200 --throughput 2000 --producer-props bootstrap.servers=$BOOTSTRAP acks=all >/dev/null 2>&1 &
nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic manyparts --group manyparts-app >/dev/null 2>&1 &
```
✅ **Expected:** two job lines.

### Step 4 · Everything is healthy
```bash
bash /apps/isr-watch.sh manyparts 3 5
bash /apps/produce-check.sh manyparts 5 all
```
✅ **Expected:** `under-replicated 0   offline 0` on every line, and `5 accepted, 0 rejected`.

### Step 5 · PowerShell: no restarts
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** three pods `1/1 Running` with `RESTARTS 0` (or whatever they were before).

---

## Part 2 · Break: someone "saves memory" on the brokers

A cost-saving change cuts the heap to 128 MB and the container limit to 512 MB. It goes through Helm,
because both are pod settings, not Kafka settings.

### Step 6 · PowerShell: apply the smaller memory settings
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/20-memory-pressure/values-small-heap.yaml --wait --timeout 10m
```
✅ **Expected:** the brokers restart one by one. Either `Release "kafka" has been upgraded.` after a few
minutes, **or** an error about waiting for the condition — if Helm times out, that's the first symptom,
not a mistake: the pods can't get healthy. Continue with the next step either way.

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · PowerShell: the pods
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** `RESTARTS` climbing, pods flipping between `Running` and `CrashLoopBackOff`, and often
`0/1` ready.

### Step 8 · PowerShell: why a pod died
```powershell
kubectl -n kafka-lab describe pod kafka-controller-0 | Select-String -Pattern "Restart Count|Last State|Reason|Exit Code|Limits" -Context 0,2
```
✅ **Expected:** a `Last State: Terminated` block with `Reason: OOMKilled` and `Exit Code: 137`. The
**kernel** killed the container for exceeding its memory limit — this is not a Kafka error, and nothing
appears in Kafka's own log about it.

### Step 9 · PowerShell: the garbage collector's story
```powershell
kubectl -n kafka-lab logs kafka-controller-0 --tail=200 | Select-String -Pattern "Pause Full|Pause Young|OutOfMemoryError"
```
✅ **Expected:** many GC lines, with `Pause Full` entries taking hundreds of milliseconds or more, and
heap numbers hugging the 128 MB ceiling (e.g. `126M->125M(128M)`). Every full pause is a moment when the
broker answers nobody: no heartbeats, no fetches, no produce responses.

### Step 10 · What that does to replication
```bash
bash /apps/isr-watch.sh manyparts 8 10
```
✅ **Expected:** `under-replicated` jumping between 0 and several hundred, sometimes with `offline`
above 0. This flapping — not a clean failure — is the classic signature of memory pressure.

### Step 11 · And to leadership
```bash
bash /apps/leaders.sh manyparts
```
✅ **Expected:** a lopsided distribution that changes every time you run it: leaders keep moving away
from whichever broker is paused or restarting.

### Step 12 · What clients see
```bash
bash /apps/produce-check.sh manyparts 10 all
```
✅ **Expected:** a mix of `ok` and `ERROR` (`TimeoutException`, `NotEnoughReplicasException`,
`NotLeaderOrFollowerException`). The application sees an unreliable cluster, not a dead one — the worst
kind to debug.

---

## Part 4 · Fix: give the memory back

### Step 13 · Stop the load
```bash
pkill -f "topic manyparts"; sleep 3
```
✅ **Expected:** `Terminated` lines for the producer and the consumer.

### Step 14 · PowerShell: restore the normal values
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml --wait --timeout 10m
```
✅ **Expected:** after a few minutes, `Release "kafka" has been upgraded. Happy Helming!` The brokers
restart with the 512 MB heap and the 1536Mi limit again.

### Step 15 · Delete the big topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic manyparts
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group manyparts-app
```
✅ **Expected:** nothing from the topic delete (300 partitions take a moment), then
`Deletion of requested consumer groups ('manyparts-app') was successful.`

---

## Part 5 · Back to normal

### Step 16 · PowerShell: stable pods
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** three pods `1/1 Running`. `RESTARTS` shows the damage from Part 3, but the number stops
growing.

### Step 17 · Replication is healthy again
```bash
bash /apps/isr-watch.sh "" 6 10
```
✅ **Expected:** `under-replicated 0   offline 0`, steady, for all topics.

### Step 18 · Clients are happy
```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
bash /apps/produce-check.sh health-check 5 all
```
✅ **Expected:** `Successfully completed leader election (PREFERRED) …` (or `Valid replica already
elected`), then `5 accepted, 0 rejected (acks=all)` on the auto-created topic `health-check`.

---

## Part 6 · Clean up

### Step 19 · Delete the check topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic health-check
```
✅ **Expected:** no output.

### Step 20 · PowerShell: confirm the memory settings are back
```powershell
kubectl -n kafka-lab get sts kafka-controller -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='KAFKA_HEAP_OPTS')].value}{'\n'}"
```
✅ **Expected:** `-Xms512m -Xmx512m`, as in step 1.

---

## Why it happened, and how to prevent it

- **Two limits, one pod:** `-Xmx` is what the JVM may use for its heap; the container's
  `resources.limits.memory` is what the **kernel** allows for everything in the container — heap,
  metaspace, code cache, thread stacks, direct byte buffers and more. When the heap is too small you
  get GC thrashing; when the limit is too close to the heap you get `OOMKilled` (exit code 137).
  A common starting point for a broker: heap ≈ half the container limit.
- **Kafka wants free memory, not a huge heap.** Most of a broker's speed comes from the operating
  system's **page cache** holding recent log segments. A 4–8 GB heap is plenty for most clusters; the
  rest of the RAM is better left to the page cache. Partitions, not messages, drive heap use.
- **Why it looked like a network problem:** a full GC pause stops every thread. The broker misses
  heartbeats, followers stop fetching, the controller fences it, leaders move, and then it comes back —
  over and over. Flapping ISRs with no network fault almost always mean GC or disk stalls.
- **Prevent it:**
  - Enable GC logging on brokers (as this scenario's values file does) and alert on total pause time.
  - Alert on container restarts and `OOMKilled`, not just on "pod Running".
  - Keep partition counts per broker sane (a few thousand at most, scenario 23) — each replica costs
    memory and file handles.
  - Change memory settings one broker at a time, and wait for `UnderReplicatedPartitions` to be 0
    between restarts (scenario 21).
