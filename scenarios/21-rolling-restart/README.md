# 21 · A rolling restart gone wrong

**What you'll learn:** why restarting two brokers at once takes a 3-broker cluster down, why the
PodDisruptionBudget doesn't save you from `kubectl delete pod`, and what a safe restart actually waits for.

**Time:** about 25 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open (it survives the broker restarts):
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 21`.

---

## Part 1 · Normal: a healthy cluster before maintenance

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic rolling --partitions 6 --replication-factor 3 --config min.insync.replicas=2
```
✅ **Expected:** `Created topic rolling.`

### Step 2 · Writes work and everything is in sync
```bash
bash /apps/produce-check.sh rolling 5 all
bash /apps/isr-watch.sh rolling 2 5
```
✅ **Expected:** `5 accepted, 0 rejected (acks=-1)` and `under-replicated 0   offline 0`.

### Step 3 · PowerShell: the safety net Kubernetes has
```powershell
kubectl -n kafka-lab get pdb
```
✅ **Expected:** a PodDisruptionBudget for `kafka-controller` (the chart creates one) with
`ALLOWED DISRUPTIONS 1`. It says: *at most one broker pod may be disrupted at a time*.

---

## Part 2 · Break: "the change window is short, let's restart two at once"

### Step 4 · PowerShell: delete two broker pods at the same time
```powershell
kubectl -n kafka-lab delete pod kafka-controller-1 kafka-controller-2 --wait=false
```
✅ **Expected:** two `pod ... deleted` lines, **immediately**. The PodDisruptionBudget did not stop
this — that's the first lesson, and step 8 explains it.

### Step 5 · Check what clients see (run this right away)
```bash
bash /apps/produce-check.sh rolling 45 all
```
✅ **Expected:** a long run of `ERROR` lines — `NotEnoughReplicasException`, `TimeoutException`,
`NotLeaderOrFollowerException` — then `ok` again once the pods are back, and a summary with a large
`rejected` count. The application was down for roughly a minute.

---

## Part 3 · Observe: what does the problem look like?

### Step 6 · The state of the cluster while it recovers
```bash
bash /apps/isr-watch.sh rolling 6 5
```
✅ **Expected:** `under-replicated 6` with `offline` sometimes above 0 right after the deletion, going
back to `0` a minute or two later.

### Step 7 · Why writes were rejected, not just slow
Only one replica of each partition was left, and the topic requires two in sync
(`min.insync.replicas=2`), so `acks=all` writes are refused (scenario 11). With **two of three**
brokers gone, the KRaft controller quorum also lost its majority, so nothing could be decided while it
lasted:
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
```
✅ **Expected:** by now the pods are back and you see a normal status with 3 voters. During the outage
this command would have failed or timed out.

### Step 8 · PowerShell: what the PodDisruptionBudget actually protects
```powershell
kubectl -n kafka-lab get pdb kafka-controller -o jsonpath="{.status.currentHealthy}/{.status.desiredHealthy} healthy, {.status.disruptionsAllowed} disruption(s) allowed{'\n'}"
```
✅ **Expected:** something like `3/2 healthy, 1 disruption(s) allowed` now that the cluster recovered.
A PDB is only consulted by the **eviction** API — `kubectl drain`, node upgrades, the cluster
autoscaler. A plain `kubectl delete pod` (or a `helm upgrade`, or someone pulling a cable) bypasses it
completely. It is a guard rail for *tools*, not a lock on your brokers.

---

## Part 4 · Fix: restart one broker at a time, and wait for the right thing

The rule: never touch the next broker until the previous one is **Ready and caught up** — 0
under-replicated partitions, not just a green pod. `lab/safe-rolling-restart.sh` does exactly that.

### Step 9 · PowerShell: read the script, then run it
```powershell
wsl -d Ubuntu -- bash lab/safe-rolling-restart.sh
```
✅ **Expected:** it checks the cluster is healthy, then for each broker: deletes the pod, waits for
Ready, waits for `0 under-replicated partitions`, and only then moves on. It ends with
`rolling restart finished: every broker restarted, never more than one at a time`. Takes ~3–5 minutes.

### Step 10 · Check the clients while it runs (start this immediately after step 9)
```bash
bash /apps/produce-check.sh rolling 120 all
```
✅ **Expected:** almost all `ok`. A couple of errors right when a leader moves are normal — the
application retries those. Compare the summary line with step 5.

---

## Part 5 · Back to normal

### Step 11 · Replication and leadership
```bash
bash /apps/isr-watch.sh rolling 3 5
bash /apps/leaders.sh rolling
```
✅ **Expected:** `under-replicated 0   offline 0`, and 2 partitions led per broker (the script ends with
a preferred leader election).

### Step 12 · PowerShell: all pods stable
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** three pods `1/1 Running`.

---

## Part 6 · Clean up

### Step 13 · Delete the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic rolling
```
✅ **Expected:** no output.

---

## Why it happened, and how to prevent it

- **Why:** with RF 3, losing two brokers leaves one replica per partition. That's below
  `min.insync.replicas=2`, so `acks=all` writes are rejected, and below the KRaft quorum majority, so
  the controller can't elect leaders or change anything. One broker at a time is not a style
  preference — it's the only safe number in a 3-node cluster.
- **"Ready" is not "caught up".** A restarted broker becomes Ready in seconds but still has to copy
  everything it missed. `kubectl rollout restart statefulset` walks pods one at a time and waits for
  Ready — not for the ISR. That's why the script waits for `--under-replicated-partitions` to be empty.
- **A PodDisruptionBudget only blocks evictions.** It stops `kubectl drain`, node upgrades and
  autoscalers from taking a second broker. It does nothing about `kubectl delete pod`, a Helm upgrade,
  a crash, or a node that reboots. Keep it (`maxUnavailable: 1`), but don't mistake it for a safety
  guarantee.
- **A safe broker restart, in order:**
  1. Check: 0 under-replicated, 0 offline partitions, all brokers Ready.
  2. Restart one broker.
  3. Wait for Ready **and** 0 under-replicated partitions.
  4. Repeat for the next broker.
  5. Preferred leader election at the end (scenario 04).
- **Also worth having:** `acks=all` with retries in the applications (a leader change should be a
  hiccup, not an outage), alerts on `UnderMinIsrPartitionCount` and `OfflinePartitionsCount`, and
  brokers spread over different nodes so one node reboot can't take two of them.
