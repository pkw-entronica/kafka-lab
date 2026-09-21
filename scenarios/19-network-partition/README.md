# 19 · A broker cut off from its peers

**What you'll learn:** what a network partition looks like from three sides at once — the isolated
broker, the rest of the cluster, and the clients — and why "the pod is Running" says nothing about
whether a broker is working.

**Time:** about 25 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 19`.

---

## Part 1 · Normal: a healthy three-broker cluster

### Step 1 · Create the topic and start traffic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic netpart --partitions 6 --replication-factor 3 --config retention.bytes=33554432
nohup kafka-producer-perf-test.sh --topic netpart --num-records 1000000000 --record-size 200 --throughput 500 --producer-props bootstrap.servers=$BOOTSTRAP acks=all >/dev/null 2>&1 &
```
✅ **Expected:** `Created topic netpart.` and a job line like `[1] 2345`.

### Step 2 · Everything is in sync, and every broker leads its share
```bash
bash /apps/isr-watch.sh netpart 2 5
bash /apps/leaders.sh netpart
```
✅ **Expected:** `under-replicated 0   offline 0`, and each broker leading 2 partitions.

### Step 3 · PowerShell: check the node's address (the policy needs it)
```powershell
kubectl get node kind-control-plane -o wide
```
✅ **Expected:** an `INTERNAL-IP` like `172.18.0.2`. If it is **not** inside `172.18.0.0/16`, edit the
two `ipBlock` lines in `scenarios/19-network-partition/netpol-isolate-broker-2.yaml` first, otherwise
Kubernetes' health probes get blocked too and the pod will be restarted.

---

## Part 2 · Break: broker 2 loses contact with the other two

A switch fails, a firewall rule lands, a security group changes. Broker 2 keeps running and stays
reachable for clients, but it can no longer talk to brokers 0 and 1.

### Step 4 · PowerShell: apply the network policy
```powershell
kubectl apply -f scenarios/19-network-partition/netpol-isolate-broker-2.yaml
```
✅ **Expected:** `networkpolicy.networking.k8s.io/isolate-broker-2 created`. **Wait ~60 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · PowerShell: the pod looks perfectly healthy
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** `kafka-controller-2` still `1/1 Running`, probably with 0 restarts. Kubernetes is happy:
the process is alive and its port answers. Nothing here hints at an outage.

### Step 6 · Kafka disagrees
```bash
bash /apps/isr-watch.sh netpart 6 10
```
✅ **Expected:** `under-replicated` climbs to about 4–6 partitions within a minute: broker 2 can't
replicate any more, so the other brokers drop it from their ISRs, and the partitions it led are taken
over by brokers 0 and 1.

### Step 7 · Leadership moved away from broker 2
```bash
bash /apps/leaders.sh netpart
```
✅ **Expected:** `broker 2 leads   0 partitions`, with its share split between brokers 0 and 1. The
controller fences a broker that stops sending heartbeats (`broker.session.timeout.ms`, 9 s by default).

### Step 8 · The quorum's view
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```
✅ **Expected:** the rows for nodes 0 and 1 are up to date, while node 2 shows an old
`LastFetchTimestamp` and a growing lag. Two of three voters are still a majority, so the cluster keeps
making decisions.

### Step 9 · The isolated broker's own logs
```powershell
kubectl -n kafka-lab logs kafka-controller-2 --tail=30 | Select-String -Pattern "ERROR|WARN|timed out|Connection|disconnect"
```
✅ **Expected:** repeated connection failures and timeouts as it tries to reach the other nodes: it
knows something is wrong, but it can't do anything about it.

### Step 10 · What clients see
```bash
bash /apps/produce-check.sh netpart 10 all
```
✅ **Expected:** mostly `ok`. Clients reach brokers 0 and 1, which now lead every partition, so the
application barely notices — while the cluster is running without any spare replica. A few errors right
after the break are normal (metadata takes a moment to catch up).

### Step 11 · The real risk
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-min-isr-partitions; echo "(nothing above = writes still safe)"
```
✅ **Expected:** usually nothing: with RF 3 and `min.insync.replicas=2`, two replicas is still enough.
One more broker problem now, and writes stop (scenario 16) or data is at risk (scenario 17).

---

## Part 4 · Fix: restore the network

### Step 12 · PowerShell: remove the policy
```powershell
kubectl delete -f scenarios/19-network-partition/netpol-isolate-broker-2.yaml
```
✅ **Expected:** `networkpolicy.networking.k8s.io "isolate-broker-2" deleted`.

---

## Part 5 · Back to normal

### Step 13 · Replication catches up
```bash
bash /apps/isr-watch.sh netpart 8 10
```
✅ **Expected:** `under-replicated` back to `0` within a minute or two, as broker 2 rejoins the ISRs.

### Step 14 · Give broker 2 its leadership back
```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
bash /apps/leaders.sh netpart
```
✅ **Expected:** `Successfully completed leader election (PREFERRED) …`, then 2 partitions per broker
again, as in step 2. (Kafka would also do this by itself within ~5 minutes.)

### Step 15 · Clients are fine
```bash
bash /apps/produce-check.sh netpart 5 all
```
✅ **Expected:** `5 accepted, 0 rejected (acks=-1)`.

---

## Part 6 · Clean up

### Step 16 · Stop the traffic and delete the topic
```bash
pkill -f "topic netpart"; sleep 3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic netpart
```
✅ **Expected:** `Terminated`, then nothing from the delete.

### Step 17 · PowerShell: make sure the policy is gone
```powershell
kubectl -n kafka-lab get networkpolicy
```
✅ **Expected:** `No resources found in kafka-lab namespace.`

---

## Going further (optional): 300 ms of latency instead of a cut

A slow link is harder to spot than a broken one. This lab can't inject latency by itself: the broker
image has no `tc`, and the pods don't have `NET_ADMIN`.

1. Measure the current produce latency:
   ```bash
   kafka-producer-perf-test.sh --topic netpart --num-records 20000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=all | tail -1
   ```
   Note the `99th` percentile.
2. Install Chaos Mesh and apply the delay — the install instructions are in the header of
   [`networkchaos-delay.yaml`](networkchaos-delay.yaml). It creates the namespace `chaos-mesh`, outside
   `kafka-lab`, so decide for yourself whether you want that in this cluster.
3. Measure again with the same command. With `acks=all`, every write waits for the slowest in-sync
   replica, so a 300 ms delay on one broker adds roughly 300 ms to the p99 of the partitions it
   replicates — while `UnderReplicatedPartitions` may still be 0.
4. Remove the delay (`kubectl delete -f …`) and measure once more.

---

## Why it happened, and how to prevent it

- **A partition is not a crash.** The process is alive, the port answers, the liveness probe passes —
  and the broker is useless to the cluster. Any check that only asks "is the pod running?" misses this
  completely.
- **What Kafka does about it:** the controller stops receiving the broker's heartbeats and **fences**
  it (`broker.session.timeout.ms`, 9 s): it loses its leaderships and is removed from every ISR. The
  data stays safe as long as the survivors keep a majority and enough in-sync replicas.
- **Asymmetric partitions are worse.** If a broker can reach the controller but not its peers (or the
  other way around), it can flap in and out of the ISR, causing repeated leader changes. Watch for a
  broker that keeps rejoining.
- **Prevent and detect:**
  - Alert on `UnderReplicatedPartitions` > 0 and on a broker whose leader count drops to 0 — both are
    visible in this scenario before any client complains.
  - Monitor the client side too: produce latency and error rate per broker.
  - Keep `min.insync.replicas=2` with RF 3 so one isolated broker can't put you one failure away from
    data loss, and spread replicas across racks/zones so a single network fault takes only one replica.
  - When you use NetworkPolicies in production, remember they are allow-lists: forgetting the kubelet's
    probe traffic (step 3) turns "isolated" into "restarted every 30 seconds".
