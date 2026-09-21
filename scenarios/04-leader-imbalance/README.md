# 04 · Leader imbalance after restarts

**What you'll learn:** all reads and writes of a partition go to its *leader*. After broker restarts,
leadership can stay piled up on one broker, which then does most of the work, until someone moves it back.

**Time:** about 20 minutes (it includes a few broker restarts).

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open (it isn't affected by the broker restarts):
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 04`.

---

## Part 1 · Normal: a balanced cluster

This team runs Kafka with **automatic leader rebalancing turned off**, because they want to control when
leadership moves. The topic `inventory` (12 partitions) gets ~500 writes/s, and every broker leads 4 partitions.

### Step 1 · PowerShell: apply the team's setting
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/04-leader-imbalance/values-no-auto-rebalance.yaml --wait
```
✅ **Expected:** after ~2 minutes, `Release "kafka" has been upgraded. Happy Helming!` The brokers restart
one by one. This setting can't be changed while Kafka runs, which is why it goes through Helm.

### Step 2 · Check the setting
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep auto.leader.rebalance
```
✅ **Expected:** `auto.leader.rebalance.enable=false …`

### Step 3 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic inventory --partitions 12 --replication-factor 3 --config retention.bytes=33554432
```
✅ **Expected:** `Created topic inventory.`

### Step 4 · Balance leadership after the restart in step 1
```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
```
✅ **Expected:** `Successfully completed leader election (PREFERRED) for partitions …`, or `Valid replica
already elected …` if nothing had to move.

### Step 5 · Start the write traffic (500 per second)
```bash
nohup kafka-producer-perf-test.sh --topic inventory --num-records 1000000000 --record-size 200 --throughput 500 --producer-props bootstrap.servers=$BOOTSTRAP acks=all >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 6 · Check the leaders
```bash
bash /apps/leaders.sh inventory
```
✅ **Expected:**
```
broker 0 leads   4 partitions
broker 1 leads   4 partitions
broker 2 leads   4 partitions
0 of 12 partitions are NOT on their preferred leader
```

### Step 7 · Check who does the work
```bash
bash /apps/leader-load.sh inventory
```
✅ **Expected** (after 15 s): each broker handles about a third of the writes, ~33% each.

---

## Part 2 · Break: routine maintenance restarts two brokers

It's done correctly: one broker at a time, and the second only after replication has caught up.

### Step 8 · PowerShell: restart broker 0 and wait until it is back
```powershell
kubectl -n kafka-lab delete pod kafka-controller-0
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-0 --timeout=300s
```
✅ **Expected:** `pod "kafka-controller-0" deleted`, then after ~30–60 s `pod/kafka-controller-0 condition met`.
If `wait` says `NotFound`, the pod is still being recreated: run the `wait` line again.

### Step 9 · Wait until replication has caught up
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions | wc -l
```
✅ **Expected:** `0`. If not, wait a few seconds and run it again.

### Step 10 · PowerShell: restart broker 1 the same way
```powershell
kubectl -n kafka-lab delete pod kafka-controller-1
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-1 --timeout=300s
```
✅ **Expected:** as in step 8. Then repeat step 9 until it prints `0`.

---

## Part 3 · Observe: what does the problem look like?

### Step 11 · Where are the leaders now?
```bash
bash /apps/leaders.sh inventory
```
✅ **Expected:** lopsided, with one broker leading nothing. For example:
```
broker 0 leads   4 partitions
broker 1 leads   0 partitions
broker 2 leads   8 partitions
5 of 12 partitions are NOT on their preferred leader
```
Run `bash /apps/leaders.sh` without a topic to see the same skew across all topics.

### Step 12 · Who does the work now?
```bash
bash /apps/leader-load.sh inventory
```
✅ **Expected:** the broker with the most leaders takes about two thirds of the writes, and the broker
with no leaders takes 0%. It only copies data as a follower.

### Step 13 · Is it a replication problem?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic inventory
```
✅ **Expected:** every `Isr:` lists all 3 brokers, so replication is fine. But for many partitions
`Leader:` is not the **first** broker in `Replicas:`, which is the *preferred* leader.

### Step 14 · Why didn't Kafka move leadership back by itself?
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep auto.leader.rebalance
```
✅ **Expected:** `auto.leader.rebalance.enable=false`. Nothing moves leaders back automatically.

---

## Part 4 · Fix: preferred leader election

### Step 15 · Move leadership back to the preferred replicas
```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
```
✅ **Expected:** `Successfully completed leader election (PREFERRED) for partitions …` It's safe to run at
any time: it only moves leadership to a preferred replica that is in sync.

---

## Part 5 · Back to normal

### Step 16 · Check the leaders
```bash
bash /apps/leaders.sh inventory
```
✅ **Expected:** 4 / 4 / 4, and `0 of 12 partitions are NOT on their preferred leader`, as in step 6.

### Step 17 · Check who does the work
```bash
bash /apps/leader-load.sh inventory
```
✅ **Expected:** about a third each again, as in step 7.

---

## Part 6 · Clean up

### Step 18 · Stop the traffic and delete the topic
```bash
pkill -f "topic inventory"; sleep 3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic inventory
```
✅ **Expected:** `Terminated`, then nothing from the delete.

### Step 19 · PowerShell: turn automatic rebalancing back on
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml --wait
```
✅ **Expected:** after ~2 minutes, `Release "kafka" has been upgraded.`

### Step 20 · Balance leadership after that restart
```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
```
✅ **Expected:** `Successfully completed …` or `Valid replica already elected …`

---

## Why it happened, and how to prevent it

- **Why:** when a leader restarts, leadership moves to another in-sync replica. When the broker comes
  back, it rejoins **only as a follower**. Leadership moves back through a *preferred leader election*,
  which is:
  - automatic when `auto.leader.rebalance.enable=true`, the default (checked every 5 minutes);
  - manual with `kafka-leader-election.sh`.
- **Prevent it:** keep automatic rebalancing on, or make the election the **last step of every rolling
  restart**.
- **Not the same problem:** if the replica *placement* itself is uneven (for example after adding
  brokers), an election can't fix it. Use `kafka-reassign-partitions.sh`.
