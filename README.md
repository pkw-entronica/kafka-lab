# Kafka Failure Lab

Break Kafka on purpose, watch what happens, and fix it, using only the Kafka command-line tools.

Every scenario follows the same path:

**Normal** → **Break** → **Observe** → **Fix** → **Back to normal** → **Clean up**

Each step gives you one command to type and the result you should see.

## Scenarios

| # | Scenario | What you'll learn | Time |
|---|---|---|---|
| 01 | [Hot partition](scenarios/01-hot-partition/README.md) | One busy key overloads one consumer, and more consumers don't help | 15 min |
| 02 | [More consumers than partitions](scenarios/02-consumers-exceed-partitions/README.md) | Why extra consumers sit idle, and what really adds capacity | 15 min |
| 03 | [Adding partitions breaks ordering](scenarios/03-partition-increase-ordering/README.md) | How events of one key get processed out of order, and the safe way to add partitions | 15 min |
| 04 | [Leader imbalance](scenarios/04-leader-imbalance/README.md) | Why one broker does most of the work after restarts | 20 min |
| 05 | [Consumer lag](scenarios/05-consumer-lag/README.md) | Is lag caused by too much input or too slow output? | 15 min |
| 06 | [Rebalance storm](scenarios/06-rebalance-storm/README.md) | Why crashing consumers stop the whole group, and how static membership helps | 20 min |
| 07 | [max.poll.interval exceeded](scenarios/07-max-poll-interval/README.md) | Slow batches get a worker thrown out of the group, and jobs run twice | 20 min |
| 08 | [Poison pill](scenarios/08-poison-pill/README.md) | One bad record crash-loops a consumer; skip it and use a dead letter topic | 20 min |
| 09 | [Duplicates after a crash](scenarios/09-duplicates-after-crash/README.md) | At-least-once delivery, the commit window, and idempotent processing | 20 min |
| 10 | [Offset reset surprises](scenarios/10-offset-reset/README.md) | How `auto.offset.reset=latest` silently skips data, and how to rewind a group | 20 min |
| 11 | [acks=1 and data loss](scenarios/11-acks-and-data-loss/README.md) | Why `min.insync.replicas` does nothing for `acks=1`, and what the ISR really promises | 15 min |
| 12 | [Record too large](scenarios/12-record-too-large/README.md) | The broker limit, the producer limit, and where big payloads really belong | 15 min |
| 13 | [Throttled producer](scenarios/13-producer-quota-timeouts/README.md) | A client quota, a full buffer, blocked sends and timeouts | 15 min |
| 14 | [Batching: throughput vs latency](scenarios/14-batching-throughput-latency/README.md) | Measure `linger.ms`, `batch.size`, compression and `acks` on this cluster | 20 min |
| 15 | [Under-replicated partitions](scenarios/15-under-replicated-partitions/README.md) | A slow broker and a missing broker, and how the ISR recovers | 20 min |
| 16 | [Offline partitions](scenarios/16-offline-partitions/README.md) | RF 1 vs RF 3, `NotEnoughReplicas`, and the KRaft quorum majority | 25 min |
| 17 | [Unclean leader election](scenarios/17-unclean-leader-election/README.md) | Offline partition, or acknowledged data gone — offsets going backwards | 30 min |
| 18 | [Disk full](scenarios/18-disk-full/README.md) | A broker fills its 1 GiB disk, and how to get it back without touching a PVC | 30 min |
| 19 | [Network partition](scenarios/19-network-partition/README.md) | A broker that is "Running" but cut off from its peers | 25 min |
| 20 | [Memory pressure](scenarios/20-memory-pressure/README.md) | GC pauses, ISR flapping and `OOMKilled`, from a heap that is too small | 30 min |
| 21 | [A rolling restart gone wrong](scenarios/21-rolling-restart/README.md) | Two brokers at once, what a PodDisruptionBudget really protects, and a safe restart script | 25 min |
| 22 | [Noisy neighbour](scenarios/22-noisy-neighbor/README.md) | One greedy client ruins everyone's latency; quotas give it back | 20 min |
| 23 | [Too many partitions](scenarios/23-too-many-partitions/README.md) | What an idle partition costs: file handles, memory, restart and failover time | 30 min |
| 24 | [Retention deletes unread data](scenarios/24-retention-vs-consumer/README.md) | A consumer down longer than the retention loses messages — and reports lag 0 | 25 min |
| 25 | [Log compaction](scenarios/25-log-compaction/README.md) | Latest value per key, tombstones, and the delete a slow consumer never sees | 25 min |
| 26 | [A corrupted log segment](scenarios/26-segment-corruption/README.md) | Log recovery, truncation, and replication repairing a damaged replica | 25 min |
| 27 | [An open transaction](scenarios/27-open-transaction/README.md) | Why `read_committed` consumers stop at the last stable offset | 20 min |
| 28 | [Retries without idempotence](scenarios/28-idempotence-ordering/README.md) | Duplicates and reordering from a leader change mid-flight | 25 min |
| 29 | [A typo creates a topic](scenarios/29-topic-auto-create/README.md) | Auto-created topics with default settings, and what turning it off looks like | 20 min |
| 30 | [Wrong advertised listeners](scenarios/30-advertised-listeners/README.md) | "It connects, then times out": how bootstrap and advertised addresses work | 25 min |
| 31 | [Authentication and ACLs](scenarios/31-sasl-acls/README.md) | Bad credentials, a missing topic ACL, a missing group ACL — and how to grant them | 35 min |
| 32 | [KRaft quorum loss](scenarios/32-kraft-quorum-loss/README.md) | One controller down is fine, two is not: metadata frozen while data keeps flowing | 25 min |

Scenarios 01–05, 10 and 11 have been run on the lab and their expected results match. The rest haven't
been run yet, so their expected results may need corrections — scenario 11 needed a redesign, and
scenarios 15 and 17 carry a warning about a break that cannot work.
From 15 onwards a scenario stops brokers, fills a disk, cuts the network or changes the Helm values —
run them one at a time, and let `cleanup.sh NN` put the cluster back. Scenario 31 switches on
authentication for everything, so finish its cleanup before you run anything else.

---

## 1. Set up the lab (once)

You need **Docker Desktop** (running), **WSL Ubuntu**, **kubectl** and **Helm**. All four are already on
this PC. Run these in **PowerShell**, in this folder:

**Build the lab** (~3 minutes; safe to run again at any time):
```powershell
wsl -d Ubuntu -- bash lab/install.sh
```
✅ **Expected:** it ends with `OK: lab is up`.

**Check that Kafka works:**
```powershell
wsl -d Ubuntu -- bash lab/smoke-test.sh
```
✅ **Expected:** `RESULT: PASS`

**Open the lab shell.** Almost every scenario command is typed here:
```powershell
kubectl -n kafka-lab exec -it kafka-client -- bash
```
✅ **Expected:** a bash prompt inside the pod `kafka-client`. Three things are ready there:
- `echo $BOOTSTRAP` prints the Kafka address;
- `ls /apps` lists the lab's helper tools;
- Kafka's own tools are already on `PATH`, so the scenarios type `kafka-topics.sh` and not a full path.
  They live in **`/opt/bitnami/kafka/bin`** — `ls /opt/bitnami/kafka/bin` is the quickest way to see
  everything Kafka ships. Use plain `bash` as above: a *login* shell (`bash -l`, `su -`) rebuilds
  `PATH` from `/etc/profile` and the Kafka tools disappear from it.

**If you have other kind clusters**, point kubectl at this one while you work on the lab, because the
`kubectl` commands in the scenarios follow your *current* context:
```powershell
kubectl config use-context kind-kind
```
The lab's own scripts (`install.sh`, `cleanup.sh`, `lab-status.sh` …) don't care: they always target
the `kind-kind` context. Set `KUBE_CONTEXT=...` if you ever rename it.

---

## 2. Run a scenario

1. Open the scenario's README (links above) and follow the steps **in order**.
2. Type each command in the **lab shell**, unless the step says **PowerShell**.
3. Compare the output with **✅ Expected**. Numbers vary a little between runs.
4. Finished, or lost? Reset the scenario from **PowerShell**. This stops its apps, deletes its topics and
   groups, and undoes its settings:
   ```powershell
   wsl -d Ubuntu -- bash cleanup.sh 03
   ```
   Run `wsl -d Ubuntu -- bash cleanup.sh` without a number to reset every scenario.

To see the whole lab at a glance at any time (brokers, disks, topics, lag, running apps):
```powershell
wsl -d Ubuntu -- bash lab/lab-status.sh
```

### The helper tools in `/apps`

The scenarios use the real Kafka tools (`kafka-topics.sh`, `kafka-consumer-groups.sh` …) wherever they
can. A few things would be too long to type, so each one is a small script you can read with
`cat /apps/<name>`:

| Tool | What it does |
|---|---|
| `lag-watch.sh GROUP` | total lag every 10 s, and whether it is growing or draining |
| `in-out.sh TOPIC GROUP` | messages/s coming in vs. going out through the group |
| `new-per-partition.sh TOPIC` | where new messages land, per partition |
| `leaders.sh [TOPIC]` | leaders per broker, and partitions not on their preferred leader |
| `leader-load.sh TOPIC` | which broker handles the writes |
| `isr-watch.sh [TOPIC]` | under-replicated and offline partitions, sampled over time |
| `replica-sizes.sh TOPIC` | how many MB of a topic each broker really stores |
| `produce-check.sh TOPIC [SECONDS] [ACKS]` | one write per second, printing `ok` or the exact error |
| `perf-table.sh` | saved `kafka-producer-perf-test.sh` runs, side by side as one table |
| `seq-check.sh FILE` | duplicates and out-of-order records in what a consumer read |
| `group-state.sh GROUP` | the group's state (`Stable`, `PreparingRebalance` …) and member count, every 2 s |
| `numbered-producer.sh TOPIC PREFIX RATE` | numbered messages (`PREFIX-000001`, …), steadily or all at once with `--burst COUNT` |
| `gap-check.sh FILE PREFIX` | which numbered messages a consumer wrote to FILE: how many, duplicates, and gaps |
| `slow-consumer.sh TOPIC GROUP NAME DELAY` | a consumer that spends DELAY seconds on each message |
| `order-producer.sh`, `account-producer.sh`, `account-consumers.sh`, `order-check.sh` | the simulated apps of scenarios 01 and 03 |
| `storm-group.sh`, `job-worker.sh`, `sensor-producer.sh`, `sensor-app.sh`, `ledger-consumer.sh` | the simulated apps of scenarios 06–09 |
| `batch-job.sh` | scenario 22's noisy neighbour: reads a whole topic over and over |

### Optional: watch it in a browser

In PowerShell, run the line below, then open <http://localhost:8080> (kafka-ui). It shows brokers,
topics, partitions and consumer lag.
```powershell
kubectl -n kafka-lab port-forward svc/kafka-ui 8080:8080
```

---

## 3. Clean up

Four levels, from the one you'll use every day to the one that removes everything. All of them run in
**PowerShell**, from the project folder.

### After a scenario — reset that scenario (the usual one)

Stops its apps, deletes its topics and consumer groups, and undoes any cluster settings it changed
(throttles, quotas, a scaled-down StatefulSet, a NetworkPolicy, Helm values). The brokers and their
data stay.
```powershell
wsl -d Ubuntu -- bash cleanup.sh 10
```
Leave out the number to reset **every** scenario: `wsl -d Ubuntu -- bash cleanup.sh`.

### Done for today — stop the lab, keep everything

Quit Docker Desktop, or just stop the node container. Nothing is lost: the Kafka data lives in the
broker disk images inside it.
```powershell
docker stop kind-control-plane
```
To come back: start Docker Desktop, `docker start kind-control-plane`, then re-mount the broker disks
with `wsl -d Ubuntu -- bash lab/node-disks.sh` — loop mounts don't survive a restart of the node
container, and the brokers won't start without them.

### Start Kafka from scratch — wipe the data, keep the cluster

Use this when the lab is in a state no `cleanup.sh` can fix. It deletes **all** Kafka data and the
three 1 GiB disk images, then rebuilds. Two of these reach **outside** the `kafka-lab` namespace:
`delete -f lab/storage.yaml` removes the cluster-wide `kafka-lab-disk-*` PersistentVolumes and the
`kafka-lab-1g` StorageClass, and the `docker exec` deletes files inside the kind node. The
`--context` / `--kube-context` flags are there on purpose — these commands are destructive, and
without them they would follow whatever context kubectl happens to be pointing at.
```powershell
helm --kube-context kind-kind -n kafka-lab uninstall kafka
kubectl --context kind-kind -n kafka-lab delete pvc --all
kubectl --context kind-kind delete -f lab/storage.yaml
docker exec kind-control-plane sh -c 'umount /mnt/kafka-disks/disk-*; rm -f /var/kafka-disks/disk-*.img'
kubectl --context kind-kind delete namespace kafka-lab
wsl -d Ubuntu -- bash lab/install.sh
```
The PVs use `persistentVolumeReclaimPolicy: Retain`, so deleting the PVCs alone leaves them `Released`
and nothing will rebind — that's why `kubectl delete -f lab/storage.yaml` is not optional.

> Don't run `helm uninstall` on its own just to change a setting: it deletes the KRaft cluster id, and
> the brokers then refuse the data already on their disks. Edit `lab/values.yaml` and re-run
> `lab/install.sh` instead.

### Finished with the lab — remove it completely

Deletes the kind cluster, its node container, the broker disk images and everything in it. Only the
files in this repo are left.
```powershell
kind delete cluster --name kind
```
**Name the cluster explicitly.** Plain `kind delete cluster` targets the one called `kind`, which *is*
this lab — but if you keep other kind clusters, being explicit is what stops you deleting the wrong
one. `kind get clusters` lists them. The Bitnami and kind images stay in Docker; remove them with
`docker image prune -a` if you want the disk space back.

---

## 4. Troubleshooting

| Problem | Fix |
|---|---|
| `Unable to connect to the server` | Start Docker Desktop and wait ~1 minute. |
| Brokers stuck in `ContainerCreating` after Docker restarted | `wsl -d Ubuntu -- bash lab/node-disks.sh` re-mounts the broker disks. |
| A command says something isn't found (`kind-control-plane`, the namespace, the pod) | kubectl is pointed at a different cluster. `kubectl config current-context` should say `kind-kind`; fix it with `kubectl config use-context kind-kind`. |
| `ls /apps` is empty or missing, or a tool a scenario uses isn't there | Run `lab/install.sh` again, then reopen the lab shell. |
| `kafka-topics.sh: command not found` in the lab shell | You're in a *login* shell (`bash -l`, `su -`), which resets `PATH`. Leave it and reopen with `kubectl -n kafka-lab exec -it kafka-client -- bash`, or call the tool by full path: `/opt/bitnami/kafka/bin/kafka-topics.sh`. |
| A command doesn't show the ✅ Expected result | Wait a few seconds and run it again (Kafka clients need time to notice changes). Still wrong? Reset with `cleanup.sh NN` and restart the scenario. |
| `Wsl/Service/0x8007274c` or `UtilAcceptVsock … failed` | Windows is low on free memory. Close other apps, run `wsl --terminate Ubuntu`, and retry. Docker and Kafka keep running. |

---

## 5. Project layout

```
README.md                    this guide
scenarios/NN-name/README.md  the scenarios, step by step
scenarios/NN-name/*.yaml     the manifests and Helm overrides a few scenarios apply
cleanup.sh                   reset one scenario or all of them
kafka-failure-lab-prompts.md the original plan all 32 scenarios were written from
lab/                         the lab itself
  install.sh                 build or repair the lab
  lab-status.sh              health view
  smoke-test.sh              end-to-end produce/consume check
  node-disks.sh              re-mount the broker disks after a Docker restart
  safe-rolling-restart.sh    restart the brokers one at a time, waiting for the ISR (scenario 21)
  apps/                      the helper tools, mounted at /apps in the lab shell
  values.yaml, *.yaml        Helm values and Kubernetes manifests
  lib.sh                     shared code for the scripts above
```

<details>
<summary><b>How the lab is built</b> (for the curious)</summary>

| What | Value |
|---|---|
| Cluster | local **kind** cluster with a single node (`kind-control-plane`), namespace `kafka-lab` |
| Kafka | Helm release `kafka`, Bitnami chart 32.4.3, Kafka 4.0.0, 3 brokers (KRaft, combined controller+broker), PLAINTEXT, no auth |
| Broker defaults | `default.replication.factor=3`, `min.insync.replicas=2`, `auto.create.topics.enable=true`, `unclean.leader.election.enable=false` |
| Bootstrap | `kafka.kafka-lab.svc.cluster.local:9092` (`$BOOTSTRAP` in the lab shell) |
| Disks | a real 1 GiB disk per broker (loop-mounted ext4, set up by `lab/node-disks.sh`), so "disk full" is real |
| Lab shell | pod `kafka-client`, same Kafka image, tools from `lab/apps` mounted at `/apps` |
| Kafka CLI tools | `/opt/bitnami/kafka/bin` (on `PATH` in the lab shell and in the broker pods) |
| UI | kafka-ui (kafbat) |

Things to know:
- **Single node:** all brokers run on one Kubernetes node, so "a node dies" is simulated by deleting a broker pod.
- **Bitnami images** moved to `docker.io/bitnamilegacy/*` in 2025. `lab/values.yaml` points there, so the
  "substituted images" warnings during install are expected.
- **Don't `helm uninstall`** to reset settings. It deletes the cluster id, and the brokers would then refuse
  their data. Change `lab/values.yaml` and run `lab/install.sh` again instead.
- **Storage:** three PersistentVolumes `kafka-lab-disk-0/1/2` (StorageClass `kafka-lab-1g`,
  `persistentVolumeReclaimPolicy: Retain`) bound to the StatefulSet's `data-kafka-controller-N` claims.
  Each points at `/mnt/kafka-disks/disk-N/data`, which only exists inside a loop-mounted ext4 image at
  `/var/kafka-disks/disk-N.img` in the kind node.
- **Wiping or removing the lab:** see [3. Clean up](#3-clean-up).

</details>

<details>
<summary><b>Adding a scenario</b></summary>

1. Create `scenarios/NN-name/README.md` with the same parts as the others:
   **Normal → Break → Observe → Fix → Back to normal → Clean up**, then "Why it happened".
2. Every step gets one command and one **✅ Expected** line. Run every command yourself before you write
   down what to expect.
3. **Normal** must start real traffic and show that it's healthy. **Break** changes exactly one thing.
   **Back to normal** repeats the checks from **Normal**.
4. If something is too long to type, add a small script to `lab/apps/`, then run `lab/install.sh` so
   it appears in `/apps`.
5. Add the scenario's apps, topics and groups to the table at the top of `cleanup.sh`.

</details>
