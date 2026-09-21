# Kafka Failure Lab: Prompts for Claude Code

**Lab setup:** Kubernetes · Bitnami Helm chart · 3 brokers in KRaft mode · Kafka CLI tools only

## How to use this file

1. Run **Prompt 0 (Setup)** once to build the cluster and a client pod.
2. For each scenario, paste the **Shared context** block and then the scenario prompt into Claude Code.
3. Every scenario follows the same five steps: **Baseline → Inject → Observe → Recover → Cleanup**, so each one leaves the lab clean for the next.
4. Run everything in a dedicated namespace (`kafka-lab`). Never point these prompts at a real cluster.

---

## Shared context (paste this before every scenario prompt)

```text
CONTEXT: I run a Kafka failure lab on Kubernetes.
- Namespace: kafka-lab. Helm release: kafka (Bitnami chart), 3 combined controller+broker nodes, KRaft, PLAINTEXT listeners, no auth.
- Bootstrap: kafka.kafka-lab.svc.cluster.local:9092. Broker pods: kafka-controller-0/1/2.
- A long-running pod "kafka-client" (same Kafka image) is where all CLI commands run: kubectl -n kafka-lab exec -it kafka-client -- bash
- Use ONLY the Kafka CLI tools (kafka-topics.sh, kafka-console-producer/consumer.sh, kafka-producer-perf-test.sh, kafka-consumer-perf-test.sh, kafka-consumer-groups.sh, kafka-configs.sh, kafka-reassign-partitions.sh, kafka-leader-election.sh, kafka-log-dirs.sh, kafka-dump-log.sh, kafka-transactions.sh, kafka-get-offsets.sh), plus kubectl and plain bash. No custom apps.
- Before running anything, check the real pod, service and script names in my cluster and adjust if they differ.
- Put each scenario in ./scenarios/<NN-name>/ containing: README.md (what the issue is, the root cause, what I should see), 01-baseline.sh, 02-inject.sh, 03-observe.sh, 04-recover.sh, 05-cleanup.sh.
- Scripts must be idempotent, echo what they do, and print the key evidence (lag, ISR, errors) clearly.
- After writing the scripts, run them step by step, show me the output, and explain what it proves. Stop and ask me before any step that deletes PVCs or anything outside the kafka-lab namespace.
```

---

## Prompt 0: Build the lab

```text
Build a Kafka failure lab on my Kubernetes cluster:
1. Create namespace kafka-lab.
2. Install the Bitnami Kafka Helm chart as release "kafka": 3 controller-eligible brokers (KRaft, no ZooKeeper), PLAINTEXT client and inter-broker listeners (no SASL), persistence enabled with SMALL PVCs (1Gi) so I can fill a disk later, and a JVM heap of about 512m. Bitnami has changed its free image/chart distribution in the past, so first check which chart version and image repository currently work, and tell me if you need an alternative.
3. Set these broker defaults: default.replication.factor=3, min.insync.replicas=2, auto.create.topics.enable=true (some scenarios need it; we will toggle it), unclean.leader.election.enable=false.
4. Create a pod "kafka-client" with the same Kafka image and a command of sleep infinity.
5. Optional but recommended: deploy kafbat/kafka-ui in the namespace and tell me how to port-forward to it, so I can see partitions, ISR and lag visually.
6. Write lab-status.sh, which prints: broker pods and their nodes, topic list, a describe of under-replicated/offline partitions (--under-replicated-partitions, --unavailable-partitions), and lag for all consumer groups.
7. Smoke test: create topic smoke (6 partitions, RF 3), produce 1000 messages, consume them, then delete the topic.
Put the Helm values in ./lab/values.yaml and the steps in ./lab/README.md. Tell me if my cluster has only one k8s node, because that affects the node-failure scenarios.
```

---

## A. Partitions and load

### 1. Hot partition (key skew)
```text
SCENARIO 01 - Hot partition.
Create topic orders (6 partitions, RF 3). Use kafka-console-producer with parse.key=true and key.separator=: to send 50,000 keyed messages, where about 90% use the key "customer-BIG" and 10% use random keys. Show per-partition end offsets (kafka-get-offsets.sh) to prove the skew. Start a consumer group of 6 console consumers (in the background, with output to /dev/null through a slow reader) and show that one member has huge lag while the others sit idle. Recover by producing with salted keys (customer-BIG-0..9) and show the load evening out. Explain that the trade-off is losing per-key ordering.
```

### 2. Too few partitions / idle consumers
```text
SCENARIO 02 - Consumers exceed partitions.
Create topic payments with 2 partitions. Start 5 consumers in one group. Use kafka-consumer-groups --describe --members --verbose to show that 3 members have no partitions. Then run producer-perf-test at a high rate and show lag growing that more consumers cannot fix. Recover by increasing to 10 partitions and show the rebalance spreading the load.
```

### 3. Adding partitions breaks key ordering
```text
SCENARIO 03 - Partition increase changes the key-to-partition mapping.
Create topic accounts (3 partitions). Produce keys acct-1..acct-20 with sequence numbers, then record which partition each key landed in (consume with print.key=true, print.partition=true). Increase to 6 partitions and produce the same keys again. Show which keys moved partitions, and explain why a consumer could now process event N+1 before event N for the same key.
```

### 4. Leader imbalance
```text
SCENARIO 04 - Leader imbalance.
Create topic inventory (12 partitions, RF 3). Show leaders spread across brokers. Restart kafka-controller-0 and kafka-controller-1 one after the other. Before the automatic rebalance happens (temporarily set auto.leader.rebalance.enable=false through kafka-configs if it's dynamically settable; otherwise explain the alternative), show most leaders sitting on one broker. Run producer-perf-test to show that broker handling most of the load. Recover with kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions.
```

---

## B. Consumer problems

### 5. Consumer lag
```text
SCENARIO 05 - Consumer lag.
Topic clicks (6 partitions). Produce continuously at about 5,000 msg/s with producer-perf-test (--throughput). Run a consumer group whose console consumers are throttled by piping into a slow reader (while read l; do sleep 0.01; done). Print LAG from kafka-consumer-groups every 10s so I can watch it grow. Recover by adding consumers up to the partition count and removing the throttle, then show the lag draining.
```

### 6. Rebalance storm
```text
SCENARIO 06 - Rebalance storm.
Topic events (6 partitions), group storm-group with 4 console consumers. Write a loop that kills and restarts a random consumer every 5-10 seconds. Set session.timeout.ms=6000 and heartbeat.interval.ms=2000. Show the group state flipping between PreparingRebalance/CompletingRebalance/Stable and throughput dropping, as measured by consumer-perf-test or offset progress. Then recover with (a) a stable membership and (b) group.instance.id (static membership), plus partition.assignment.strategy=CooperativeStickyAssignor, and compare rebalance behavior. If the broker supports the new consumer group protocol (group.protocol=consumer, KIP-848), demonstrate that too.
```

### 7. max.poll.interval.ms exceeded
```text
SCENARIO 07 - Slow processing gets the consumer kicked out.
Topic jobs (3 partitions). Run a console consumer with max.poll.interval.ms=10000 and max.poll.records=500, piped into a reader that sleeps 1s per line, so stdout backpressure blocks the poll loop. Show the member leaving the group, the rebalance, and the same records being redelivered. Recover by lowering max.poll.records or raising max.poll.interval.ms, and explain the formula: records × per-record time < max.poll.interval.ms.
```

### 8. Poison pill message
```text
SCENARIO 08 - Poison pill.
Topic metrics (1 partition). Produce 100 valid 4-byte integers with --property value.serializer=org.apache.kafka.common.serialization.IntegerSerializer (or produce raw bytes if that's easier), then one bad record (a long string), then 100 more valid ones. Consume with --value-deserializer org.apache.kafka.common.serialization.IntegerDeserializer in a group and show the consumer crashing at the same offset every time it restarts, with lag stuck. Recover by skipping the bad offset (kafka-consumer-groups --reset-offsets --to-offset N+1 for that partition). Explain dead-letter-queue patterns as the real fix.
```

### 9. Duplicates / offset commit timing
```text
SCENARIO 09 - Duplicate processing after a crash.
Topic ledger (1 partition), 10,000 numbered messages. Consume in group dup-group with enable.auto.commit=true and auto.commit.interval.ms=30000, writing the output to a file. After about 10s, kill -9 the consumer (no clean shutdown, so nothing gets committed). Restart it and show duplicate numbers in the combined output file (sort | uniq -d | wc -l). Explain at-least-once vs at-most-once, and how commit-before-process would instead lose messages.
```

### 10. Offset reset surprise
```text
SCENARIO 10 - auto.offset.reset surprises.
Topic audit (3 partitions) with 1,000 existing messages. Start a NEW group with auto.offset.reset=latest and show it reads nothing. Then show that a group with committed offsets that fall outside retention (set retention.ms very low, wait for deletion) gets reset silently. Show the recovery options: --reset-offsets --to-earliest / --to-datetime / --shift-by, each with --dry-run first.
```

---

## C. Producer problems

### 11. Message loss with acks=1
```text
SCENARIO 11 - Data loss with acks=1.
Topic critical (1 partition, RF 3) with min.insync.replicas=2, and unclean.leader.election.enable=true on the topic. Isolate the follower replicas from the leader (for example, a NetworkPolicy blocking inter-broker traffic to the leader pod) so the ISR shrinks to just the leader. Produce numbered messages with kafka-verifiable-producer.sh --acks 1, then kill the leader pod so an out-of-sync follower is elected. Compare the acked message count with what is actually in the log, and prove the loss. Repeat with acks=all, min.insync.replicas=2 and unclean election disabled, and show that the producer gets NotEnoughReplicas errors instead of losing data.
```

### 12. Record too large
```text
SCENARIO 12 - RecordTooLargeException.
Topic docs with max.message.bytes=100000. Produce a 500KB message (generate the payload file with head -c). Show the error. Then show all three limits that have to line up: topic max.message.bytes, producer max.request.size, and consumer max.partition.fetch.bytes/fetch.max.bytes. Raise all three and succeed. Discuss the alternative: store large blobs elsewhere and send a reference.
```

### 13. Producer buffer exhaustion / timeouts
```text
SCENARIO 13 - Producer blocks and times out.
Topic firehose. Apply a broker-side producer quota to a client.id (kafka-configs --entity-type clients --add-config producer_byte_rate=102400). Run producer-perf-test with that client.id at an unlimited rate, with buffer.memory=1048576, max.block.ms=5000 and delivery.timeout.ms=10000. Show throttling, blocked sends and TimeoutExceptions. Recover by removing the quota. Also explain what produce-throttle-time and record-error-rate indicate.
```

### 14. Batching: throughput vs latency
```text
SCENARIO 14 - Batching tuning.
Using producer-perf-test with the same record count, compare the throughput and latency percentiles for:
(a) linger.ms=0 batch.size=16384
(b) linger.ms=20 batch.size=131072
(c) (b) plus compression.type=lz4
(d) acks=1 vs acks=all.
Output a comparison table and explain the trade-offs.
```

---

## D. Broker and cluster problems

### 15. Under-replicated partitions
```text
SCENARIO 15 - Under-replicated partitions.
Topic replicated (6 partitions, RF 3). Produce continuously. Make one broker's followers fall behind: first by setting a tiny replication throttle (leader/follower.replication.throttled.rate plus the throttled.replicas topic configs), and second by deleting one broker pod. Watch kafka-topics --describe --under-replicated-partitions and the ISR shrinking. Recover and watch the ISR expand again. Explain replica.lag.time.max.ms.
```

### 16. Offline partitions / NotEnoughReplicas
```text
SCENARIO 16 - Losing too many brokers.
Topic A: RF 3, min.insync.replicas=2. Topic B: RF 1. Scale the StatefulSet so 2 of the 3 brokers are down, or block them with a NetworkPolicy. Keep in mind the KRaft controller quorum also needs a majority, so explain what happens to the quorum. Show that topic A rejects acks=all writes (NotEnoughReplicas) and topic B partitions become --unavailable-partitions. Restore the brokers and verify. Explain why RF 1 is dangerous.
```

### 17. Unclean leader election
```text
SCENARIO 17 - Unclean leader election (can be combined with 11).
Show step by step: the ISR shrinks to one broker, writes continue, that broker dies, and an out-of-sync replica comes back. With unclean.leader.election.enable=false, the partition stays offline (availability is lost). With it set to true, the partition comes online but the offsets go backwards (data is lost). Use kafka-get-offsets before and after as proof.
```

### 18. Disk full
```text
SCENARIO 18 - Broker disk full.
The PVCs are 1Gi. Create topic filler (RF 3, retention.ms=-1, retention.bytes=-1) and use producer-perf-test with 100KB records to fill one broker's log dir. Watch df inside the broker pod and kafka-log-dirs.sh. Show what happens (the log dir goes offline or the broker shuts down, and partitions move or become unavailable). Recover by deleting the topic or lowering retention while other brokers are up, and if needed explain PVC expansion. Ask me before deleting any PVC.
```

### 19. Network partition and latency
```text
SCENARIO 19 - Network problems.
(a) Use a NetworkPolicy to isolate kafka-controller-2 from its peers but not from clients, and show ISR changes, possible leader movement, and client errors.
(b) Add latency: check whether tc/netem is available in the pod (NET_ADMIN), or whether Chaos Mesh is installed. If neither is, give me the Chaos Mesh install steps and a NetworkChaos manifest adding 300ms of delay to one broker.
Measure produce latency (producer-perf-test) before and after.
```

### 20. Memory pressure / GC pauses / OOMKilled
```text
SCENARIO 20 - JVM and memory pressure.
Using a Helm upgrade, set one broker to a tiny heap (-Xmx128m) and a tight container memory limit. Put load on it with many partitions (create 500 partitions) plus producer and consumer perf tests. Show GC pauses in the logs (enable GC logging if needed), ISR flapping, leader changes, and possibly OOMKilled in kubectl describe pod. Revert the values afterwards.
```

### 21. Bad rolling restart
```text
SCENARIO 21 - Rolling restart gone wrong.
Check whether the chart created a PodDisruptionBudget. Simulate careless maintenance by deleting 2 broker pods at the same time while producing with acks=all. Show the unavailability. Then do it the right way: restart one pod at a time, and wait for under-replicated partitions to reach 0 before moving on. Write a safe-rolling-restart.sh that enforces this, and add a PDB with maxUnavailable=1.
```

### 22. Client quotas / noisy neighbor
```text
SCENARIO 22 - Noisy neighbor.
Two clients share the cluster: "batch-job" producing flat out, and "api" producing at a low rate while measuring latency. Show api latency getting worse. Then apply quotas to batch-job (producer_byte_rate, consumer_byte_rate, request_percentage) and show api latency recovering.
```

### 23. Too many partitions
```text
SCENARIO 23 - Partition overload.
Create topics until the cluster has about 3,000-5,000 partitions (RF 3). The small lab has limited resources, so scale the number down if broker memory is tight and tell me. Measure: broker restart time, the time for leaders to move after a pod deletion, and memory/file-handle use. Clean up afterwards.
```

---

## E. Data and storage

### 24. Retention deletes unconsumed data
```text
SCENARIO 24 - Consumer slower than retention.
Topic short-lived with retention.ms=60000 and segment.ms=10000. Produce 10,000 messages, keep the consumer group stopped for 3 minutes, then start it. Show the log start offset jumping past the committed offset, the OffsetOutOfRange handling, and the missing messages. Explain how to size retention against the worst-case consumer downtime.
```

### 25. Log compaction and tombstones
```text
SCENARIO 25 - Compaction behavior.
Compacted topic user-profile (cleanup.policy=compact, a small segment.ms, min.cleanable.dirty.ratio=0.01, delete.retention.ms=30000). Produce multiple versions of the same keys, then a tombstone (a null value, using the null.marker property if supported). Show: the latest-value-per-key result after compaction, the tombstone staying visible for delete.retention.ms and then disappearing, and a slow consumer missing the delete. Use kafka-dump-log.sh to look at the segments.
```

### 26. Log segment corruption (advanced)
```text
SCENARIO 26 - Corrupted segment.
On one broker, while it is STOPPED (scale down or use a debug pod mounting its PVC), truncate or overwrite bytes in the tail of the active .log segment for one partition, and delete its .index file. Start the broker again and show the log recovery / truncation messages in the logs, and how replication repairs it from the leader. Ask me before touching any PVC.
```

---

## F. Delivery guarantees and transactions

### 27. Stuck read_committed consumer (open transaction)
```text
SCENARIO 27 - Open transaction blocks read_committed consumers.
Topic tx-topic. Use kafka-producer-perf-test with --transactional-id and a long --transaction-duration-ms (or kill it with -9 mid-transaction). Show that a consumer with isolation.level=read_committed sees nothing past the LSO (last stable offset) while read_uncommitted sees the data. Use kafka-transactions.sh list/describe/find-hanging, and abort if needed. Explain transaction.timeout.ms.
```

### 28. Idempotence and ordering on retries
```text
SCENARIO 28 - Duplicates and reordering from retries.
Run producer-perf-test or verifiable-producer with enable.idempotence=false, retries high and max.in.flight.requests.per.connection=5 while injecting broker disruption (deleting the leader pod mid-run). Check the log for duplicates or out-of-order sequence numbers. Repeat with enable.idempotence=true and compare.
```

---

## G. Operations and Kubernetes-specific

### 29. Topic auto-creation typo
```text
SCENARIO 29 - Accidental topic creation.
With auto.create.topics.enable=true, produce to "ordres" (a typo). Show the new topic created with default settings (and possibly the wrong RF or partition count), with the data "lost" there. Then disable auto-create via a Helm upgrade, and show the producer getting UNKNOWN_TOPIC_OR_PARTITION instead.
```

### 30. Wrong advertised listeners / external access
```text
SCENARIO 30 - Advertised listener misconfiguration.
Explain how Kafka bootstrap works: the client connects once, gets the advertised addresses, and then reconnects to those. Enable an external listener (NodePort) via Helm but deliberately set a wrong advertised host. Show that a client from my Docker host can bootstrap, then times out or can't resolve the broker address. Fix it and verify. Keep this reversible and document the values change.
```

### 31. Authentication / ACL failures (optional, needs SASL)
```text
SCENARIO 31 - Auth and ACL errors.
Upgrade the chart to add a SASL_PLAINTEXT (SCRAM-SHA-512) client listener with an authorizer enabled. Create users app-ok and app-denied. Show: a wrong password (SaslAuthenticationException), a missing WRITE ACL (TopicAuthorizationException), and a missing group READ ACL (GroupAuthorizationException). Grant the ACLs with kafka-acls.sh and verify. Give me a revert path back to PLAINTEXT for the other scenarios.
```

### 32. Controller quorum loss (KRaft)
```text
SCENARIO 32 - KRaft quorum loss.
Use kafka-metadata-quorum.sh describe --status/--replication to show the leader and voters. Take down 1 controller (the quorum survives), then 2 (the quorum is lost). Show that metadata operations (create topic, leader election) fail while existing leaders may keep serving for a while. Restore and verify that the quorum lag is 0.
```

---

## Suggested order

For a steady learning curve: **0 → 5 → 1 → 2 → 7 → 8 → 9 → 15 → 16 → 11/17 → 24 → 12 → 13 → 21 → 19 → 27 → the rest.**

## Notes for a single-node Kubernetes cluster

- Deleting pods and using NetworkPolicy work fine. "Node failure" scenarios become pod failures.
- NetworkPolicy only takes effect if your CNI enforces it (Calico or Cilium do; the default kindnet/flannel may not). Ask Claude Code to check first.
- `tc`/netem needs NET_ADMIN. Chaos Mesh is the cleaner option for latency and packet loss.
