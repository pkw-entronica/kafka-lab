# 30 · Wrong advertised listeners

**What you'll learn:** how Kafka's bootstrap really works — the client connects once, is told where the
brokers *say* they are, and then reconnects to those addresses — and why a wrong `advertised.listeners`
gives you the most confusing failure in Kafka: "it connects, then times out".

**Time:** about 25 minutes (two Helm upgrades).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact messages
> will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 30` (it also
  removes the external listener again).

---

## Part 1 · Normal: how a client finds the brokers

Connecting to Kafka is always two steps:
1. The client connects to a **bootstrap** address and asks for metadata.
2. The brokers answer with the address **each broker advertises** for that listener, plus which broker
   leads which partition. The client then opens connections to *those* addresses and forgets the
   bootstrap address.

So the bootstrap address only has to work once; the advertised addresses have to work for everything.

### Step 1 · Create a topic to test with
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic external-test --partitions 3 --replication-factor 3
```
✅ **Expected:** `Created topic external-test.`

### Step 2 · What do the brokers advertise today?
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep advertised.listeners
```
✅ **Expected:** something like
`advertised.listeners=CLIENT://kafka-controller-0.kafka-controller-headless.kafka-lab.svc.cluster.local:9092,INTERNAL://…`
— DNS names that only resolve **inside** the cluster. That's why `$BOOTSTRAP` works from this pod.

### Step 3 · Writing works from inside the cluster
```bash
bash /apps/produce-check.sh external-test 3 all
```
✅ **Expected:** `3 accepted, 0 rejected (acks=-1)`.

---

## Part 2 · Break: expose the cluster, advertise the wrong host

The team wants to reach Kafka from outside Kubernetes, so they enable a NodePort listener. The
advertised host is filled in from a template, and it's wrong.

### Step 4 · PowerShell: apply the external listener
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/30-advertised-listeners/values-external-nodeport.yaml --wait --timeout 10m
```
✅ **Expected:** after ~2–3 minutes, `Release "kafka" has been upgraded. Happy Helming!`

### Step 5 · PowerShell: what got created, and the node's address
```powershell
kubectl -n kafka-lab get svc | Select-String -Pattern "external"
kubectl get node kind-control-plane -o jsonpath="{.status.addresses[?(@.type=='InternalIP')].address}{'\n'}"
```
✅ **Expected:** three NodePort services (`kafka-controller-0-external`, `-1-external`, `-2-external`)
on ports `30094`, `30095`, `30096`, and a node address like `172.18.0.2`. **Write the address down** —
the next steps call it `$NODE`.

---

## Part 3 · Observe: bootstrap works, everything else doesn't

### Step 6 · Set the address and check the port is reachable
```bash
NODE=172.18.0.2; timeout 3 bash -c "</dev/tcp/$NODE/30094" && echo "TCP connect to $NODE:30094 works"
```
✅ **Expected:** `TCP connect to 172.18.0.2:30094 works` (use **your** address from step 5). The
bootstrap step will succeed — the network path is fine.

### Step 7 · Now try to actually use it
```bash
echo hello | kafka-console-producer.sh --bootstrap-server $NODE:30094 --topic external-test --producer-property max.block.ms=15000
```
✅ **Expected:** warnings about not being able to connect or resolve
`kafka-external.invalid` (an `UnknownHostException`), and then
`org.apache.kafka.common.errors.TimeoutException: Topic external-test not present in metadata after 15000 ms.`
The client reached a broker, got the metadata, and was told to go to a host that doesn't exist.

### Step 8 · The smoking gun
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep advertised.listeners
```
✅ **Expected:** the same line as in step 2 plus
`EXTERNAL://kafka-external.invalid:30094`. That string is what every external client is told to
connect to — the bootstrap address it used is irrelevant from that point on.

### Step 9 · Inside the cluster nothing changed
```bash
bash /apps/produce-check.sh external-test 3 all
```
✅ **Expected:** `3 accepted, 0 rejected (acks=-1)`. Each listener advertises its own address, so the
internal clients never noticed. That's why this bug typically only breaks "the new team".

---

## Part 4 · Fix: advertise an address the client can reach

### Step 10 · PowerShell: set the advertised host to the node's address
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/30-advertised-listeners/values-external-nodeport.yaml --set externalAccess.controller.service.domain=172.18.0.2 --wait --timeout 10m
```
✅ **Expected:** `Release "kafka" has been upgraded.` again after ~2–3 minutes. Use **your** node
address from step 5.

### Step 11 · Check what is advertised now
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name 0 --describe --all | grep advertised.listeners
```
✅ **Expected:** `EXTERNAL://172.18.0.2:30094` instead of the invalid name.

---

## Part 5 · Back to normal

### Step 12 · Write through the external listener
```bash
NODE=172.18.0.2; echo hello-from-outside | kafka-console-producer.sh --bootstrap-server $NODE:30094 --topic external-test --producer-property max.block.ms=15000
```
✅ **Expected:** no output — the write worked.

### Step 13 · Read it back through the internal one
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic external-test --from-beginning --timeout-ms 10000 2>/dev/null | grep hello-from-outside
```
✅ **Expected:** `hello-from-outside`. Same cluster, same data, two different doors.

---

## Part 6 · Clean up

### Step 14 · PowerShell: remove the external listener
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml --wait --timeout 10m
```
✅ **Expected:** `Release "kafka" has been upgraded.` and `kubectl -n kafka-lab get svc` no longer lists
the `*-external` services.

### Step 15 · Delete the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic external-test
```
✅ **Expected:** no output.

---

## Why it happened, and how to prevent it

- **`advertised.listeners` is the address other machines are told to use.** `listeners` is where the
  broker binds; `advertised.listeners` is what it publishes. They are different on purpose: inside
  Kubernetes a broker binds `0.0.0.0:9092` and advertises its pod DNS name; for external clients it
  advertises a node address, a load balancer, or a public DNS name.
- **Each listener has its own advertised address**, which is how one cluster serves internal clients,
  external clients and inter-broker traffic at the same time — and why breaking one of them leaves the
  others perfectly healthy.
- **The signature of this bug:** the client connects to the bootstrap address without complaining, then
  times out with "Topic … not present in metadata", or logs `UnknownHostException` /
  "Connection to node 1 could not be established" for an address you didn't type anywhere. The fix is
  never in the bootstrap string — always check what the brokers advertise.
- **How to check it quickly:**
  - `kafka-broker-api-versions.sh --bootstrap-server <addr>` prints one block per broker and fails
    per broker when the advertised address is unreachable;
  - `kafka-configs.sh --entity-type brokers --entity-name <id> --describe --all | grep advertised`;
  - from the client's machine, resolve and connect to the advertised host:port by hand.
- **Prevent it:** one advertised address per network the clients live in, DNS names rather than IPs
  where addresses can change, and a smoke test from **outside** the cluster in CI after every change to
  the listener configuration. In Kubernetes, per-pod NodePort or LoadBalancer services (as here) are
  needed because every broker must be individually addressable — a single Service in front of all
  brokers can't work for the client protocol.
