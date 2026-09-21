# 01 · Hot partition (key skew)

**What you'll learn:** one very busy key sends almost all traffic to one partition. One consumer
drowns while the others sit idle, and adding consumers doesn't help.

**Time:** about 15 minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 01`.

---

## Part 1 · Normal: an order pipeline that works

A shop sends ~180 orders/s from many different customers. The key is the customer id. Six consumers
process them, each spending ~10 ms per order (so at most ~95 orders/s each).

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic orders --partitions 6 --replication-factor 3
```
✅ **Expected:** `Created topic orders.`

### Step 2 · Start the shop (the order producer)
```bash
nohup bash /apps/order-producer.sh normal >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start 6 consumers
```bash
for i in 1 2 3 4 5 6; do nohup bash /apps/slow-consumer.sh orders orders-group c$i 0.01 >/dev/null 2>&1 & done
```
✅ **Expected:** six job lines, `[2]` … `[7]`.

### Step 4 · Check that new orders spread over all partitions
```bash
bash /apps/new-per-partition.sh orders
```
✅ **Expected** (after 10 s): every partition gets about the same share, ~17% each.
```
partition 0       302 new   16.8%  #######
partition 1       297 new   16.5%  #######
...
partition 5       305 new   16.9%  #######
```

### Step 5 · Check that every consumer keeps up
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group orders-group
```
✅ **Expected:** 6 rows, one per partition, with each `CLIENT-ID` (`c1` … `c6`) once. `LAG` is small
(below ~100) on every partition.

---

## Part 2 · Break: a whale customer arrives

One huge customer, `customer-BIG`, now places ~90% of all orders. The total is still ~180/s.

### Step 6 · Switch the shop to whale traffic
```bash
pkill -f order-producer.sh; nohup bash /apps/order-producer.sh whale >/dev/null 2>&1 &
```
✅ **Expected:** a new job line. The old producer ends (you may see `Terminated`). **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · Where do new orders go now?
```bash
bash /apps/new-per-partition.sh orders
```
✅ **Expected:** one partition gets ~90% of all new orders (in this lab it's partition 5), and the rest get 1–2% each.
```
partition 0        28 new    1.6%  #
...
partition 5      1650 new   91.7%  #####################################
```

### Step 8 · Who is falling behind?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group orders-group
```
✅ **Expected:** partition 5 has a `LAG` of thousands. Run the command again after 10 s: it has grown
by ~700. The other five partitions stay near 0, so their consumers are idle.

### Step 9 · Which key fills the hot partition?
```bash
end=$(kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic orders --partitions 5 | cut -d: -f3); kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic orders --partition 5 --offset $((end - 500)) --max-messages 500 --property print.key=true 2>/dev/null | cut -f1 | sort | uniq -c | sort -rn | head -3
```
✅ **Expected:** almost all of the last 500 orders there have the key `customer-BIG`:
```
    460 customer-BIG
      1 customer-48213
      1 customer-09177
```

### Step 10 · Would more consumers help?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group orders-group --members
```
✅ **Expected:** 6 members, each with `#PARTITIONS 1`. There is already one consumer per partition.
A 7th consumer would get `#PARTITIONS 0`: a partition is read by exactly one member of a group.

---

## Part 4 · Fix: salt the hot key

The whale's orders get a suffix, `customer-BIG-0` … `customer-BIG-9`, so they spread over several
partitions. In a real system this is a producer code change, e.g. `key = customerId + "-" + (orderId % 10)`.

### Step 11 · Switch the shop to salted keys
```bash
pkill -f order-producer.sh; nohup bash /apps/order-producer.sh salted >/dev/null 2>&1 &
```
✅ **Expected:** a new job line.

### Step 12 · Check where new orders go now
```bash
bash /apps/new-per-partition.sh orders
```
✅ **Expected:** every partition gets orders again, and the busiest one gets ~40% instead of ~90%. It's
not perfectly even, because 10 salts land on 6 partitions unevenly.

---

## Part 5 · Back to normal

### Step 13 · Watch the old backlog drain
```bash
bash /apps/lag-watch.sh orders-group 6
```
✅ **Expected:** `draining by ~70 msg/s` on every line until the total lag is small and `flat`. Run it
again if the backlog was large, because it can take a few minutes.

### Step 14 · Check that every consumer keeps up again
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group orders-group
```
✅ **Expected:** `LAG` is small on all 6 partitions, as in step 5.

---

## Part 6 · Clean up

### Step 15 · Stop the shop and the consumers
```bash
pkill -f "topic orders"; sleep 5
```
✅ **Expected:** several `Terminated` lines.

### Step 16 · Delete the consumer group and the topic
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group orders-group
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic orders
```
✅ **Expected:** `Deletion of requested consumer groups ('orders-group') was successful.` The topic
delete prints nothing.

---

## Why it happened, and how to prevent it

- **Why:** the producer picks the partition with `murmur2(key) % partitions`, so one key always goes to
  one partition. That's how Kafka keeps per-key order. A partition is read by exactly one consumer in a
  group, so one hot key means one overloaded consumer.
- **The trade-off of salting:** the whale's orders are no longer kept in order relative to each other.
  Only salt keys whose events are order-independent, or re-sequence them downstream.
- **Other fixes:**
  - a finer key when order is only needed at that level (`customer/order-id`);
  - a separate topic for the whale;
  - 2–3× more salts than partitions, for an even spread.
- **Spot it early:** watch **per-partition** message rates and lag. A topic-wide average hides a hot partition.
