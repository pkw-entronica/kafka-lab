# 31 · Authentication and ACL errors

**What you'll learn:** the three failures every team hits on their first secured cluster — bad
credentials, a missing topic ACL, and a missing **group** ACL — what each one looks like, and how to
grant exactly what an application needs with `kafka-acls.sh`.

**Time:** about 35 minutes (two Helm upgrades).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact messages
> will differ.

> ⚠️ **The most invasive scenario in this lab.** While it runs, every client needs credentials, so
> kafka-ui and the lab's own scripts (`lab-status.sh`, `smoke-test.sh`, `cleanup.sh` for *other*
> scenarios) can't talk to Kafka. Finish Part 6, or run `wsl -d Ubuntu -- bash cleanup.sh 31`, before
> you do anything else in the lab.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Passwords here are lab passwords in plain text. In a real cluster they belong in a Secret, and the
  listener should be `SASL_SSL`, not `SASL_PLAINTEXT`.

---

## Part 1 · Normal: an open cluster, and the users we'll need

Right now the client listener is `PLAINTEXT`: no authentication, no authorization, anyone who can reach
the port can do anything. We create the SCRAM credentials **first**, while that's still true.

### Step 1 · Create the topic the application will use
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic secure-orders --partitions 3 --replication-factor 3
```
✅ **Expected:** `Created topic secure-orders.`

### Step 2 · Create three SCRAM-SHA-512 users
```bash
for u in admin app-ok app-denied; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type users --entity-name $u --add-config "SCRAM-SHA-512=[password=$u-secret]"; done
```
✅ **Expected:** three times `Completed updating config for user $u.` The credentials are stored in the
cluster metadata, so they survive restarts.

### Step 3 · Check they exist
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type users
```
✅ **Expected:** a `SCRAM-SHA-512=salt=…` line for `admin`, `app-ok` and `app-denied`. The passwords
themselves are not stored, only the SCRAM salt and verifier.

---

## Part 2 · Break: security is switched on

### Step 4 · PowerShell: require SASL on the client listener and enable the authorizer
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml -f scenarios/31-sasl-acls/values-sasl-acl.yaml --wait --timeout 10m
```
✅ **Expected:** after ~2–3 minutes, `Release "kafka" has been upgraded. Happy Helming!` The brokers
restart one at a time. From now on the client listener is `SASL_PLAINTEXT` with SCRAM-SHA-512, and
`allowEveryoneIfNoAclFound=false` means: **no ACL, no access**.

### Step 5 · The old way of connecting stops working
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list
```
✅ **Expected:** it hangs for a while and fails — typically
`org.apache.kafka.common.errors.TimeoutException: Call(callName=listTopics…) timed out` — because the
client speaks plain Kafka protocol to a listener that now expects a SASL handshake. Press `Ctrl+C` if
it takes too long.

---

## Part 3 · Observe: three different failures

### Step 6 · Write client config files for each user
```bash
for u in admin app-ok app-denied; do printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s-secret";\n' "$u" "$u" > /tmp/s31-$u.properties; done
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="app-ok" password="wrong-password";\n' > /tmp/s31-wrong.properties
ls /tmp/s31-*.properties
```
✅ **Expected:** four files. Every Kafka CLI tool takes one of them: `--command-config` for admin tools,
`--producer.config` for the producer, `--consumer.config` for the consumer.

### Step 7 · The admin (a super user) can work
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --list
```
✅ **Expected:** the topic list, including `secure-orders`. `admin` is in `superUsers`, so the
authorizer lets it do anything — that's how you administer a cluster before any ACL exists.

### Step 8 · Failure 1: the wrong password
```bash
echo test | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --producer.config /tmp/s31-wrong.properties
```
✅ **Expected:**
`org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`.
The connection is refused before any topic is involved — this is **authentication**, step one.

### Step 9 · Failure 2: the right password, no permission
```bash
echo test | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --producer.config /tmp/s31-app-ok.properties
```
✅ **Expected:**
`org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [secure-orders]`.
The user is who they say they are, but nothing grants them `WRITE` on this topic — **authorization**,
step two. `app-denied` gets exactly the same error, which is the point of the next parts.

### Step 10 · Failure 3: allowed to read the topic, but not to use a group
Grant only the topic side first:
```bash
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --add --allow-principal User:app-ok --operation READ --topic secure-orders
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --group app-group --from-beginning --timeout-ms 15000 --consumer.config /tmp/s31-app-ok.properties
```
✅ **Expected:** `Adding ACLs for resource …` and then
`org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: app-group`.
A consumer needs **two** permissions: `READ` on the topic **and** `READ` on the consumer group. This one
catches almost everybody.

---

## Part 4 · Fix: grant exactly what the application needs

### Step 11 · Let app-ok write to the topic
```bash
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --add --allow-principal User:app-ok --operation WRITE --operation DESCRIBE --topic secure-orders
```
✅ **Expected:** `Adding ACLs for resource ResourcePattern(resourceType=TOPIC, name=secure-orders, …)`
followed by the resulting ACL list.

### Step 12 · Let app-ok use its consumer group
```bash
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --add --allow-principal User:app-ok --operation READ --group app-group
```
✅ **Expected:** the same kind of output for `ResourceType=GROUP, name=app-group`.

### Step 13 · Look at what is now allowed
```bash
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --list
```
✅ **Expected:** three entries for `User:app-ok` — `READ` and `WRITE`/`DESCRIBE` on topic
`secure-orders`, and `READ` on group `app-group`. Nothing at all for `app-denied`.

---

## Part 5 · Back to normal

### Step 14 · app-ok can produce
```bash
echo "order-1" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --producer.config /tmp/s31-app-ok.properties
```
✅ **Expected:** no output — the write worked.

### Step 15 · app-ok can consume with its group
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --group app-group --from-beginning --timeout-ms 15000 --consumer.config /tmp/s31-app-ok.properties
```
✅ **Expected:** `order-1` and `Processed a total of 1 messages`.

### Step 16 · app-denied still can't
```bash
echo "order-2" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic secure-orders --producer.config /tmp/s31-app-denied.properties
```
✅ **Expected:** `TopicAuthorizationException: Not authorized to access topics: [secure-orders]` again.
Each user gets only what its own ACLs allow.

---

## Part 6 · Clean up (do this before any other scenario)

### Step 17 · Remove the ACLs while the authorizer is still on
```bash
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --remove --allow-principal User:app-ok --operation READ --operation WRITE --operation DESCRIBE --topic secure-orders --force
kafka-acls.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --remove --allow-principal User:app-ok --operation READ --group app-group --force
```
✅ **Expected:** `Removing ACLs for resource …` for both. Once the authorizer is switched off you can no
longer manage ACLs, so this has to happen first.

### Step 18 · Delete the topic and the SCRAM users
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --delete --topic secure-orders
for u in app-ok app-denied admin; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/s31-admin.properties --alter --entity-type users --entity-name $u --delete-config SCRAM-SHA-512; done
rm -f /tmp/s31-*
```
✅ **Expected:** three times `Completed updating config for user …`. Delete `admin` last — it's the
account you're using.

### Step 19 · PowerShell: back to PLAINTEXT for the rest of the lab
```powershell
helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 -n kafka-lab -f lab/values.yaml --wait --timeout 10m
```
✅ **Expected:** `Release "kafka" has been upgraded.` after ~2–3 minutes.

### Step 20 · Everything works without credentials again
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list
```
✅ **Expected:** the topic list, no config file needed. kafka-ui and the lab scripts work again too.

---

## Why it happened, and how to prevent it

- **Two separate gates.** *Authentication* answers "who are you?" (SASL/SCRAM, mTLS, OAuth) and fails
  with `SaslAuthenticationException`. *Authorization* answers "may you do this?" (the authorizer plus
  ACLs) and fails with `TopicAuthorizationException`, `GroupAuthorizationException` or
  `ClusterAuthorizationException`. Reading the exception name tells you which gate closed.
- **What a normal application actually needs:**
  | Client | ACLs |
  |---|---|
  | Producer | `WRITE` (+ `DESCRIBE`) on the topic |
  | Consumer | `READ` on the topic **and** `READ` on the group |
  | Transactional producer | the above plus `WRITE`/`DESCRIBE` on the `transactional.id` |
  | Admin tooling | `DESCRIBE`/`ALTER` on the cluster or the specific resources |
  Prefixed ACLs (`--resource-pattern-type prefixed --topic orders-`) keep this manageable, and
  `--allow-principal` can name a group of services through a shared user.
- **`allowEveryoneIfNoAclFound`** decides what happens to a resource with no ACLs: `true` means "open
  unless denied" (easy to roll out, easy to forget a topic), `false` means "closed unless allowed" —
  the safe default this scenario uses. Roll it out in that order: authenticate first, add ACLs while
  everything is still open, then flip this to `false`.
- **Super users bypass everything**, which is why `User:ANONYMOUS` had to be a super user here: the
  inter-broker and controller listeners stayed `PLAINTEXT`, so the brokers authenticate as nobody. In
  production, secure those listeners too and never leave `ANONYMOUS` in `superUsers`.
- **Use `SASL_SSL`, not `SASL_PLAINTEXT`.** SCRAM over a plaintext connection protects the password
  from being read directly, but everything else — including the messages — is in the clear.
- **What to watch:** authorization failures per principal (they appear in the broker's
  `kafka-authorizer.log`), failed authentication counts, and any ACL wildcards that quietly grant more
  than intended.
