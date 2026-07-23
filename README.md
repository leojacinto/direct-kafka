# Stream Connect for Direct Kafka — CMDB CI Sync (Confluent Cloud)

Config runbook for wiring ServiceNow Direct Kafka to a Confluent Cloud cluster for `cmdb_ci` sync. Native Kafka protocol (no MID, no REST proxy).

- **You'll need:** a Confluent Cloud cluster (bootstrap server + cluster id), a Kafka API key/secret, and admin on the ServiceNow instance. You capture the cluster values during setup below.
- **Scope:** create the ServiceNow records in a dedicated app scope (e.g. `sc_direct_kafka`).
- **Topics:** `cmdb.ci.inbound` (Kafka → CI), `cmdb.ci.outbound` (CI → Kafka)

---

## Data flow — who produces, who consumes

| Topic | Produced by (source of content) | Consumed by (destination) |
|---|---|---|
| `cmdb.ci.inbound` | **external** source system (writes CI data to the topic) | **ServiceNow** → upserts `cmdb_ci` |
| `cmdb.ci.outbound` | **ServiceNow** (`cmdb_ci` changes) | external subscribers |

```
INBOUND (built):   source system ──produce──▶ Confluent[cmdb.ci.inbound] ──ServiceNow consumes──▶ cmdb_ci
OUTBOUND (next):   cmdb_ci ──ServiceNow produces──▶ Confluent[cmdb.ci.outbound] ──consume──▶ external subscribers
```

For the **inbound** flow, content originates **outside** ServiceNow — ServiceNow only reads it off Kafka and writes `cmdb_ci`. It does not create inbound content. (In testing, a sample producer stands in for the external source system.)

---

## Prerequisites

1. **All 9 Stream Connect gate plugins active** (`stream_connect.direct_kafka`, `action_step.kafka`, `etl_consumer.kafka`, `flow_trigger.kafka`, `kafka_consumer`, `stream_connect.alerting`, `stream_connect.common.core`, `stream_connect.schema`, `stream_connect.kafka_connection`). Activate the master installer `com.glide.hub.stream_connect.onprem_installer`, then **verify each of the 9 with `GlidePluginManager.isActive()` and activate any that didn't come up** — the installer does not reliably bring them all active (`flow_trigger.kafka` required a separate activation).
2. **Roles on the operating user:** `stream_connect_admin`, `stream_connect_api`, `kafka_admin`.
3. **Confluent Cloud side** — topics + ACLs configured (see next section).

---

## Confluent Cloud side (cluster, API key, topics, ACLs)

### 1. Provision a cluster

1. Sign in at [confluent.cloud](https://confluent.cloud) (create an account if new). A default **environment** is created for you.
2. **Add cluster** → pick a type: **Basic** (single-zone — fine for dev/eval), **Standard** (multi-zone, production), **Enterprise**, or **Dedicated**. Click **Begin configuration**.
3. Choose **cloud provider** (AWS / Azure / Google Cloud) and **region**, select a single availability zone, **Continue**.
4. Name the cluster → **Launch cluster** (provisioning takes a few minutes).
5. On **Cluster Overview**, note the **bootstrap server** (`<host>:9092`) and **cluster id** (`lkc-…`) — you'll use the bootstrap server for the ServiceNow Kafka Connection (step 2 of the config chain).

### 2. Create an API key

1. In the cluster: **Cluster Overview → API keys → + Add key**.
2. Scope: **Granular access** → **Next**.
3. Owner: **Create a new one** (a new service account — recommended for integrations) or **Use existing account**.
4. Confluent shows the **key** and **secret** — the **secret is displayed once**; download/copy both. Key = username, secret = password for the ServiceNow credential (step 1 of the config chain).

> Match the cluster's auth to the ServiceNow credential: for a Confluent Cloud API key that's `SASL_SSL` / `PLAIN` with key=username, secret=password (see ServiceNow config chain, step 1).

### 3. Topics + ACLs

**Topics**: `cmdb.ci.inbound`, `cmdb.ci.outbound` — 3 partitions each. (Confluent Cloud fixes replication factor at 3; it's not a setting you choose.)

**ACLs for the API-key principal** (all `ALLOW`):

| Resource | Operations | Pattern |
|---|---|---|
| Cluster | `DESCRIBE`, `DESCRIBE_CONFIGS`, `CREATE`, `ALTER`, `CLUSTER_ACTION`, `IDEMPOTENT_WRITE` | — |
| Topic (each) | `DESCRIBE`, `READ`, `WRITE` | LITERAL **and** PREFIXED |
| Consumer group | `READ` | group = `sys_kafka_stream:<stream_sys_id>` |

- **Topic-level ACLs are a separate scope from cluster-level — both are required** (cluster ACLs alone won't list/read a topic → `403 Topic authorization failed`).
- `READ` on the topic + `READ` on the consumer group are the minimum for the inbound consumer; `CREATE`/`ALTER`/`CLUSTER_ACTION`/`IDEMPOTENT_WRITE` are only needed to create topics / produce from this principal.

---

## External producer — source system → `cmdb.ci.inbound`

**This is what starts the inbound flow.** ServiceNow's consumer sits idle and polls; a message *arriving on the topic* is the trigger. That message is produced by an **external** system (the source of CI data), **not** by ServiceNow. So the producing side must be configured to publish to Kafka. What it needs:

- **Broker / auth:** bootstrap server `<host>:9092`, `SASL_SSL` / `PLAIN`.
- **Credentials:** a Confluent API key/secret whose principal has **`WRITE`** on `cmdb.ci.inbound` (same service account is fine, or a separate one).
- **Topic:** `cmdb.ci.inbound`.
- **Value:** a JSON string (the consumer reads it as text and `JSON.parse`s it):

```json
{ "correlation_id": "CI-0001", "name": "web-server-01", "sys_class_name": "cmdb_ci_server", "operational_status": "1", "short_description": "..." }
```

  `correlation_id` is the only required field (it's the upsert key); the rest are mapped if present.

The **topic** and the **message value contract** above are fixed by the ServiceNow consumer. Everything else on the producing side is **not** dictated here:

- **Key / partitioning, publish cadence (e.g. on CI create/update), and the source's own auth model** are decided by whoever owns the source system and should be confirmed with that team — this runbook doesn't cover the producing application's internal config. Treat any specifics here as suggested defaults (standard Kafka practice), not requirements.

Any Kafka producer can feed the topic — the source app's native client, `kafka-console-producer`, or the Confluent Cloud console's produce-message feature for hand-testing. Once a message lands, ServiceNow consumes it automatically (below).

---

## ServiceNow config chain

Build in this order (each references the previous):

| # | Record | Table | Must-set fields |
|---|---|---|---|
| 1 | Kafka Credential | `kafka_credentials` | `security_protocol=SASL_SSL`, `sasl_mechanism=PLAIN`, `user_name`/`password` = Confluent API key/secret |
| 2 | Kafka Connection | `kafka_connection` (ext. `sys_connection`) | `host` = the cluster's bootstrap server, i.e. `<host>:9092` in one field (see watchout), `use_mid=false`, `credential` → #1 |
| 3 | Connection & Credential Alias | `sys_alias` | `connection_type=kafka_connection` (see watchout) |
| 4 | Direct Kafka Cluster | `sys_kafka_direct_cluster` | `name`, `external_kafka` → #3 alias |
| 5 | Test Connection | (UI action on #4) | on success, topics auto-sync into `sys_kafka_direct_topic` |

---

## Inbound consumer: `cmdb.ci.inbound` → `cmdb_ci` ✅

Native Script Consumer, verified end-to-end (insert + idempotent update by `correlation_id`).

**Build:**
1. **Topic Alias** — `sys_sc_topic_alias` `name=cmdb-ci-inbound-alias`. Set `sys_kafka_direct_topic.sc_topic_alias` on the inbound topic to this alias.
2. **Script Consumer** — `sys_kafka_script_consumer` (extends `sys_kafka_etl_consumer`): `name=CMDB CI Inbound`, `serialization_format=text`, `delivery_guarantee=at_least_once`. Paste the contents of [`consumer_cmdb_ci_inbound.js`](consumer_cmdb_ci_inbound.js) into the **`event_consumer`** field. That file is **the code ServiceNow runs on each consumed message** — it parses the JSON and upserts `cmdb_ci`. It is *not* a producer and does not create messages.
3. **Kafka Stream** — `sys_kafka_stream`: `kafka_etl_consumer` → #2, `sc_topic_alias` → #1, `initial_offset=earliest`, `message_handling=dynamic`, `run_as=<user>`.
4. **Activate** — "Activate" UI action on the stream, or `new sn_ih_kafka.ETLConsumerSubscription().activate(streamGR)`. Creates a `sys_kafka_subscription` (state `ACTIVE`) + partition group that polls the broker.

The consumer reads the message published by the [external producer](#external-producer--source-system--cmdbciinbound) and upserts by `correlation_id` — re-sending the same id updates that CI in place (no duplicate).

**Verify:** `sys_kafka_subscription` is `ACTIVE` / `has_error=0`; produce a test message to `cmdb.ci.inbound` (see External producer above); confirm the `cmdb_ci` row (matched by `correlation_id`) was created/updated.

---

## Outbound producer: `cmdb_ci` → `cmdb.ci.outbound`

Next phase — being finalized, will be documented here on completion.

---

## Watchouts

- **`kafka_connection.host` must be `host:port` in one field** (e.g. `<host>:9092`). A separate `port` field is ignored when building `bootstrap.servers` → `Invalid url in bootstrap.servers`.
- **`sys_alias.connection_type` must = `kafka_connection`** or the `Verify Connection and Alias Match` business rule blocks creation with 403.
- **Credential goes in `user_name`/`password`**, not `additional_properties`.
- **Scope is set by session state** (`sys_user_preference` `apps.current_app`), not by any field in the request — Table API inserts silently inherit the acting user's current app. Set/verify scope before creating, and re-`GET` each record's scope after creating.
- **`sys_kafka_direct_topic` is not manually creatable** — it auto-populates after a successful Test Connection.
- **Direct Kafka requires `use_mid=false`** (MID not supported for this connection type; enforced by BR).
- **A Topic Alias links in one place only** (`Prevent multiple topics on StreamConnect`): either on `sys_kafka_direct_topic.sc_topic_alias` or the m2m table, not both.
- **Consumer stream binds via `sc_topic_alias`** (the Direct Kafka path), not the Hermes `topic` field.
- **Test data currently on instance:** `cmdb_ci` rows `correlation_id` = `KAFKA-INBOUND-001`, `KAFKA-INBOUND-002` (from consumer verification — delete when done).

---

## References

- [Stream Connect for Direct Kafka — ServiceNow product docs](https://www.servicenow.com/docs/r/integrate-applications/integration-hub/direct-kafka.html)
- [Stream Connect for Direct Kafka: one cluster, zero middleware — Community article](https://www.servicenow.com/community/workflow-data-fabric-articles/stream-connect-for-direct-kafka-one-cluster-zero-middleware-full/ta-p/3531895)
