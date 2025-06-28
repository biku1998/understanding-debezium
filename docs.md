# Docs

Sure! Here's a breakdown of those PostgreSQL configuration settings related to **WAL (Write-Ahead Logging)** and **logical replication**:

---

## 🔁 Logical Replication Settings

```sh
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 512MB
```

These settings are typically found in your `postgresql.conf` file and are used to configure PostgreSQL for **logical replication**, which allows changes to specific tables to be streamed to subscribers (often used for data migration, real-time analytics, etc.).

---

### 1. `wal_level = logical`

- **Purpose**: Sets the level of information written to the WAL (Write-Ahead Log).
- **Values**:

  - `minimal`: Smallest amount of logging, not enough for replication or point-in-time recovery.
  - `replica`: Required for streaming replication and point-in-time recovery.
  - `logical`: Required for **logical replication**.

- **Effect**: Enables the database to produce enough information to support logical decoding and replication (e.g., tracking row-level changes).

---

### 2. `max_wal_senders = 10`

- **Purpose**: Specifies the maximum number of concurrent connections (processes) that can stream WAL data to standby or logical replication subscribers.
- **Effect**: Allows up to 10 simultaneous replication connections (logical or physical).
- **Note**: Each logical subscriber (or replica) uses one WAL sender.

---

### 3. `max_replication_slots = 10`

- **Purpose**: Sets the maximum number of replication slots that can be created.
- **Effect**:

  - Each logical replication subscriber uses one replication slot.
  - Slots prevent the removal of WAL files that are still needed by a replica.
  - Logical slots also retain changes until they're consumed, which helps ensure no data is lost if a replica is temporarily offline.

---

### 4. `wal_keep_size = 512MB`

- **Purpose**: Ensures PostgreSQL retains at least this much WAL data on disk, even if it has been archived or streamed already.
- **Effect**: Helps avoid replication lag issues due to missing WAL files, especially for slow or delayed replicas.
- **Unit**: In megabytes.
- **Note**: This is a newer setting (replaces `wal_keep_segments` in recent versions).

---

### Summary

| Setting                      | Purpose                                                             |
| ---------------------------- | ------------------------------------------------------------------- |
| `wal_level = logical`        | Enables row-level WAL changes for logical replication               |
| `max_wal_senders = 10`       | Allows up to 10 replication connections                             |
| `max_replication_slots = 10` | Allows 10 active replication slots for logical/physical replication |
| `wal_keep_size = 512MB`      | Retains 512MB of WAL files to support delayed replicas              |

Let me know if you’d like examples of how this setup is used in practice, like with `pglogical` or change data capture systems.

---

## Distinction between kafka-connect debezium

Great question — and it's an important distinction!

### ❓ Are `kafka-connect` and **Debezium** the same?

**No, they are not exactly the same**, but they are **closely related**.

---

### 🔹 Kafka Connect

- **Kafka Connect** is a **framework** provided by Apache Kafka.
- It allows you to run **connectors** that move data **into** or **out of** Kafka.
- It supports:

  - **Source connectors**: Read data _from_ external systems (e.g., databases, APIs).
  - **Sink connectors**: Write data _to_ external systems (e.g., Elasticsearch, PostgreSQL).

> Think of it as a plugin runner for "data mover" components.

---

### 🔸 Debezium

- **Debezium** is an **open-source CDC (Change Data Capture) toolset**.
- It provides **Kafka Connect-compatible connectors**, specifically **source connectors** for databases like:

  - PostgreSQL
  - MySQL
  - MongoDB
  - SQL Server, etc.

- Debezium connectors are built to **detect row-level changes** and emit those changes as **Kafka events**.

> Debezium is a **vendor of connectors** for Kafka Connect, focused on database change capture.

---

### 💡 Analogy

Think of it like this:

- **Kafka Connect** = An engine that runs connectors
- **Debezium** = A company that makes smart plug-ins (connectors) for that engine, specifically for databases

---

### 📦 In Your Compose File

```yaml
image: debezium/connect:2.4
```

This image:

- Includes the Kafka Connect **runtime**.
- Includes **Debezium’s source connectors** pre-installed (e.g., for PostgreSQL).
- Starts Kafka Connect **with Debezium connectors** ready to be used.

So in practice, your `kafka-connect` service **is** running Kafka Connect **with Debezium plugins** — hence the confusion.

---

### ✅ Summary

| Component     | Role                                  | In Your Setup                 |
| ------------- | ------------------------------------- | ----------------------------- |
| Kafka Connect | Runtime to run connectors             | `kafka-connect` service       |
| Debezium      | Source connectors for databases (CDC) | Used _within_ `kafka-connect` |
| Image used    | `debezium/connect:2.4`                | Combines both                 |

Let me know if you'd like to see an example of a Debezium connector config JSON to register in Kafka Connect.

---

## So is this the service which we are also going to tell which kafka topic to push events to or that is being done elsewhere?

Excellent follow-up — and here’s the key:

> ✅ **Yes — the `kafka-connect` (Debezium) service is where you tell it which Kafka topic to send events to.**
> But **you usually don't specify the topic directly** — Debezium **derives the topic name** based on the connector configuration.

---

### 🔹 How Kafka Topics Are Determined

When registering a Debezium connector, you **do not explicitly set the Kafka topic name**. Instead, it is **automatically generated** based on this config property:

```json
"database.server.name": "ecommerce"
```

Combined with the schema and table, the topic becomes:

```
<database.server.name>.<schema>.<table>
e.g., ecommerce.public.orders
```

So if you're tracking the `orders` table in schema `public`, you get a Kafka topic like:

```
ecommerce.public.orders
```

---

### 🔧 Want to Control the Topic Name?

If you want to customize topic names more directly, you can:

1. **Use a Kafka Connect SMT (Single Message Transform)** to rename topics.
2. **Configure your consumer** to listen to the generated topic.
3. **Write a custom connector (less common)** or use a sink connector with topic remapping.

---

### 🔄 Who _Consumes_ the Topic?

That’s handled by your **`analytics-consumer`** service:

- It listens to topics like `ecommerce.public.orders`
- Transforms and writes data to the `postgres-analytics` database

---

### ✅ Summary

| Task                                               | Done by                              | How                                                      |
| -------------------------------------------------- | ------------------------------------ | -------------------------------------------------------- |
| Decide **which Kafka topic** to send CDC events to | ✅ `kafka-connect` / Debezium        | Auto-generates based on DB/table info                    |
| Publish CDC events to Kafka                        | ✅ Debezium (inside `kafka-connect`) | Connects to Kafka broker                                 |
| Consume Kafka topic                                | ✅ `analytics-consumer`              | Subscribes to topic(s) you specify in the consumer logic |

---

Would you like to see an example of the full connector config that gets posted to `kafka-connect` with topic naming behavior?

---

## And where are we telling this service that where is my kafka running?

Excellent observation — and you're absolutely right to ask.

> ✅ You **tell `kafka-connect` where Kafka is running** using the `BOOTSTRAP_SERVERS` environment variable in the Docker Compose file.

---

### 🔍 In Your `docker-compose.yml`:

Here’s the relevant section under the `kafka-connect` service:

```yaml
environment:
  GROUP_ID: 1
  CONFIG_STORAGE_TOPIC: connect_configs
  OFFSET_STORAGE_TOPIC: connect_offsets
  STATUS_STORAGE_TOPIC: connect_statuses
  BOOTSTRAP_SERVERS: kafka:9092
```

---

### 🔗 Explanation:

- **`BOOTSTRAP_SERVERS: kafka:9092`**

  - Tells `kafka-connect` how to reach the Kafka cluster.
  - `kafka` refers to the **hostname of the Kafka container** (as defined in the Compose file).
  - `9092` is the Kafka **internal port** (as defined under the Kafka service).

So, inside the Docker network created by Compose, `kafka:9092` is a valid internal address.

---

### 🧠 Why is this needed?

Because:

- Kafka Connect (`kafka-connect`) is a **Kafka client**, like a producer or consumer.
- It needs to **publish internal config topics** (e.g., `connect_configs`) and **push Debezium CDC events** to Kafka.

---

### ✅ Summary

| Purpose               | Config Key             | Value in Compose                                    |
| --------------------- | ---------------------- | --------------------------------------------------- |
| Where is Kafka?       | `BOOTSTRAP_SERVERS`    | `kafka:9092`                                        |
| How is that resolved? | Docker Compose network | Container name `kafka` resolves to the Kafka broker |

---

Let me know if you want to simulate registering the connector via `curl` or Postman using this setup!
