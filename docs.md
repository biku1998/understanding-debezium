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

## Datasets for blog prep

- https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_geolocation_dataset.csv
- https://www.kaggle.com/datasets/bytadit/ecommerce-order-dataset/data

```py

[
    "olist_geolocation_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_order_payments_dataset.csv",
    "product_category_name_translation.csv"
]
```

````md
I've some datasets
[
"olist_geolocation_dataset.csv",
"olist_customers_dataset.csv",
"olist_sellers_dataset.csv",
"olist_products_dataset.csv",
"olist_orders_dataset.csv",
"olist_order_items_dataset.csv",
"olist_order_reviews_dataset.csv",
"olist_order_payments_dataset.csv",
"product_category_name_translation.csv"
]

And I want to create schema and ingest them in a postgres database.

I'll give you a few entries from all the files.

1. olist_geolocation_dataset.csv

```csv
"geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"
"01037",-23.54562128115268,-46.63929204800168,sao paulo,SP
"01046",-23.546081127035535,-46.64482029837157,sao paulo,SP
```
````

2. olist_customers_dataset.csv

```csv
"customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"
"06b8999e2fba1a1fbc88172c00ba8bc7","861eff4711a542e4b93843c6dd7febb0","14409",franca,SP
"18955e83d337fd6b2def6b18a428ac77","290c77bc529b7ac935b93aa66c333dc3","09790",sao bernardo do campo,SP
```

3. olist_sellers_dataset.csv

```csv
"seller_id","seller_zip_code_prefix","seller_city","seller_state"
"3442f8959a84dea7ee197c632cb2df15","13023",campinas,SP
d1b65fc7debc3361ea86b5f14c68d2e2,"13844",mogi guacu,SP
```

4. olist_products_dataset.csv

```csv
"product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"
"1e9e8ef04dbcff4541ed26657ea517e5",perfumaria,40,287,1,225,16,10,14
"3aa071139cb16b67ca9e5dea641aaa2f",artes,44,276,1,1000,30,18,20
```

5. olist_orders_dataset.csv

```csv
"order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
e481f51cbdc54678b7cc49136f2d6af7,"9ef432eb6251297304e76186b10a928d",delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00
"53cdb2fc8bc7dce0b6741e2150273451",b0830fb4747a6c6d20dea0b8c802d7ef,delivered,2018-07-24 20:41:37,2018-07-26 03:24:27,2018-07-26 14:31:00,2018-08-07 15:27:45,2018-08-13 00:00:00
```

6. olist_order_items_dataset.csv

```csv
"order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"
"00010242fe8c5a6d1ba2dd792cb16214",1,"4244733e06e7ecb4970a6e2683c13e61","48436dade18ac8b2bce089ec2a041202",2017-09-19 09:45:35,58.90,13.29
"00018f77f2f0320c557190d7a144bdd3",1,e5f2d52b802189ee658865ca93d83a8f,dd7ddc04e1b6c2c614352b383efe2d36,2017-05-03 11:05:13,239.90,19.93
```

7. olist_order_reviews_dataset.csv

```csv
"review_id","order_id","review_score","review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp"
"7bc2406110b926393aa56f80a40eba40","73fc7af87114b39712e6da79b0a377eb",4,,,2018-01-18 00:00:00,2018-01-18 21:46:59
"80e641a11e56f04c1ad469d5645fdfde","a548910a1c6147796b98fdf73dbeba33",5,,,2018-03-10 00:00:00,2018-03-11 03:05:13
```

8. olist_order_payments_dataset.csv

```csv
"order_id","payment_sequential","payment_type","payment_installments","payment_value"
b81ef226f3fe1789b1e8b2acac839d17,1,credit_card,8,99.33
a9810da82917af2d9aefd1278f1dcfa0,1,credit_card,1,24.39
```

9. product_category_name_translation.csv
   product_category_name,product_category_name_english
   beleza_saude,health_beauty
   informatica_acessorios,computers_accessories

Help me create a database schema migration for this for postgres.

```

```
