# Quick Start: Debezium for Real-Time Analytics

## Prerequisites

- Docker and Docker Compose
- 4GB+ RAM available

## Step 1: Create Project Structure

```bash
mkdir debezium-analytics && cd debezium-analytics
```

## Step 2: Docker Compose Setup

```yaml
# docker-compose.yml
version: "3.8"

services:
  postgres-source:
    image: postgres:15
    environment:
      POSTGRES_DB: ecommerce
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    command: postgres -c wal_level=logical

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-connect:
    image: debezium/connect:2.4
    depends_on:
      - kafka
      - postgres-source
    ports:
      - "8083:8083"
    environment:
      GROUP_ID: 1
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      BOOTSTRAP_SERVERS: kafka:9092

  postgres-analytics:
    image: postgres:15
    environment:
      POSTGRES_DB: analytics
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5433:5432"
```

## Step 3: Database Setup

```sql
-- init.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO users (email, name) VALUES
('john@example.com', 'John Doe'),
('jane@example.com', 'Jane Smith');

-- Create Debezium user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
```

## Step 4: Start Services

```bash
docker-compose up -d
sleep 30  # Wait for services to be ready
```

## Step 5: Deploy Debezium Connector

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres-source",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.dbname": "ecommerce",
      "database.server.name": "ecommerce-server",
      "table.include.list": "public.users,public.orders",
      "plugin.name": "pgoutput",
      "slot.name": "debezium_slot"
    }
  }' \
  http://localhost:8083/connectors
```

## Step 6: Verify Setup

```bash
# Check connector status
curl -X GET http://localhost:8083/connectors/postgres-connector/status

# List Kafka topics
docker exec -it debezium-analytics-kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 --list

# Monitor messages
docker exec -it debezium-analytics-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-server.public.users \
  --from-beginning
```

## Step 7: Test Real-Time Sync

```bash
# Insert test data
docker exec -it debezium-analytics-postgres-source-1 psql -U postgres -d ecommerce \
  -c "INSERT INTO orders (user_id, amount, status) VALUES (1, 99.99, 'completed');"

# Check Kafka messages
docker exec -it debezium-analytics-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-server.public.orders \
  --from-beginning
```

## Step 8: Create Analytics Consumer

```python
# consumer.py
import json
import psycopg2
from kafka import KafkaConsumer

def setup_analytics_db():
    conn = psycopg2.connect(
        host="localhost", port="5433",
        database="analytics", user="postgres", password="postgres"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_analytics (
            user_id INTEGER PRIMARY KEY,
            email VARCHAR(255),
            total_orders INTEGER DEFAULT 0,
            total_spent DECIMAL(10,2) DEFAULT 0
        )
    """)
    conn.commit()
    return conn

def process_message(conn, message):
    event = message.value
    cursor = conn.cursor()

    if 'users' in message.topic:
        if event['op'] == 'c':  # Create
            cursor.execute("""
                INSERT INTO user_analytics (user_id, email)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
            """, (event['after']['id'], event['after']['email']))
        elif event['op'] == 'u':  # Update
            cursor.execute("""
                UPDATE user_analytics SET email = %s WHERE user_id = %s
            """, (event['after']['email'], event['after']['id']))

    elif 'orders' in message.topic:
        if event['op'] == 'c':  # Create
            cursor.execute("""
                UPDATE user_analytics
                SET total_orders = total_orders + 1,
                    total_spent = total_spent + %s
                WHERE user_id = %s
            """, (event['after']['amount'], event['after']['user_id']))

    conn.commit()

def main():
    conn = setup_analytics_db()
    consumer = KafkaConsumer(
        'ecommerce-server.public.users',
        'ecommerce-server.public.orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting analytics consumer...")
    for message in consumer:
        process_message(conn, message)
        print(f"Processed: {message.topic} - {message.value['op']}")

if __name__ == "__main__":
    main()
```

## Step 9: Run Analytics Consumer

```bash
pip install kafka-python psycopg2-binary
python consumer.py
```

## Step 10: Test End-to-End

```bash
# Insert more data
docker exec -it debezium-analytics-postgres-source-1 psql -U postgres -d ecommerce \
  -c "INSERT INTO orders (user_id, amount, status) VALUES (1, 149.99, 'completed');"

# Check analytics
docker exec -it debezium-analytics-postgres-analytics-1 psql -U postgres -d analytics \
  -c "SELECT * FROM user_analytics;"
```

## Key Points

1. **PostgreSQL Configuration**: Must have `wal_level = logical`
2. **User Permissions**: Debezium user needs `REPLICATION` and `SELECT` privileges
3. **Kafka Topics**: Automatically created with pattern `{server}.{schema}.{table}`
4. **Message Format**: JSON with `op` (operation), `before`, and `after` fields
5. **Error Handling**: Monitor connector status and consumer lag

## Production Considerations

- **Security**: Use SSL/TLS, proper authentication
- **Monitoring**: Set up Prometheus/Grafana for metrics
- **Scaling**: Multiple Kafka brokers, horizontal scaling
- **Backup**: Regular backups of both databases
- **Testing**: Load testing, failure testing

This quick start provides a working foundation that you can build upon for your production deployment.
