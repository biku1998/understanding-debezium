# Practical Implementation: Debezium for Real-Time Analytics

This guide provides a complete, step-by-step implementation of Debezium for real-time analytics, building on the comprehensive guide.

## Prerequisites

- Docker and Docker Compose installed
- Basic knowledge of PostgreSQL, Kafka, and Docker
- At least 4GB RAM available for the stack

## Step 1: Project Setup

Create the following directory structure:

```
debezium-analytics/
├── docker-compose.yml
├── postgres/
│   ├── init.sql
│   └── postgresql.conf
├── kafka/
│   └── server.properties
├── debezium/
│   └── connector-config.json
├── analytics-consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
└── README.md
```

## Step 2: Docker Compose Setup

```yaml
# docker-compose.yml
version: "3.8"

services:
  # PostgreSQL Source Database
  postgres-source:
    image: postgres:15
    container_name: postgres-source
    environment:
      POSTGRES_DB: ecommerce
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - ./postgres/postgresql.conf:/etc/postgresql/postgresql.conf
    command: postgres -c config_file=/etc/postgresql/postgresql.conf
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Zookeeper
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  # Kafka
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    volumes:
      - ./kafka/server.properties:/etc/kafka/server.properties
    command: kafka-server-start /etc/kafka/server.properties

  # Kafka Connect with Debezium
  kafka-connect:
    image: debezium/connect:2.4
    container_name: kafka-connect
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
    volumes:
      - ./debezium/connector-config.json:/tmp/connector-config.json

  # Analytics PostgreSQL Database
  postgres-analytics:
    image: postgres:15
    container_name: postgres-analytics
    environment:
      POSTGRES_DB: analytics
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5433:5432"
    volumes:
      - ./postgres/analytics-init.sql:/docker-entrypoint-initdb.d/analytics-init.sql

  # Analytics Consumer
  analytics-consumer:
    build: ./analytics-consumer
    container_name: analytics-consumer
    depends_on:
      - kafka
      - postgres-analytics
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      ANALYTICS_DB_HOST: postgres-analytics
      ANALYTICS_DB_PORT: 5432
      ANALYTICS_DB_NAME: analytics
      ANALYTICS_DB_USER: postgres
      ANALYTICS_DB_PASSWORD: postgres

  # Monitoring Stack
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./monitoring/grafana/datasources:/etc/grafana/provisioning/datasources
```

## Step 3: PostgreSQL Configuration

```sql
-- postgres/init.sql
-- Create the source database schema
CREATE DATABASE ecommerce;

\c ecommerce;

-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (email, first_name, last_name) VALUES
('john@example.com', 'John', 'Doe'),
('jane@example.com', 'Jane', 'Smith'),
('bob@example.com', 'Bob', 'Johnson');

INSERT INTO products (name, description, price, stock_quantity) VALUES
('Laptop', 'High-performance laptop', 999.99, 50),
('Mouse', 'Wireless mouse', 29.99, 100),
('Keyboard', 'Mechanical keyboard', 89.99, 75);

-- Create Debezium user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

```conf
# postgres/postgresql.conf
# Basic settings
listen_addresses = '*'
port = 5432
max_connections = 100

# Memory settings
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB
maintenance_work_mem = 64MB

# WAL settings for logical replication
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_segments = 64

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_rotation_age = 1d
log_rotation_size = 100MB
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = -1
log_autovacuum_min_duration = 0
log_error_verbosity = verbose
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_statement = 'all'
```

```sql
-- postgres/analytics-init.sql
-- Create analytics database schema
CREATE DATABASE analytics;

\c analytics;

-- Create analytics tables
CREATE TABLE user_analytics (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(10,2) DEFAULT 0,
    last_order_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE product_analytics (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    name VARCHAR(255),
    total_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0,
    average_price DECIMAL(10,2) DEFAULT 0,
    last_sale_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_analytics (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    user_id INTEGER,
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    items_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_user_analytics_user_id ON user_analytics(user_id);
CREATE INDEX idx_product_analytics_product_id ON product_analytics(product_id);
CREATE INDEX idx_order_analytics_order_id ON order_analytics(order_id);
CREATE INDEX idx_order_analytics_user_id ON order_analytics(user_id);
```

## Step 4: Kafka Configuration

```properties
# kafka/server.properties
# Broker settings
broker.id=1
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://localhost:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log settings
log.dirs=/var/lib/kafka/data
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# Zookeeper
zookeeper.connect=zookeeper:2181
zookeeper.connection.timeout.ms=18000

# Group coordinator
group.initial.rebalance.delay.ms=0

# Compression
compression.type=lz4
```

## Step 5: Debezium Connector Configuration

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "ecommerce",
    "database.server.name": "ecommerce-server",
    "table.include.list": "public.users,public.products,public.orders,public.order_items",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "slot.name": "debezium_slot",
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",
    "include.schema.changes": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewDocumentState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.operation.header": "true"
  }
}
```

## Step 6: Analytics Consumer

```python
# analytics-consumer/consumer.py
import json
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsConsumer:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.db_config = {
            'host': os.getenv('ANALYTICS_DB_HOST', 'localhost'),
            'port': os.getenv('ANALYTICS_DB_PORT', '5432'),
            'database': os.getenv('ANALYTICS_DB_NAME', 'analytics'),
            'user': os.getenv('ANALYTICS_DB_USER', 'postgres'),
            'password': os.getenv('ANALYTICS_DB_PASSWORD', 'postgres')
        }

        self.consumer = None
        self.db_connection = None

    def connect_kafka(self):
        """Connect to Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'ecommerce-server.public.users',
                'ecommerce-server.public.products',
                'ecommerce-server.public.orders',
                'ecommerce-server.public.order_items',
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='analytics-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Connected to Kafka")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def connect_database(self):
        """Connect to PostgreSQL analytics database"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("Connected to analytics database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def process_user_event(self, event):
        """Process user events"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if event['op'] == 'c':  # Create
                    cursor.execute("""
                        INSERT INTO user_analytics (user_id, email, first_name, last_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            email = EXCLUDED.email,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        event['after']['id'],
                        event['after']['email'],
                        event['after']['first_name'],
                        event['after']['last_name']
                    ))

                elif event['op'] == 'u':  # Update
                    cursor.execute("""
                        UPDATE user_analytics
                        SET email = %s, first_name = %s, last_name = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    """, (
                        event['after']['email'],
                        event['after']['first_name'],
                        event['after']['last_name'],
                        event['after']['id']
                    ))

                elif event['op'] == 'd':  # Delete
                    cursor.execute("DELETE FROM user_analytics WHERE user_id = %s",
                                 (event['before']['id'],))

                self.db_connection.commit()
                logger.info(f"Processed user event: {event['op']} for user {event.get('after', {}).get('id', event.get('before', {}).get('id'))}")

        except Exception as e:
            logger.error(f"Error processing user event: {e}")
            self.db_connection.rollback()

    def process_product_event(self, event):
        """Process product events"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if event['op'] == 'c':  # Create
                    cursor.execute("""
                        INSERT INTO product_analytics (product_id, name, average_price)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (product_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            average_price = EXCLUDED.average_price,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        event['after']['id'],
                        event['after']['name'],
                        event['after']['price']
                    ))

                elif event['op'] == 'u':  # Update
                    cursor.execute("""
                        UPDATE product_analytics
                        SET name = %s, average_price = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (
                        event['after']['name'],
                        event['after']['price'],
                        event['after']['id']
                    ))

                elif event['op'] == 'd':  # Delete
                    cursor.execute("DELETE FROM product_analytics WHERE product_id = %s",
                                 (event['before']['id'],))

                self.db_connection.commit()
                logger.info(f"Processed product event: {event['op']} for product {event.get('after', {}).get('id', event.get('before', {}).get('id'))}")

        except Exception as e:
            logger.error(f"Error processing product event: {e}")
            self.db_connection.rollback()

    def process_order_event(self, event):
        """Process order events"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if event['op'] == 'c':  # Create
                    # Insert order analytics
                    cursor.execute("""
                        INSERT INTO order_analytics (order_id, user_id, total_amount, status)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (order_id) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            total_amount = EXCLUDED.total_amount,
                            status = EXCLUDED.status
                    """, (
                        event['after']['id'],
                        event['after']['user_id'],
                        event['after']['total_amount'],
                        event['after']['status']
                    ))

                    # Update user analytics
                    cursor.execute("""
                        UPDATE user_analytics
                        SET total_orders = total_orders + 1,
                            total_spent = total_spent + %s,
                            last_order_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    """, (event['after']['total_amount'], event['after']['user_id']))

                elif event['op'] == 'u':  # Update
                    cursor.execute("""
                        UPDATE order_analytics
                        SET user_id = %s, total_amount = %s, status = %s
                        WHERE order_id = %s
                    """, (
                        event['after']['user_id'],
                        event['after']['total_amount'],
                        event['after']['status'],
                        event['after']['id']
                    ))

                elif event['op'] == 'd':  # Delete
                    cursor.execute("DELETE FROM order_analytics WHERE order_id = %s",
                                 (event['before']['id'],))

                self.db_connection.commit()
                logger.info(f"Processed order event: {event['op']} for order {event.get('after', {}).get('id', event.get('before', {}).get('id'))}")

        except Exception as e:
            logger.error(f"Error processing order event: {e}")
            self.db_connection.rollback()

    def process_order_item_event(self, event):
        """Process order item events"""
        try:
            with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if event['op'] == 'c':  # Create
                    # Update product analytics
                    cursor.execute("""
                        UPDATE product_analytics
                        SET total_sold = total_sold + %s,
                            total_revenue = total_revenue + (%s * %s),
                            last_sale_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (
                        event['after']['quantity'],
                        event['after']['quantity'],
                        event['after']['unit_price'],
                        event['after']['product_id']
                    ))

                    # Update order analytics
                    cursor.execute("""
                        UPDATE order_analytics
                        SET items_count = items_count + 1
                        WHERE order_id = %s
                    """, (event['after']['order_id'],))

                elif event['op'] == 'd':  # Delete
                    # Update product analytics
                    cursor.execute("""
                        UPDATE product_analytics
                        SET total_sold = total_sold - %s,
                            total_revenue = total_revenue - (%s * %s),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (
                        event['before']['quantity'],
                        event['before']['quantity'],
                        event['before']['unit_price'],
                        event['before']['product_id']
                    ))

                    # Update order analytics
                    cursor.execute("""
                        UPDATE order_analytics
                        SET items_count = items_count - 1
                        WHERE order_id = %s
                    """, (event['before']['order_id'],))

                self.db_connection.commit()
                logger.info(f"Processed order item event: {event['op']}")

        except Exception as e:
            logger.error(f"Error processing order item event: {e}")
            self.db_connection.rollback()

    def process_message(self, message):
        """Process incoming Kafka message"""
        try:
            event = message.value
            topic = message.topic

            if 'users' in topic:
                self.process_user_event(event)
            elif 'products' in topic:
                self.process_product_event(event)
            elif 'orders' in topic and 'order_items' not in topic:
                self.process_order_event(event)
            elif 'order_items' in topic:
                self.process_order_item_event(event)
            else:
                logger.warning(f"Unknown topic: {topic}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main consumer loop"""
        try:
            self.connect_kafka()
            self.connect_database()

            logger.info("Starting analytics consumer...")

            for message in self.consumer:
                self.process_message(message)

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.db_connection:
                self.db_connection.close()

if __name__ == "__main__":
    consumer = AnalyticsConsumer()
    consumer.run()
```

```txt
# analytics-consumer/requirements.txt
kafka-python==2.0.2
psycopg2-binary==2.9.7
```

```dockerfile
# analytics-consumer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]
```

## Step 7: Monitoring Setup

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "kafka"
    static_configs:
      - targets: ["kafka:9090"]
    metrics_path: "/metrics"

  - job_name: "kafka-connect"
    static_configs:
      - targets: ["kafka-connect:8083"]
    metrics_path: "/metrics"

  - job_name: "postgres-source"
    static_configs:
      - targets: ["postgres-source:5432"]
    metrics_path: "/metrics"

  - job_name: "postgres-analytics"
    static_configs:
      - targets: ["postgres-analytics:5432"]
    metrics_path: "/metrics"
```

## Step 8: Deployment and Testing

### Start the Stack

```bash
# Start all services
docker-compose up -d

# Wait for services to be ready
docker-compose logs -f
```

### Deploy Debezium Connector

```bash
# Wait for Kafka Connect to be ready
sleep 30

# Deploy the connector
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/connector-config.json \
  http://localhost:8083/connectors

# Check connector status
curl -X GET http://localhost:8083/connectors/postgres-connector/status
```

### Test the Setup

#### 1. Insert Test Data

```bash
# Connect to source database
docker exec -it postgres-source psql -U postgres -d ecommerce

# Insert test data
INSERT INTO orders (user_id, total_amount, status) VALUES (1, 129.98, 'completed');
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 999.99), (1, 2, 1, 29.99);
```

#### 2. Verify Analytics

```bash
# Connect to analytics database
docker exec -it postgres-analytics psql -U postgres -d analytics

# Check analytics data
SELECT * FROM user_analytics;
SELECT * FROM product_analytics;
SELECT * FROM order_analytics;
```

#### 3. Monitor Kafka Topics

```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Monitor messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-server.public.orders \
  --from-beginning
```

## Step 9: Performance Testing

### Load Testing Script

```python
# load_test.py
import psycopg2
import random
import time
from datetime import datetime

def load_test():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="ecommerce",
        user="postgres",
        password="postgres"
    )

    cursor = conn.cursor()

    # Generate random orders
    for i in range(1000):
        user_id = random.randint(1, 3)
        product_id = random.randint(1, 3)
        quantity = random.randint(1, 5)

        # Get product price
        cursor.execute("SELECT price FROM products WHERE id = %s", (product_id,))
        price = cursor.fetchone()[0]
        total_amount = price * quantity

        # Create order
        cursor.execute("""
            INSERT INTO orders (user_id, total_amount, status)
            VALUES (%s, %s, 'completed') RETURNING id
        """, (user_id, total_amount))
        order_id = cursor.fetchone()[0]

        # Create order item
        cursor.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
        """, (order_id, product_id, quantity, price))

        conn.commit()

        if i % 100 == 0:
            print(f"Created {i} orders")
            time.sleep(0.1)  # Small delay to avoid overwhelming

    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_test()
```

## Step 10: Monitoring and Alerting

### Key Metrics to Monitor

1. **Kafka Metrics**:

   - Consumer lag
   - Message throughput
   - Topic partition sizes

2. **Debezium Metrics**:

   - Connector status
   - Snapshot progress
   - Error rates

3. **PostgreSQL Metrics**:

   - WAL generation rate
   - Replication lag
   - Connection count

4. **Application Metrics**:
   - End-to-end latency
   - Processing rate
   - Error rates

### Grafana Dashboard Queries

```sql
-- User Analytics Dashboard
SELECT
    user_id,
    email,
    total_orders,
    total_spent,
    last_order_date
FROM user_analytics
ORDER BY total_spent DESC
LIMIT 10;

-- Product Analytics Dashboard
SELECT
    product_id,
    name,
    total_sold,
    total_revenue,
    average_price
FROM product_analytics
ORDER BY total_revenue DESC
LIMIT 10;

-- Order Analytics Dashboard
SELECT
    DATE(created_at) as order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM order_analytics
GROUP BY DATE(created_at)
ORDER BY order_date DESC
LIMIT 30;
```

## Troubleshooting

### Common Issues

1. **Connector Fails to Start**:

   ```bash
   # Check logs
   docker-compose logs kafka-connect

   # Verify PostgreSQL configuration
   docker exec -it postgres-source psql -U postgres -c "SHOW wal_level;"
   ```

2. **High Consumer Lag**:

   ```bash
   # Check consumer group
   docker exec -it kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe --group analytics-consumer-group
   ```

3. **Data Not Appearing in Analytics**:

   ```bash
   # Check consumer logs
   docker-compose logs analytics-consumer

   # Verify Kafka topics
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic ecommerce-server.public.users \
     --from-beginning
   ```

## Production Considerations

1. **Security**:

   - Use SSL/TLS for all connections
   - Implement proper authentication
   - Use secrets management

2. **Scalability**:

   - Use multiple Kafka brokers
   - Implement horizontal scaling
   - Use connection pooling

3. **Monitoring**:

   - Set up comprehensive alerting
   - Monitor resource usage
   - Track business metrics

4. **Backup and Recovery**:
   - Regular backups of both databases
   - Test recovery procedures
   - Document runbooks

This practical implementation provides a complete, working example of Debezium for real-time analytics that you can use as a starting point for your production deployment.
