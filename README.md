# Debezium Real-Time Analytics POC

This is a proof-of-concept implementation of Debezium for real-time analytics, demonstrating how to capture changes from a PostgreSQL transactional database and stream them to an analytics database in real-time.

## Architecture

```
[PostgreSQL Source] → [Debezium] → [Kafka] → [Analytics Consumer] → [PostgreSQL Analytics]
```

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available
- curl (for deploying the connector)

## Quick Start

### 1. Start the Services

```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
sleep 30
```

### 2. Deploy Debezium Connector

```bash
# Deploy the connector
./deploy-connector.sh
```

### 3. Verify Setup

```bash
# Check connector status
curl -X GET http://localhost:8083/connectors/postgres-connector/status

# List Kafka topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 4. Test Real-Time Sync

```bash
# Insert test data
docker exec -it postgres-source psql -U postgres -d ecommerce \
  -c "INSERT INTO orders (user_id, total_amount, status) VALUES (1, 99.99, 'completed');"

# Check analytics database
docker exec -it postgres-analytics psql -U postgres -d analytics \
  -c "SELECT * FROM user_analytics;"
```

## Services

- **postgres-source** (port 5432): Source transactional database
- **postgres-analytics** (port 5433): Analytics database
- **kafka** (port 9092): Apache Kafka broker
- **kafka-connect** (port 8083): Kafka Connect with Debezium
- **zookeeper** (port 2181): Zookeeper for Kafka
- **analytics-consumer**: Python consumer processing Kafka messages

## Database Schemas

### Source Database (ecommerce)

- `users`: User information
- `products`: Product catalog
- `orders`: Order headers
- `order_items`: Order line items

### Analytics Database (analytics)

- `user_analytics`: Aggregated user metrics
- `product_analytics`: Product performance metrics
- `order_analytics`: Order tracking

## Monitoring

### Check Kafka Messages

```bash
# Monitor user changes
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-server.public.users \
  --from-beginning

# Monitor order changes
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-server.public.orders \
  --from-beginning
```

### Check Consumer Logs

```bash
# View analytics consumer logs
docker-compose logs -f analytics-consumer
```

### Database Queries

#### Source Database

```bash
# Connect to source
docker exec -it postgres-source psql -U postgres -d ecommerce

# Check tables
\dt

# View data
SELECT * FROM users;
SELECT * FROM orders;
```

#### Analytics Database

```bash
# Connect to analytics
docker exec -it postgres-analytics psql -U postgres -d analytics

# Check analytics
SELECT * FROM user_analytics;
SELECT * FROM product_analytics;
SELECT * FROM order_analytics;
```

## Testing Scenarios

### 1. Create New User

```sql
INSERT INTO users (email, first_name, last_name)
VALUES ('alice@example.com', 'Alice', 'Johnson');
```

### 2. Create New Order

```sql
INSERT INTO orders (user_id, total_amount, status)
VALUES (1, 129.98, 'completed');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES (1, 1, 1, 999.99), (1, 2, 1, 29.99);
```

### 3. Update User

```sql
UPDATE users SET first_name = 'Johnny' WHERE id = 1;
```

### 4. Update Order Status

```sql
UPDATE orders SET status = 'shipped' WHERE id = 1;
```

## Troubleshooting

### Common Issues

1. **Connector fails to start**

   ```bash
   # Check logs
   docker-compose logs kafka-connect

   # Verify PostgreSQL configuration
   docker exec -it postgres-source psql -U postgres -c "SHOW wal_level;"
   ```

2. **No messages in Kafka**

   ```bash
   # Check connector status
   curl -X GET http://localhost:8083/connectors/postgres-connector/status

   # Check replication slots
   docker exec -it postgres-source psql -U postgres -c "SELECT * FROM pg_replication_slots;"
   ```

3. **Analytics not updating**

   ```bash
   # Check consumer logs
   docker-compose logs analytics-consumer

   # Verify Kafka topics
   docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

### Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (will delete all data)
docker-compose down -v
```

## Performance Notes

- **Latency**: Expect 100ms - 2 seconds end-to-end
- **Throughput**: Depends on WAL generation rate
- **Resource Usage**: ~4GB RAM for the entire stack
- **Storage**: Kafka retains messages for 7 days by default

## Next Steps

1. **Production Setup**: Use multiple Kafka brokers, proper monitoring
2. **Security**: Enable SSL/TLS, proper authentication
3. **Scaling**: Horizontal scaling for high-volume data
4. **Monitoring**: Prometheus/Grafana dashboards
5. **Error Handling**: Dead letter queues, retry mechanisms

## Files Structure

```
.
├── docker-compose.yml          # Main orchestration
├── deploy-connector.sh         # Connector deployment script
├── postgres/
│   ├── init.sql               # Source database setup
│   ├── postgresql.conf        # PostgreSQL configuration
│   └── analytics-init.sql     # Analytics database setup
└── analytics-consumer/
    ├── consumer.py            # Python consumer
    ├── requirements.txt       # Python dependencies
    └── Dockerfile            # Consumer container
```

This POC demonstrates the core concepts of Debezium for real-time analytics. The setup is suitable for development and testing, but requires additional configuration for production use.
