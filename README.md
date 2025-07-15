# Debezium Real-Time Analytics POC

This is a proof-of-concept implementation of Debezium for real-time analytics, demonstrating how to capture changes from a PostgreSQL transactional database and stream them to an analytics database in real-time.

## Architecture

```
[PostgreSQL Source] → [Debezium] → [Kafka] → [Analytics Consumer] → [PostgreSQL Analytics]
```

### Network Configuration

All services run on a custom Docker network called `debezium-network` for better isolation and explicit control. This follows production best practices and makes the architecture more clear.

**Network Details:**

- **Network Name**: `debezium-network`
- **Driver**: `bridge`
- **Services**: All 9 services are connected to this network
- **Communication**: Services can reach each other using service names (e.g., `kafka:9092`, `postgres-transactional:5432`)

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available
- curl (for deploying the connector)

## Quick Start

### 1. Start the Services

```bash
# Start all services with fresh data
./start.sh

# Or start manually
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
sleep 30
```

### 2. Deploy Debezium Connector

The connector is automatically deployed by the `connector-deployer` service when you start the stack. No manual intervention needed!

If you need to redeploy the connector manually:

```bash
# Redeploy the connector
docker-compose up connector-deployer
```

### 3. Verify Setup

```bash
# Check connector status
curl -X GET http://localhost:8083/connectors/postgres-connector/status

# List Kafka topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 4. Test Real-Time Sync

#### Option A: Manual Test Data

```bash
# Insert test data
docker exec -it postgres-transactional psql -U postgres -d ecommerce \
  -c "INSERT INTO customers (customer_id, customer_unique_id, customer_city, customer_state) VALUES ('test123', 'unique123', 'Test City', 'TS');"

# Check analytics database
docker exec -it postgres-analytics psql -U postgres -d analytics \
  -c "SELECT * FROM dim_customers;"
```

#### Option B: Load Sample Data (Recommended)

```bash
# Run the data loader to populate the transactional database
docker exec -it transactional-dataloader python data-loader.py

# The analytics consumer will automatically process the data as it's loaded
```

## Services

- **postgres-transactional** (port 5432): Source transactional database
- **postgres-analytics** (port 5433): Analytics database
- **kafka** (port 9092): Apache Kafka broker
- **kafka-connect** (port 8083): Kafka Connect with Debezium
- **connector-deployer**: Automatically deploys the Debezium connector
- **zookeeper** (port 2181): Zookeeper for Kafka
- **analytics-consumer**: Python consumer processing Kafka messages
- **transactional-dataloader**: Data loading service with e-commerce dataset
- **metabase** (port 3000): Analytics visualization dashboard

## Data Loading

The project includes a comprehensive e-commerce dataset from Olist (Brazilian e-commerce platform) that provides realistic data for testing Debezium's change capture capabilities.

### Dataset Contents

- **Customers**: 99,441 customer records
- **Sellers**: 3,095 seller records
- **Products**: 32,951 product records
- **Orders**: 99,441 order records
- **Order Items**: 112,650 order item records
- **Order Payments**: 103,886 payment records
- **Order Reviews**: 99,224 review records
- **Geolocation**: 8,016 location records
- **Product Categories**: 71 category translations

### Loading Process

The data loader:

1. **Clears existing data** to ensure a clean state
2. **Loads static data** (customers, sellers, products, geolocation, categories)
3. **Loads transactional data** in batches (orders, items, payments, reviews)
4. **Simulates realistic timing** with delays between batches

### Configuration

The data loader is configured directly in `docker-compose.yml`:

```yaml
environment:
  SPEED_MULTIPLIER: 10.0 # Speed multiplier (1.0 = real time)
  DURATION_HOURS: 2.0 # Simulation duration in hours
  CLEAR_DATA: true # Clear existing data before loading
```

## Database Schemas

### Source Database (ecommerce)

- `customers`: Customer information
- `sellers`: Seller information
- `products`: Product catalog
- `orders`: Order headers
- `order_items`: Order line items
- `order_payments`: Payment information
- `order_reviews`: Customer reviews
- `geolocation`: Geographic data
- `product_category_name_translation`: Category translations

### Analytics Database (analytics)

- `fact_orders`: Fact table with order details
- `dim_customers`: Customer dimension table
- `dim_sellers`: Seller dimension table
- `dim_products`: Product dimension table
- `dim_time`: Time dimension table
- `product_category_name_translation`: Category lookup table

## Monitoring

### Check Kafka Messages

```bash
# Monitor customer changes
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.public.customers \
  --from-beginning

# Monitor order changes
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.public.orders \
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
docker exec -it postgres-transactional psql -U postgres -d ecommerce

# Check tables
\dt

# View data
SELECT * FROM customers LIMIT 5;
SELECT * FROM orders LIMIT 5;
SELECT COUNT(*) FROM orders;
```

#### Analytics Database

```bash
# Connect to analytics
docker exec -it postgres-analytics psql -U postgres -d analytics

# Check analytics
SELECT * FROM dim_customers LIMIT 5;
SELECT * FROM fact_orders LIMIT 5;
SELECT COUNT(*) FROM fact_orders;
```

## Testing Scenarios

### 1. Create New Customer

```sql
INSERT INTO customers (customer_id, customer_unique_id, customer_city, customer_state)
VALUES ('test_cust_001', 'unique_001', 'São Paulo', 'SP');
```

### 2. Create New Order

```sql
INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
VALUES ('test_order_001', 'test_cust_001', 'processing', NOW());

INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, price, freight_value)
VALUES ('test_order_001', 1, 'product_001', 'seller_001', 99.99, 10.00);
```

### 3. Update Customer

```sql
UPDATE customers SET customer_city = 'Rio de Janeiro' WHERE customer_id = 'test_cust_001';
```

### 4. Update Order Status

```sql
UPDATE orders SET order_status = 'shipped' WHERE order_id = 'test_order_001';
```

## Troubleshooting

### Common Issues

1. **Connector fails to start**

   ```bash
   # Check logs
   docker-compose logs kafka-connect

   # Verify PostgreSQL configuration
   docker exec -it postgres-transactional psql -U postgres -c "SHOW wal_level;"
   ```

2. **No messages in Kafka**

   ```bash
   # Check connector status
   curl -X GET http://localhost:8083/connectors/postgres-connector/status

   # Check replication slots
   docker exec -it postgres-transactional psql -U postgres -c "SELECT * FROM pg_replication_slots;"
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
./stop.sh

# Or stop manually
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
├── docker-compose.yml                    # Main orchestration
├── start.sh                             # Start script (clears data and starts fresh)
├── stop.sh                              # Stop script
├── postgres/
│   ├── transactional-init.sql           # Source database setup
│   ├── postgresql.conf                  # PostgreSQL configuration
│   └── analytics-init.sql               # Analytics database setup
├── connector-deployer/
│   ├── deploy-connector.sh              # Dockerized connector deployment
│   └── Dockerfile                       # Connector deployer container
├── transactional-dataloader/
│   ├── data-loader.py                   # Data loading script
│   ├── requirements.txt                 # Python dependencies
│   ├── Dockerfile                       # Data loader container
│   └── e-commerce-data/                 # CSV data files
└── analytics-consumer/
    ├── consumer.py                      # Python consumer
    ├── requirements.txt                 # Python dependencies
    └── Dockerfile                       # Consumer container
```

This POC demonstrates the core concepts of Debezium for real-time analytics. The setup is suitable for development and testing, but requires additional configuration for production use.
