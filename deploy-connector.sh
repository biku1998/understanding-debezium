#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."

# Wait for Kafka Connect to be ready
sleep 10

echo "Deploying Debezium connector..."

curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres-transactional",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.dbname": "ecommerce",
      "database.server.name": "ecommerce",
      "topic.prefix": "ecommerce",
      "table.include.list": "public.customers,public.sellers,public.products,public.orders,public.order_items,public.order_reviews,public.order_payments,public.product_category_name_translation",
      "plugin.name": "pgoutput",
      "publication.autocreate.mode": "filtered",
      "slot.name": "debezium_slot",
      "snapshot.mode": "initial",
      "snapshot.locking.mode": "minimal"
    }
  }' \
  http://localhost:8083/connectors

echo "Connector deployed. Checking status..."
sleep 5

curl -X GET http://localhost:8083/connectors/postgres-connector/status

echo ""
echo "Setup complete! You can now:"
echo "1. Check Kafka topics: docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list"
echo "2. Monitor messages: docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.public.customers --from-beginning"
echo "3. Insert test data: docker exec -it postgres-transactional psql -U postgres -d ecommerce -c \"INSERT INTO customers (customer_id, customer_unique_id, customer_city, customer_state) VALUES ('test123', 'unique123', 'Test City', 'TS');\"" 