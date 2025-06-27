#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."
sleep 30

echo "Deploying Debezium connector..."

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
      "table.include.list": "public.users,public.products,public.orders,public.order_items",
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
echo "2. Monitor messages: docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce-server.public.users --from-beginning"
echo "3. Insert test data: docker exec -it postgres-source psql -U postgres -d ecommerce -c \"INSERT INTO orders (user_id, total_amount, status) VALUES (1, 99.99, 'completed');\"" 