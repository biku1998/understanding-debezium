#!/bin/bash

set -e

# Debug information
echo "=== Debug Information ==="
echo "Current directory: $(pwd)"
echo "Script location: $0"
echo "Script exists: $(test -f "$0" && echo "YES" || echo "NO")"
echo "Script permissions: $(ls -la "$0" 2>/dev/null || echo "Cannot check permissions")"
echo "Shell: $SHELL"
echo "========================="
echo ""

# Configuration from environment variables
KAFKA_CONNECT_URL=${KAFKA_CONNECT_URL:-http://kafka-connect:8083}
CONNECTOR_NAME=${CONNECTOR_NAME:-postgres-connector}
MAX_RETRIES=${MAX_RETRIES:-30}
RETRY_DELAY=${RETRY_DELAY:-10}

echo "=== Debezium Connector Deployer ==="
echo "Kafka Connect URL: $KAFKA_CONNECT_URL"
echo "Connector Name: $CONNECTOR_NAME"
echo "Max Retries: $MAX_RETRIES"
echo "Retry Delay: $RETRY_DELAY seconds"
echo ""

# Function to check if Kafka Connect is ready
check_kafka_connect() {
    echo "Checking if Kafka Connect is ready..."
    
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -s -f "$KAFKA_CONNECT_URL" > /dev/null 2>&1; then
            echo "✅ Kafka Connect is ready!"
            return 0
        else
            echo "⏳ Attempt $i/$MAX_RETRIES: Kafka Connect not ready yet..."
            if [ $i -lt $MAX_RETRIES ]; then
                sleep $RETRY_DELAY
            fi
        fi
    done
    
    echo "❌ Kafka Connect failed to start after $MAX_RETRIES attempts"
    return 1
}

# Function to check if connector already exists
check_connector_exists() {
    echo "Checking if connector '$CONNECTOR_NAME' already exists..."
    
    if curl -s -f "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
        echo "✅ Connector '$CONNECTOR_NAME' already exists"
        return 0
    else
        echo "ℹ️  Connector '$CONNECTOR_NAME' does not exist"
        return 1
    fi
}

# Function to deploy the connector
deploy_connector() {
    echo "Deploying Debezium connector..."
    
    # Connector configuration
    CONNECTOR_CONFIG=$(cat <<EOF
{
    "name": "$CONNECTOR_NAME",
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
}
EOF
)

    # Deploy the connector
    RESPONSE=$(curl -s -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        --data "$CONNECTOR_CONFIG" \
        "$KAFKA_CONNECT_URL/connectors")
    
    HTTP_CODE="${RESPONSE: -3}"
    RESPONSE_BODY="${RESPONSE%???}"
    
    if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "409" ]; then
        echo "✅ Connector deployed successfully (HTTP $HTTP_CODE)"
        return 0
    else
        echo "❌ Failed to deploy connector (HTTP $HTTP_CODE)"
        echo "Response: $RESPONSE_BODY"
        return 1
    fi
}

# Function to check connector status
check_connector_status() {
    echo "Checking connector status..."
    
    for i in $(seq 1 10); do
        STATUS_RESPONSE=$(curl -s "$KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null || echo "{}")
        CONNECTOR_STATE=$(echo "$STATUS_RESPONSE" | jq -r '.connector.state // "UNKNOWN"')
        
        echo "Connector state: $CONNECTOR_STATE"
        
        if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
            echo "✅ Connector is running successfully!"
            return 0
        elif [ "$CONNECTOR_STATE" = "FAILED" ]; then
            echo "❌ Connector failed to start"
            echo "Status details:"
            echo "$STATUS_RESPONSE" | jq '.'
            return 1
        else
            echo "⏳ Connector is starting... (attempt $i/10)"
            sleep 5
        fi
    done
    
    echo "⚠️  Connector status check timed out"
    return 1
}

# Function to list Kafka topics
list_kafka_topics() {
    echo ""
    echo "=== Available Kafka Topics ==="
    echo "You can monitor these topics:"
    echo "- ecommerce.public.customers"
    echo "- ecommerce.public.sellers"
    echo "- ecommerce.public.products"
    echo "- ecommerce.public.orders"
    echo "- ecommerce.public.order_items"
    echo "- ecommerce.public.order_reviews"
    echo "- ecommerce.public.order_payments"
    echo "- ecommerce.public.product_category_name_translation"
    echo ""
}

# Main execution
main() {
    # Wait for Kafka Connect to be ready
    if ! check_kafka_connect; then
        exit 1
    fi
    
    # Check if connector already exists
    if check_connector_exists; then
        echo "Connector already exists, checking status..."
    else
        # Deploy the connector
        if ! deploy_connector; then
            exit 1
        fi
    fi
    
    # Check connector status
    if ! check_connector_status; then
        exit 1
    fi
    
    # Show available topics
    list_kafka_topics
    
    echo "=== Setup Complete! ==="
    echo "You can now:"
    echo "1. Monitor messages: docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic ecommerce.public.customers --from-beginning"
    echo "2. Run data loader: docker exec -it transactional-dataloader /app/start.sh"
    echo "3. Check analytics: docker exec -it postgres-analytics psql -U postgres -d analytics -c \"SELECT COUNT(*) FROM fact_orders;\""
    echo ""
}

# Run main function
main 