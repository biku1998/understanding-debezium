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