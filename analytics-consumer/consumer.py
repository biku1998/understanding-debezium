import json
import logging
import os
import psycopg2
import base64
import struct
import time
from datetime import datetime
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalyticsConsumer:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.db_config = {
            'host': os.getenv('ANALYTICS_DB_HOST', 'localhost'),
            'port': int(os.getenv('ANALYTICS_DB_PORT', '5432')),
            'database': os.getenv('ANALYTICS_DB_NAME', 'analytics'),
            'user': os.getenv('ANALYTICS_DB_USER', 'postgres'),
            'password': os.getenv('ANALYTICS_DB_PASSWORD', 'postgres')
        }
        self.consumer = None
        self.connection_pool = None

    def decode_decimal(self, encoded_value):
        """Improved decimal decoding with better error handling for Debezium format"""
        try:
            if encoded_value is None:
                return 0.0
            if isinstance(encoded_value, (int, float)):
                return float(encoded_value)
            if isinstance(encoded_value, str):
                # Handle Debezium decimal format
                decoded_bytes = base64.b64decode(encoded_value)
                # Check if it's a valid decimal format
                if len(decoded_bytes) < 1:
                    logger.warning(f"Invalid decimal format: {encoded_value}, decoded bytes: {decoded_bytes}")
                    return 0.0
                if len(decoded_bytes) == 1:
                    value = struct.unpack('>b', decoded_bytes)[0]
                elif len(decoded_bytes) == 2:
                    value = struct.unpack('>h', decoded_bytes)[0]
                elif len(decoded_bytes) == 4:
                    value = struct.unpack('>i', decoded_bytes)[0]
                elif len(decoded_bytes) == 8:
                    value = struct.unpack('>q', decoded_bytes)[0]
                else:
                    value = int.from_bytes(decoded_bytes, byteorder='big', signed=True)
                scale = 2
                result = value / (10 ** scale)
                return float(result)
            return 0.0
        except Exception as e:
            logger.warning(f"Failed to decode decimal value {encoded_value}: {e}")
            return 0.0

    def get_safe_value(self, data, key, default=None):
        """Safely get value from nested dictionary with null checks"""
        try:
            return data.get(key, default) if data else default
        except (KeyError, TypeError, AttributeError):
            return default

    def connect_kafka(self, retries=5, delay=5):
        for attempt in range(retries):
            try:
                self.consumer = KafkaConsumer(
                    'ecommerce-server.public.users',
                    'ecommerce-server.public.products',
                    'ecommerce-server.public.orders',
                    'ecommerce-server.public.order_items',
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,  # manual commit
                    group_id='analytics-consumer-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("Connected to Kafka")
                return
            except Exception as e:
                logger.error(f"Kafka connection failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("All Kafka connection retries failed.")
                    raise

    def connect_database(self, retries=5, delay=3):
        for attempt in range(retries):
            try:
                # Create connection pool for better performance
                self.connection_pool = SimpleConnectionPool(
                    minconn=1, maxconn=10, **self.db_config
                )
                logger.info("Connected to analytics database with connection pool")
                return
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("All retries failed.")
                    raise

    def get_db_connection(self):
        """Get connection from pool"""
        return self.connection_pool.getconn()

    def return_db_connection(self, conn):
        """Return connection to pool"""
        self.connection_pool.putconn(conn)

    def safe_commit(self):
        try:
            self.consumer.commit()
        except Exception as e:
            logger.error(f"Failed to commit Kafka offsets: {e}")
            # Re-raise to prevent silent failures
            raise

    def process_user_event(self, event):
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    # Validate required fields
                    user_id = self.get_safe_value(after_data, 'id')
                    email = self.get_safe_value(after_data, 'email')
                    first_name = self.get_safe_value(after_data, 'first_name')
                    last_name = self.get_safe_value(after_data, 'last_name')
                    
                    if not user_id:
                        logger.warning("Skipping user create event: missing user_id")
                        return
                    
                    cursor.execute("""
                        INSERT INTO user_analytics (user_id, email, first_name, last_name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            email = EXCLUDED.email,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            updated_at = CURRENT_TIMESTAMP
                    """, (user_id, email, first_name, last_name))
                    
                elif event['op'] == 'u':
                    user_id = self.get_safe_value(after_data, 'id')
                    email = self.get_safe_value(after_data, 'email')
                    first_name = self.get_safe_value(after_data, 'first_name')
                    last_name = self.get_safe_value(after_data, 'last_name')
                    
                    if not user_id:
                        logger.warning("Skipping user update event: missing user_id")
                        return
                    
                    cursor.execute("""
                        UPDATE user_analytics
                        SET email = %s, first_name = %s, last_name = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    """, (email, first_name, last_name, user_id))
                    
                elif event['op'] == 'd':
                    user_id = self.get_safe_value(before_data, 'id')
                    if not user_id:
                        logger.warning("Skipping user delete event: missing user_id")
                        return
                    
                    cursor.execute("DELETE FROM user_analytics WHERE user_id = %s", (user_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"User event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_product_event(self, event):
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                # Get price from after or before data
                price = self.decode_decimal(
                    self.get_safe_value(after_data, 'price') or 
                    self.get_safe_value(before_data, 'price')
                )
                
                if event['op'] == 'c':
                    product_id = self.get_safe_value(after_data, 'id')
                    name = self.get_safe_value(after_data, 'name')
                    
                    if not product_id:
                        logger.warning("Skipping product create event: missing product_id")
                        return
                    
                    cursor.execute("""
                        INSERT INTO product_analytics (product_id, name, average_price)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (product_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            average_price = EXCLUDED.average_price,
                            updated_at = CURRENT_TIMESTAMP
                    """, (product_id, name, price))
                    
                elif event['op'] == 'u':
                    product_id = self.get_safe_value(after_data, 'id')
                    name = self.get_safe_value(after_data, 'name')
                    
                    if not product_id:
                        logger.warning("Skipping product update event: missing product_id")
                        return
                    
                    cursor.execute("""
                        UPDATE product_analytics
                        SET name = %s, average_price = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (name, price, product_id))
                    
                elif event['op'] == 'd':
                    product_id = self.get_safe_value(before_data, 'id')
                    if not product_id:
                        logger.warning("Skipping product delete event: missing product_id")
                        return
                    
                    cursor.execute("DELETE FROM product_analytics WHERE product_id = %s", (product_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Product event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_order_event(self, event):
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                total_amount = self.decode_decimal(
                    self.get_safe_value(after_data, 'total_amount') or 
                    self.get_safe_value(before_data, 'total_amount')
                )
                
                if event['op'] == 'c':
                    order_id = self.get_safe_value(after_data, 'id')
                    user_id = self.get_safe_value(after_data, 'user_id')
                    status = self.get_safe_value(after_data, 'status')
                    
                    if not order_id or not user_id:
                        logger.warning("Skipping order create event: missing order_id or user_id")
                        return
                    
                    # Use transaction for related operations
                    cursor.execute("""
                        INSERT INTO order_analytics (order_id, user_id, total_amount, status)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (order_id) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            total_amount = EXCLUDED.total_amount,
                            status = EXCLUDED.status
                    """, (order_id, user_id, total_amount, status))
                    
                    cursor.execute("""
                        UPDATE user_analytics 
                        SET total_orders = total_orders + 1,
                            total_spent = total_spent + %s,
                            last_order_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    """, (total_amount, user_id))
                    
                elif event['op'] == 'u':
                    order_id = self.get_safe_value(after_data, 'id')
                    user_id = self.get_safe_value(after_data, 'user_id')
                    status = self.get_safe_value(after_data, 'status')
                    old_user_id = self.get_safe_value(before_data, 'user_id')
                    old_total_amount = self.decode_decimal(self.get_safe_value(before_data, 'total_amount'))
                    
                    if not order_id or not user_id:
                        logger.warning("Skipping order update event: missing order_id or user_id")
                        return
                    
                    # Update order analytics
                    cursor.execute("""
                        UPDATE order_analytics 
                        SET user_id = %s, total_amount = %s, status = %s
                        WHERE order_id = %s
                    """, (user_id, total_amount, status, order_id))
                    
                    # Handle user change and amount change
                    if old_user_id and old_user_id != user_id:
                        # Remove from old user
                        cursor.execute("""
                            UPDATE user_analytics 
                            SET total_orders = GREATEST(0, total_orders - 1),
                                total_spent = GREATEST(0, total_spent - %s),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s
                        """, (old_total_amount, old_user_id))
                        
                        # Add to new user
                        cursor.execute("""
                            UPDATE user_analytics 
                            SET total_orders = total_orders + 1,
                                total_spent = total_spent + %s,
                                last_order_date = CURRENT_TIMESTAMP,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s
                        """, (total_amount, user_id))
                    elif old_user_id == user_id and old_total_amount != total_amount:
                        # Update amount for same user
                        amount_diff = total_amount - old_total_amount
                        cursor.execute("""
                            UPDATE user_analytics 
                            SET total_spent = total_spent + %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s
                        """, (amount_diff, user_id))
                    
                elif event['op'] == 'd':
                    order_id = self.get_safe_value(before_data, 'id')
                    user_id = self.get_safe_value(before_data, 'user_id')
                    old_total_amount = self.decode_decimal(self.get_safe_value(before_data, 'total_amount'))
                    
                    if not order_id:
                        logger.warning("Skipping order delete event: missing order_id")
                        return
                    
                    cursor.execute("DELETE FROM order_analytics WHERE order_id = %s", (order_id,))
                    
                    # Update user analytics
                    if user_id:
                        cursor.execute("""
                            UPDATE user_analytics 
                            SET total_orders = GREATEST(0, total_orders - 1),
                                total_spent = GREATEST(0, total_spent - %s),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = %s
                        """, (old_total_amount, user_id))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Order event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_order_item_event(self, event):
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                unit_price = self.decode_decimal(
                    self.get_safe_value(after_data, 'unit_price') or 
                    self.get_safe_value(before_data, 'unit_price')
                )
                quantity = self.get_safe_value(after_data, 'quantity') or self.get_safe_value(before_data, 'quantity', 0)
                
                if event['op'] == 'c':
                    product_id = self.get_safe_value(after_data, 'product_id')
                    order_id = self.get_safe_value(after_data, 'order_id')
                    
                    if not product_id or not order_id:
                        logger.warning("Skipping order item create event: missing product_id or order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE product_analytics 
                        SET total_sold = total_sold + %s,
                            total_revenue = total_revenue + (%s * %s),
                            last_sale_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (quantity, quantity, unit_price, product_id))
                    
                    cursor.execute("""
                        UPDATE order_analytics 
                        SET items_count = items_count + 1
                        WHERE order_id = %s
                    """, (order_id,))
                    
                elif event['op'] == 'u':
                    # Handle order item updates
                    product_id = self.get_safe_value(after_data, 'product_id')
                    order_id = self.get_safe_value(after_data, 'order_id')
                    old_quantity = self.get_safe_value(before_data, 'quantity', 0)
                    old_unit_price = self.decode_decimal(self.get_safe_value(before_data, 'unit_price'))
                    
                    if not product_id or not order_id:
                        logger.warning("Skipping order item update event: missing product_id or order_id")
                        return
                    
                    # Update product analytics with quantity/price difference
                    quantity_diff = quantity - old_quantity
                    revenue_diff = (quantity * unit_price) - (old_quantity * old_unit_price)
                    
                    cursor.execute("""
                        UPDATE product_analytics 
                        SET total_sold = total_sold + %s,
                            total_revenue = total_revenue + %s,
                            last_sale_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (quantity_diff, revenue_diff, product_id))
                    
                elif event['op'] == 'd':
                    product_id = self.get_safe_value(before_data, 'product_id')
                    order_id = self.get_safe_value(before_data, 'order_id')
                    
                    if not product_id or not order_id:
                        logger.warning("Skipping order item delete event: missing product_id or order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE product_analytics 
                        SET total_sold = GREATEST(0, total_sold - %s),
                            total_revenue = GREATEST(0, total_revenue - (%s * %s)),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE product_id = %s
                    """, (quantity, quantity, unit_price, product_id))
                    
                    cursor.execute("""
                        UPDATE order_analytics 
                        SET items_count = GREATEST(0, items_count - 1)
                        WHERE order_id = %s
                    """, (order_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Order item event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_message(self, message):
        try:
            logger.info(f"Processing message: {message}, arrived at {datetime.now()}")
            event = message.value
            topic = message.topic

            if not event or 'payload' not in event or not event['payload']:
                logger.warning("Skipping message due to missing payload")
                return

            payload = event['payload']
            payload['after'] = payload.get('after') or {}
            payload['before'] = payload.get('before') or {}

            if 'users' in topic:
                self.process_user_event(payload)
            elif 'products' in topic:
                self.process_product_event(payload)
            elif 'orders' in topic and 'order_items' not in topic:
                self.process_order_event(payload)
            elif 'order_items' in topic:
                self.process_order_item_event(payload)

            self.safe_commit()
        except Exception as e:
            logger.error(f"Error processing message: {e}, Topic: {message.topic if message else 'Unknown'}")
            raise

    def run(self):
        try:
            self.connect_kafka()
            self.connect_database()
            
            logger.info("Starting analytics consumer...")
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.connection_pool:
                self.connection_pool.closeall()

if __name__ == "__main__":
    consumer = AnalyticsConsumer()
    consumer.run()
