import json
import logging
import os
import psycopg2
import base64
import struct
import time
from datetime import datetime, date
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

    def parse_date(self, date_str):
        """Parse date string to date object"""
        try:
            if isinstance(date_str, str):
                return datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
            elif isinstance(date_str, datetime):
                return date_str.date()
            elif isinstance(date_str, date):
                return date_str
            return None
        except Exception as e:
            logger.warning(f"Failed to parse date {date_str}: {e}")
            return None

    def get_time_dimension(self, date_obj):
        """Get time dimension values from date"""
        if not date_obj:
            return None, None, None, None, None, None
        
        try:
            year = date_obj.year
            quarter = (date_obj.month - 1) // 3 + 1
            month = date_obj.month
            week = date_obj.isocalendar()[1]
            day = date_obj.day
            weekday = date_obj.strftime('%A')
            return year, quarter, month, week, day, weekday
        except Exception as e:
            logger.warning(f"Failed to get time dimension for {date_obj}: {e}")
            return None, None, None, None, None, None

    def connect_kafka(self, retries=5, delay=5):
        for attempt in range(retries):
            try:
                self.consumer = KafkaConsumer(
                    'ecommerce.public.customers',
                    'ecommerce.public.sellers', 
                    'ecommerce.public.products',
                    'ecommerce.public.orders',
                    'ecommerce.public.order_items',
                    'ecommerce.public.order_reviews',
                    'ecommerce.public.order_payments',
                    'ecommerce.public.product_category_name_translation',
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

    def process_customer_event(self, event):
        """Process customer events to update dim_customers"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    customer_id = self.get_safe_value(after_data, 'customer_id')
                    customer_unique_id = self.get_safe_value(after_data, 'customer_unique_id')
                    city = self.get_safe_value(after_data, 'customer_city')
                    state = self.get_safe_value(after_data, 'customer_state')
                    
                    if not customer_id:
                        logger.warning("Skipping customer create event: missing customer_id")
                        return
                    
                    cursor.execute("""
                        INSERT INTO dim_customers (customer_id, customer_unique_id, city, state)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (customer_id) DO UPDATE SET
                            customer_unique_id = EXCLUDED.customer_unique_id,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state
                    """, (customer_id, customer_unique_id, city, state))
                    
                elif event['op'] == 'u':
                    customer_id = self.get_safe_value(after_data, 'customer_id')
                    customer_unique_id = self.get_safe_value(after_data, 'customer_unique_id')
                    city = self.get_safe_value(after_data, 'customer_city')
                    state = self.get_safe_value(after_data, 'customer_state')
                    
                    if not customer_id:
                        logger.warning("Skipping customer update event: missing customer_id")
                        return
                    
                    cursor.execute("""
                        UPDATE dim_customers
                        SET customer_unique_id = %s, city = %s, state = %s
                        WHERE customer_id = %s
                    """, (customer_unique_id, city, state, customer_id))
                    
                elif event['op'] == 'd':
                    customer_id = self.get_safe_value(before_data, 'customer_id')
                    if not customer_id:
                        logger.warning("Skipping customer delete event: missing customer_id")
                        return
                    
                    cursor.execute("DELETE FROM dim_customers WHERE customer_id = %s", (customer_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Customer event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_seller_event(self, event):
        """Process seller events to update dim_sellers"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    seller_id = self.get_safe_value(after_data, 'seller_id')
                    city = self.get_safe_value(after_data, 'seller_city')
                    state = self.get_safe_value(after_data, 'seller_state')
                    
                    if not seller_id:
                        logger.warning("Skipping seller create event: missing seller_id")
                        return
                    
                    cursor.execute("""
                        INSERT INTO dim_sellers (seller_id, city, state)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (seller_id) DO UPDATE SET
                            city = EXCLUDED.city,
                            state = EXCLUDED.state
                    """, (seller_id, city, state))
                    
                elif event['op'] == 'u':
                    seller_id = self.get_safe_value(after_data, 'seller_id')
                    city = self.get_safe_value(after_data, 'seller_city')
                    state = self.get_safe_value(after_data, 'seller_state')
                    
                    if not seller_id:
                        logger.warning("Skipping seller update event: missing seller_id")
                        return
                    
                    cursor.execute("""
                        UPDATE dim_sellers
                        SET city = %s, state = %s
                        WHERE seller_id = %s
                    """, (city, state, seller_id))
                    
                elif event['op'] == 'd':
                    seller_id = self.get_safe_value(before_data, 'seller_id')
                    if not seller_id:
                        logger.warning("Skipping seller delete event: missing seller_id")
                        return
                    
                    cursor.execute("DELETE FROM dim_sellers WHERE seller_id = %s", (seller_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Seller event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_product_event(self, event):
        """Process product events to update dim_products"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    product_id = self.get_safe_value(after_data, 'product_id')
                    category_name = self.get_safe_value(after_data, 'product_category_name')
                    name_length = self.get_safe_value(after_data, 'product_name_length')
                    description_length = self.get_safe_value(after_data, 'product_description_length')
                    photos_qty = self.get_safe_value(after_data, 'product_photos_qty')
                    weight_g = self.get_safe_value(after_data, 'product_weight_g')
                    length_cm = self.get_safe_value(after_data, 'product_length_cm')
                    height_cm = self.get_safe_value(after_data, 'product_height_cm')
                    width_cm = self.get_safe_value(after_data, 'product_width_cm')
                    
                    if not product_id:
                        logger.warning("Skipping product create event: missing product_id")
                        return
                    
                    # Get English category name from translation table
                    category_name_english = None
                    if category_name:
                        cursor.execute("""
                            SELECT product_category_name_english 
                            FROM product_category_name_translation 
                            WHERE product_category_name = %s
                        """, (category_name,))
                        result = cursor.fetchone()
                        if result:
                            category_name_english = result['product_category_name_english']
                    
                    cursor.execute("""
                        INSERT INTO dim_products (
                            product_id, category_name, category_name_english, name_length,
                            description_length, photos_qty, weight_g, length_cm, height_cm, width_cm
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (product_id) DO UPDATE SET
                            category_name = EXCLUDED.category_name,
                            category_name_english = EXCLUDED.category_name_english,
                            name_length = EXCLUDED.name_length,
                            description_length = EXCLUDED.description_length,
                            photos_qty = EXCLUDED.photos_qty,
                            weight_g = EXCLUDED.weight_g,
                            length_cm = EXCLUDED.length_cm,
                            height_cm = EXCLUDED.height_cm,
                            width_cm = EXCLUDED.width_cm
                    """, (product_id, category_name, category_name_english, name_length,
                          description_length, photos_qty, weight_g, length_cm, height_cm, width_cm))
                    
                elif event['op'] == 'u':
                    product_id = self.get_safe_value(after_data, 'product_id')
                    category_name = self.get_safe_value(after_data, 'product_category_name')
                    name_length = self.get_safe_value(after_data, 'product_name_length')
                    description_length = self.get_safe_value(after_data, 'product_description_length')
                    photos_qty = self.get_safe_value(after_data, 'product_photos_qty')
                    weight_g = self.get_safe_value(after_data, 'product_weight_g')
                    length_cm = self.get_safe_value(after_data, 'product_length_cm')
                    height_cm = self.get_safe_value(after_data, 'product_height_cm')
                    width_cm = self.get_safe_value(after_data, 'product_width_cm')
                    
                    if not product_id:
                        logger.warning("Skipping product update event: missing product_id")
                        return
                    
                    # Get English category name from translation table
                    category_name_english = None
                    if category_name:
                        cursor.execute("""
                            SELECT product_category_name_english 
                            FROM product_category_name_translation 
                            WHERE product_category_name = %s
                        """, (category_name,))
                        result = cursor.fetchone()
                        if result:
                            category_name_english = result['product_category_name_english']
                    
                    cursor.execute("""
                        UPDATE dim_products
                        SET category_name = %s, category_name_english = %s, name_length = %s,
                            description_length = %s, photos_qty = %s, weight_g = %s,
                            length_cm = %s, height_cm = %s, width_cm = %s
                        WHERE product_id = %s
                    """, (category_name, category_name_english, name_length, description_length,
                          photos_qty, weight_g, length_cm, height_cm, width_cm, product_id))
                    
                elif event['op'] == 'd':
                    product_id = self.get_safe_value(before_data, 'product_id')
                    if not product_id:
                        logger.warning("Skipping product delete event: missing product_id")
                        return
                    
                    cursor.execute("DELETE FROM dim_products WHERE product_id = %s", (product_id,))
                
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
        """Process order events to update fact_orders and dim_time"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    customer_id = self.get_safe_value(after_data, 'customer_id')
                    order_status = self.get_safe_value(after_data, 'order_status')
                    order_purchase_timestamp = self.get_safe_value(after_data, 'order_purchase_timestamp')
                    order_delivered_customer_date = self.get_safe_value(after_data, 'order_delivered_customer_date')
                    order_estimated_delivery_date = self.get_safe_value(after_data, 'order_estimated_delivery_date')
                    
                    if not order_id:
                        logger.warning("Skipping order create event: missing order_id")
                        return
                    
                    # Parse dates
                    order_purchase_date = self.parse_date(order_purchase_timestamp)
                    delivery_date = self.parse_date(order_delivered_customer_date)
                    estimated_delivery_date = self.parse_date(order_estimated_delivery_date)
                    
                    # Insert time dimension for order purchase date
                    if order_purchase_date:
                        year, quarter, month, week, day, weekday = self.get_time_dimension(order_purchase_date)
                        cursor.execute("""
                            INSERT INTO dim_time (date, year, quarter, month, week, day, weekday)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (date) DO NOTHING
                        """, (order_purchase_date, year, quarter, month, week, day, weekday))
                    
                    # Insert fact record (will be updated with order_items data later)
                    cursor.execute("""
                        INSERT INTO fact_orders (
                            order_id, customer_id, order_status, order_purchase_date,
                            delivery_date, estimated_delivery_date
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO UPDATE SET
                            customer_id = EXCLUDED.customer_id,
                            order_status = EXCLUDED.order_status,
                            order_purchase_date = EXCLUDED.order_purchase_date,
                            delivery_date = EXCLUDED.delivery_date,
                            estimated_delivery_date = EXCLUDED.estimated_delivery_date
                    """, (order_id, customer_id, order_status, order_purchase_date,
                          delivery_date, estimated_delivery_date))
                    
                elif event['op'] == 'u':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    customer_id = self.get_safe_value(after_data, 'customer_id')
                    order_status = self.get_safe_value(after_data, 'order_status')
                    order_purchase_timestamp = self.get_safe_value(after_data, 'order_purchase_timestamp')
                    order_delivered_customer_date = self.get_safe_value(after_data, 'order_delivered_customer_date')
                    order_estimated_delivery_date = self.get_safe_value(after_data, 'order_estimated_delivery_date')
                    
                    if not order_id:
                        logger.warning("Skipping order update event: missing order_id")
                        return
                    
                    # Parse dates
                    order_purchase_date = self.parse_date(order_purchase_timestamp)
                    delivery_date = self.parse_date(order_delivered_customer_date)
                    estimated_delivery_date = self.parse_date(order_estimated_delivery_date)
                    
                    # Insert time dimension for order purchase date
                    if order_purchase_date:
                        year, quarter, month, week, day, weekday = self.get_time_dimension(order_purchase_date)
                        cursor.execute("""
                            INSERT INTO dim_time (date, year, quarter, month, week, day, weekday)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (date) DO NOTHING
                        """, (order_purchase_date, year, quarter, month, week, day, weekday))
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET customer_id = %s, order_status = %s, order_purchase_date = %s,
                            delivery_date = %s, estimated_delivery_date = %s
                        WHERE order_id = %s
                    """, (customer_id, order_status, order_purchase_date,
                          delivery_date, estimated_delivery_date, order_id))
                    
                elif event['op'] == 'd':
                    order_id = self.get_safe_value(before_data, 'order_id')
                    if not order_id:
                        logger.warning("Skipping order delete event: missing order_id")
                        return
                    
                    cursor.execute("DELETE FROM fact_orders WHERE order_id = %s", (order_id,))
                
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
        """Process order item events to update fact_orders with product and seller info"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    order_item_id = self.get_safe_value(after_data, 'order_item_id')
                    product_id = self.get_safe_value(after_data, 'product_id')
                    seller_id = self.get_safe_value(after_data, 'seller_id')
                    price = self.decode_decimal(self.get_safe_value(after_data, 'price'))
                    freight_value = self.decode_decimal(self.get_safe_value(after_data, 'freight_value'))
                    
                    if not order_id or not product_id or not seller_id:
                        logger.warning("Skipping order item create event: missing required fields")
                        return
                    
                    # Update fact_orders with product and seller info
                    cursor.execute("""
                        UPDATE fact_orders
                        SET product_id = %s, seller_id = %s, order_item_id = %s,
                            price = %s, freight_value = %s
                        WHERE order_id = %s
                    """, (product_id, seller_id, order_item_id, price, freight_value, order_id))
                    
                elif event['op'] == 'u':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    order_item_id = self.get_safe_value(after_data, 'order_item_id')
                    product_id = self.get_safe_value(after_data, 'product_id')
                    seller_id = self.get_safe_value(after_data, 'seller_id')
                    price = self.decode_decimal(self.get_safe_value(after_data, 'price'))
                    freight_value = self.decode_decimal(self.get_safe_value(after_data, 'freight_value'))
                    
                    if not order_id or not product_id or not seller_id:
                        logger.warning("Skipping order item update event: missing required fields")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET product_id = %s, seller_id = %s, order_item_id = %s,
                            price = %s, freight_value = %s
                        WHERE order_id = %s
                    """, (product_id, seller_id, order_item_id, price, freight_value, order_id))
                    
                elif event['op'] == 'd':
                    order_id = self.get_safe_value(before_data, 'order_id')
                    if not order_id:
                        logger.warning("Skipping order item delete event: missing order_id")
                        return
                    
                    # Clear product and seller info from fact_orders
                    cursor.execute("""
                        UPDATE fact_orders
                        SET product_id = NULL, seller_id = NULL, order_item_id = NULL,
                            price = NULL, freight_value = NULL
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

    def process_order_review_event(self, event):
        """Process order review events to update fact_orders with review score"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    review_score = self.get_safe_value(after_data, 'review_score')
                    
                    if not order_id:
                        logger.warning("Skipping order review create event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET review_score = %s
                        WHERE order_id = %s
                    """, (review_score, order_id))
                    
                elif event['op'] == 'u':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    review_score = self.get_safe_value(after_data, 'review_score')
                    
                    if not order_id:
                        logger.warning("Skipping order review update event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET review_score = %s
                        WHERE order_id = %s
                    """, (review_score, order_id))
                    
                elif event['op'] == 'd':
                    order_id = self.get_safe_value(before_data, 'order_id')
                    if not order_id:
                        logger.warning("Skipping order review delete event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET review_score = NULL
                        WHERE order_id = %s
                    """, (order_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Order review event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_order_payment_event(self, event):
        """Process order payment events to update fact_orders with payment info"""
        if not event or 'op' not in event:
            return
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                after_data = event.get('after', {}) or {}
                before_data = event.get('before', {}) or {}
                
                if event['op'] == 'c':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    payment_type = self.get_safe_value(after_data, 'payment_type')
                    payment_installments = self.get_safe_value(after_data, 'payment_installments')
                    payment_value = self.decode_decimal(self.get_safe_value(after_data, 'payment_value'))
                    
                    if not order_id:
                        logger.warning("Skipping order payment create event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET payment_type = %s, payment_installments = %s, payment_value = %s
                        WHERE order_id = %s
                    """, (payment_type, payment_installments, payment_value, order_id))
                    
                elif event['op'] == 'u':
                    order_id = self.get_safe_value(after_data, 'order_id')
                    payment_type = self.get_safe_value(after_data, 'payment_type')
                    payment_installments = self.get_safe_value(after_data, 'payment_installments')
                    payment_value = self.decode_decimal(self.get_safe_value(after_data, 'payment_value'))
                    
                    if not order_id:
                        logger.warning("Skipping order payment update event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET payment_type = %s, payment_installments = %s, payment_value = %s
                        WHERE order_id = %s
                    """, (payment_type, payment_installments, payment_value, order_id))
                    
                elif event['op'] == 'd':
                    order_id = self.get_safe_value(before_data, 'order_id')
                    if not order_id:
                        logger.warning("Skipping order payment delete event: missing order_id")
                        return
                    
                    cursor.execute("""
                        UPDATE fact_orders
                        SET payment_type = NULL, payment_installments = NULL, payment_value = NULL
                        WHERE order_id = %s
                    """, (order_id,))
                
                conn.commit()
        except Exception as e:
            logger.error(f"Order payment event error: {e}, Event: {event}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_db_connection(conn)

    def process_message(self, message):
        try:
            logger.info(f"Processing message from topic: {message.topic}")
            event = message.value
            topic = message.topic

            if not event or 'payload' not in event or not event['payload']:
                logger.warning("Skipping message due to missing payload")
                return

            payload = event['payload']
            payload['after'] = payload.get('after') or {}
            payload['before'] = payload.get('before') or {}

            if 'customers' in topic:
                self.process_customer_event(payload)
            elif 'sellers' in topic:
                self.process_seller_event(payload)
            elif 'products' in topic and 'product_category_name_translation' not in topic:
                self.process_product_event(payload)
            elif 'orders' in topic and 'order_items' not in topic and 'order_reviews' not in topic and 'order_payments' not in topic:
                self.process_order_event(payload)
            elif 'order_items' in topic:
                self.process_order_item_event(payload)
            elif 'order_reviews' in topic:
                self.process_order_review_event(payload)
            elif 'order_payments' in topic:
                self.process_order_payment_event(payload)

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
