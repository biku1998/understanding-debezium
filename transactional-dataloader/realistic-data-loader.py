import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import random
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealisticDataLoader:
    def __init__(self, speed_multiplier: float = 1.0):
        """
        Initialize the realistic data loader
        
        Args:
            speed_multiplier: Speed up time (1.0 = real time, 10.0 = 10x faster)
        """
        self.speed_multiplier = speed_multiplier
        
        # Database connection config
        self.db_user = os.getenv('TRANSACTIONAL_DB_USER', 'postgres')
        self.db_pass = os.getenv('TRANSACTIONAL_DB_PASSWORD', 'postgres')
        self.db_host = os.getenv('TRANSACTIONAL_DB_HOST', 'localhost')
        self.db_port = os.getenv('TRANSACTIONAL_DB_PORT', '5432')
        self.db_name = os.getenv('TRANSACTIONAL_DB_NAME', 'ecommerce')
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f'postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}'
        )
        
        # Data storage
        self.data = {}
        self.stats = {
            'customers_loaded': 0,
            'sellers_loaded': 0,
            'products_loaded': 0,
            'orders_created': 0,
            'order_items_created': 0,
            'payments_created': 0,
            'reviews_created': 0,
            'start_time': None,
            'end_time': None
        }

    def sleep(self, seconds: float):
        """Sleep for the specified time, adjusted by speed multiplier"""
        adjusted_time = seconds / self.speed_multiplier
        time.sleep(adjusted_time)
        logger.info(f"Slept for {adjusted_time:.2f}s (real time: {seconds:.2f}s)")

    def load_reference_data(self):
        """Load all reference data (customers, sellers, products, geolocation)"""
        logger.info("=== PHASE 1: Loading Reference Data ===")
        
        # Load geolocation data
        logger.info("Loading geolocation data...")
        df_geo = pd.read_csv('./e-commerce-data/olist_geolocation_dataset.csv')
        df_geo.to_sql('geolocation', self.engine, index=False, if_exists='append')
        logger.info(f"  Loaded {len(df_geo)} geolocation records")
        
        # Load customers
        logger.info("Loading customers data...")
        df_customers = pd.read_csv('./e-commerce-data/olist_customers_dataset.csv')
        df_customers.to_sql('customers', self.engine, index=False, if_exists='append')
        self.data['customers'] = df_customers
        self.stats['customers_loaded'] = len(df_customers)
        logger.info(f"  Loaded {len(df_customers)} customer records")
        
        # Load sellers
        logger.info("Loading sellers data...")
        df_sellers = pd.read_csv('./e-commerce-data/olist_sellers_dataset.csv')
        df_sellers.to_sql('sellers', self.engine, index=False, if_exists='append')
        self.data['sellers'] = df_sellers
        self.stats['sellers_loaded'] = len(df_sellers)
        logger.info(f"  Loaded {len(df_sellers)} seller records")
        
        # Load product category translations
        logger.info("Loading product category translations...")
        df_cat_translation = pd.read_csv('./e-commerce-data/product_category_name_translation.csv')
        df_cat_translation.to_sql('product_category_name_translation', self.engine, index=False, if_exists='append')
        self.data['category_translations'] = df_cat_translation
        logger.info(f"  Loaded {len(df_cat_translation)} category translation records")
        
        # Load products
        logger.info("Loading products data...")
        df_products = pd.read_csv('./e-commerce-data/olist_products_dataset.csv')
        df_products = df_products.merge(df_cat_translation, on='product_category_name', how='left')
        df_products = df_products.drop(columns=['product_category_name_english'])
        df_products.to_sql('products', self.engine, index=False, if_exists='append')
        self.data['products'] = df_products
        self.stats['products_loaded'] = len(df_products)
        logger.info(f"  Loaded {len(df_products)} product records")
        
        # Load historical orders for reference
        logger.info("Loading historical orders for reference...")
        df_orders = pd.read_csv('./e-commerce-data/olist_orders_dataset.csv', parse_dates=[
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ])
        self.data['historical_orders'] = df_orders
        
        df_order_items = pd.read_csv('./e-commerce-data/olist_order_items_dataset.csv', parse_dates=['shipping_limit_date'])
        self.data['historical_order_items'] = df_order_items
        
        df_payments = pd.read_csv('./e-commerce-data/olist_order_payments_dataset.csv')
        self.data['historical_payments'] = df_payments
        
        df_reviews = pd.read_csv('./e-commerce-data/olist_order_reviews_dataset.csv', parse_dates=[
            'review_creation_date',
            'review_answer_timestamp'
        ])
        df_reviews = df_reviews.drop_duplicates(subset=['review_id'], keep='first')
        self.data['historical_reviews'] = df_reviews
        
        logger.info("Reference data loading complete!")

    def simulate_realistic_orders(self, duration_hours: float = 2.0):
        """
        Simulate realistic order creation over time
        
        Args:
            duration_hours: How long to run the simulation
        """
        logger.info(f"=== PHASE 2: Simulating Orders for {duration_hours} hours ===")
        
        self.stats['start_time'] = datetime.now()
        
        # Calculate simulation parameters
        total_seconds = duration_hours * 3600
        orders_per_hour = 50  # Adjust based on your needs
        total_orders = int(orders_per_hour * duration_hours)
        interval_between_orders = total_seconds / total_orders
        
        logger.info(f"Will create {total_orders} orders over {duration_hours} hours")
        logger.info(f"Average interval between orders: {interval_between_orders:.2f} seconds")
        
        # Get sample data for realistic order creation
        customers = self.data['customers'].sample(n=min(1000, len(self.data['customers'])))
        products = self.data['products'].sample(n=min(500, len(self.data['products'])))
        sellers = self.data['sellers'].sample(n=min(200, len(self.data['sellers'])))
        
        # Create orders with realistic timing
        for i in range(total_orders):
            try:
                # Create order
                order = self._create_realistic_order(customers, i)
                self._insert_order(order)
                self.stats['orders_created'] += 1
                
                # Create order items (1-3 items per order)
                num_items = random.randint(1, 3)
                total_payment_value = 0
                
                for j in range(num_items):
                    order_item = self._create_realistic_order_item(order['order_id'], products, sellers, j)
                    self._insert_order_item(order_item)
                    self.stats['order_items_created'] += 1
                    total_payment_value += order_item['price'] + order_item['freight_value']
                
                # Create single payment for the entire order (not per item)
                payment = self._create_realistic_payment(order['order_id'], total_payment_value)
                self._insert_payment(payment)
                self.stats['payments_created'] += 1
                
                # Simulate review after delivery (70% of orders get reviewed)
                if random.random() < 0.7:
                    # Review comes 1-7 days after delivery
                    review_delay = random.randint(1, 7) * 24 * 3600  # seconds
                    self.sleep(review_delay / self.speed_multiplier)
                    
                    review = self._create_realistic_review(order['order_id'])
                    self._insert_review(review)
                    self.stats['reviews_created'] += 1
                
                # Log progress
                if (i + 1) % 10 == 0:
                    logger.info(f"Created {i + 1}/{total_orders} orders")
                
                # Sleep between orders (with some randomness)
                sleep_time = interval_between_orders * random.uniform(0.5, 1.5)
                self.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error creating order {i}: {e}")
                continue
        
        self.stats['end_time'] = datetime.now()
        logger.info("Order simulation complete!")

    def _create_realistic_order(self, customers: pd.DataFrame, order_index: int) -> Dict:
        """Create a realistic order"""
        customer = customers.iloc[order_index % len(customers)]
        
        # Simulate realistic order timing
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        purchase_time = base_time + timedelta(minutes=random.randint(0, 60))
        approved_time = purchase_time + timedelta(minutes=random.randint(1, 10))
        delivered_time = approved_time + timedelta(days=random.randint(1, 15))
        
        return {
            'order_id': f'ORDER_{int(datetime.now().timestamp() * 1000)}_{order_index:06d}',
            'customer_id': customer['customer_id'],
            'order_status': random.choice(['delivered', 'shipped', 'processing']),
            'order_purchase_timestamp': purchase_time,
            'order_approved_at': approved_time,
            'order_delivered_carrier_date': delivered_time - timedelta(days=1),
            'order_delivered_customer_date': delivered_time,
            'order_estimated_delivery_date': delivered_time + timedelta(days=random.randint(-2, 3))
        }

    def _create_realistic_order_item(self, order_id: str, products: pd.DataFrame, sellers: pd.DataFrame, item_index: int) -> Dict:
        """Create a realistic order item"""
        product = products.iloc[random.randint(0, len(products) - 1)]
        seller = sellers.iloc[random.randint(0, len(sellers) - 1)]
        
        return {
            'order_id': order_id,
            'order_item_id': item_index + 1,
            'product_id': product['product_id'],
            'seller_id': seller['seller_id'],
            'shipping_limit_date': datetime.now() + timedelta(days=random.randint(1, 7)),
            'price': round(random.uniform(10.0, 500.0), 2),
            'freight_value': round(random.uniform(5.0, 50.0), 2)
        }

    def _create_realistic_payment(self, order_id: str, total_value: float) -> Dict:
        """Create a realistic payment"""
        payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card']
        
        return {
            'order_id': order_id,
            'payment_sequential': 1,
            'payment_type': random.choice(payment_types),
            'payment_installments': random.randint(1, 12),
            'payment_value': total_value
        }

    def _create_realistic_review(self, order_id: str) -> Dict:
        """Create a realistic review"""
        review_score = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.1, 0.15, 0.3, 0.4])[0]
        
        return {
            'review_id': f'REVIEW_{int(datetime.now().timestamp() * 1000)}_{random.randint(100000, 999999)}',
            'order_id': order_id,
            'review_score': review_score,
            'review_comment_title': f'Review for order {order_id}',
            'review_comment_message': f'Customer feedback for order {order_id}',
            'review_creation_date': datetime.now().date(),
            'review_answer_timestamp': datetime.now()
        }

    def _insert_order(self, order: Dict):
        """Insert order into database"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, 
                                  order_approved_at, order_delivered_carrier_date, 
                                  order_delivered_customer_date, order_estimated_delivery_date)
                VALUES (:order_id, :customer_id, :order_status, :order_purchase_timestamp,
                       :order_approved_at, :order_delivered_carrier_date,
                       :order_delivered_customer_date, :order_estimated_delivery_date)
            """), order)
            conn.commit()

    def _insert_order_item(self, order_item: Dict):
        """Insert order item into database"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO order_items (order_id, order_item_id, product_id, seller_id,
                                       shipping_limit_date, price, freight_value)
                VALUES (:order_id, :order_item_id, :product_id, :seller_id,
                       :shipping_limit_date, :price, :freight_value)
            """), order_item)
            conn.commit()

    def _insert_payment(self, payment: Dict):
        """Insert payment into database"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO order_payments (order_id, payment_sequential, payment_type,
                                          payment_installments, payment_value)
                VALUES (:order_id, :payment_sequential, :payment_type,
                       :payment_installments, :payment_value)
            """), payment)
            conn.commit()

    def _insert_review(self, review: Dict):
        """Insert review into database"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_title,
                                         review_comment_message, review_creation_date, review_answer_timestamp)
                VALUES (:review_id, :order_id, :review_score, :review_comment_title,
                       :review_comment_message, :review_creation_date, :review_answer_timestamp)
            """), review)
            conn.commit()

    def clear_existing_data(self):
        """Clear existing data from all tables"""
        logger.info("Clearing existing data...")
        with self.engine.connect() as conn:
            # Clear tables in the correct order (child tables first, then parent tables)
            tables_to_truncate = [
                'order_payments',      # References orders
                'order_reviews',       # References orders
                'order_items',         # References orders, products, sellers
                'orders',              # References customers
                'products',            # No dependencies
                'sellers',             # No dependencies
                'customers',           # No dependencies
                'geolocation',         # No dependencies
                'product_category_name_translation'  # No dependencies
            ]
            try:
                # Use CASCADE to handle foreign key constraints
                conn.execute(text('TRUNCATE ' + ', '.join(tables_to_truncate) + ' CASCADE'))
                logger.info(f"  Truncated tables: {', '.join(tables_to_truncate)}")
                
                # Verify tables are empty
                for table in tables_to_truncate:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM {table}'))
                    count = result.scalar()
                    if count > 0:
                        logger.warning(f"  Warning: Table {table} still has {count} records after truncate")
                    else:
                        logger.info(f"  ✓ Table {table} is empty")
                        
            except Exception as e:
                logger.error(f"  Error truncating tables: {e}")
                # Fallback: delete from tables in reverse order
                logger.info("  Trying fallback delete method...")
                try:
                    for table in tables_to_truncate:
                        conn.execute(text(f'DELETE FROM {table}'))
                        logger.info(f"  Deleted from {table}")
                    conn.commit()
                except Exception as e2:
                    logger.error(f"  Fallback delete also failed: {e2}")
            conn.commit()

    def print_stats(self):
        """Print final statistics"""
        logger.info("=== FINAL STATISTICS ===")
        logger.info(f"Customers loaded: {self.stats['customers_loaded']}")
        logger.info(f"Sellers loaded: {self.stats['sellers_loaded']}")
        logger.info(f"Products loaded: {self.stats['products_loaded']}")
        logger.info(f"Orders created: {self.stats['orders_created']}")
        logger.info(f"Order items created: {self.stats['order_items_created']}")
        logger.info(f"Payments created: {self.stats['payments_created']}")
        logger.info(f"Reviews created: {self.stats['reviews_created']}")
        
        if self.stats['start_time'] and self.stats['end_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
            logger.info(f"Simulation duration: {duration}")
            logger.info(f"Orders per hour: {self.stats['orders_created'] / (duration.total_seconds() / 3600):.2f}")

def main():
    """Main function to run the realistic data loader"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Realistic E-commerce Data Loader')
    parser.add_argument('--speed', type=float, default=10.0, 
                       help='Speed multiplier (1.0 = real time, 10.0 = 10x faster)')
    parser.add_argument('--duration', type=float, default=2.0,
                       help='Duration of order simulation in hours')
    parser.add_argument('--clear', action='store_true',
                       help='Clear existing data before loading')
    
    args = parser.parse_args()
    
    # Wait for database to be ready
    logger.info("Waiting for database to be ready...")
    time.sleep(20)
    
    # Initialize loader
    loader = RealisticDataLoader(speed_multiplier=args.speed)
    
    # Clear data if requested
    if args.clear:
        loader.clear_existing_data()
        # Small delay to ensure database has processed the changes
        time.sleep(2)
    
    # Load reference data
    loader.load_reference_data()
    
    # Simulate realistic orders
    loader.simulate_realistic_orders(duration_hours=args.duration)
    
    # Print final statistics
    loader.print_stats()
    
    logger.info("✅ Realistic data loading complete!")

if __name__ == "__main__":
    main() 