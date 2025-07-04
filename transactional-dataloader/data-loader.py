import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import random

# --- Database connection config ---
DB_USER = os.getenv('TRANSACTIONAL_DB_USER', 'postgres')
DB_PASS = os.getenv('TRANSACTIONAL_DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('TRANSACTIONAL_DB_HOST', 'localhost')
DB_PORT = os.getenv('TRANSACTIONAL_DB_PORT', '5432')
DB_NAME = os.getenv('TRANSACTIONAL_DB_NAME', 'ecommerce')

# --- Create SQLAlchemy engine ---
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Sleep for 10 seconds to ensure the database is ready
time.sleep(20)

# --- Clear existing data first ---
print("Clearing existing data...")
with engine.connect() as conn:
    # Truncate tables in reverse dependency order to avoid foreign key constraint violations
    tables_to_truncate = [
        'order_payments',
        'order_reviews', 
        'order_items',
        'orders',
        'products',
        'sellers',
        'customers',
        'geolocation',
        'product_category_name_translation'
    ]
    try:
        conn.execute(text('TRUNCATE ' + ', '.join(tables_to_truncate) + ' CASCADE'))
        print(f"  Truncated tables: {', '.join(tables_to_truncate)}")
    except Exception as e:
        print(f"  Warning: Could not truncate tables: {e}")
    conn.commit()

# --- Load static data first (one-time load) ---

print("Loading geolocation data...")
df_geo = pd.read_csv('./e-commerce-data/olist_geolocation_dataset.csv')
df_geo.to_sql('geolocation', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_geo)} geolocation records")

print("Loading customers data...")
df_customers = pd.read_csv('./e-commerce-data/olist_customers_dataset.csv')
df_customers.to_sql('customers', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_customers)} customer records")

print("Loading sellers data...")
df_sellers = pd.read_csv('./e-commerce-data/olist_sellers_dataset.csv')
df_sellers.to_sql('sellers', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_sellers)} seller records")

print("Loading product category translation data...")
df_cat_translation = pd.read_csv('./e-commerce-data/product_category_name_translation.csv')
df_cat_translation.to_sql('product_category_name_translation', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_cat_translation)} category translation records")

print("Loading products data...")
df_products = pd.read_csv('./e-commerce-data/olist_products_dataset.csv')
df_products = df_products.merge(df_cat_translation, on='product_category_name', how='left')
# Drop the product_category_name_english column as it doesn't exist in the database table
df_products = df_products.drop(columns=['product_category_name_english'])
df_products.to_sql('products', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_products)} product records")

# --- Load dynamic data in batches ---

print("Loading orders and related data in batches...")

# Load all the dynamic data into DataFrames first
df_orders = pd.read_csv('./e-commerce-data/olist_orders_dataset.csv', parse_dates=[
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
])

df_order_items = pd.read_csv('./e-commerce-data/olist_order_items_dataset.csv', parse_dates=['shipping_limit_date'])

df_reviews = pd.read_csv('./e-commerce-data/olist_order_reviews_dataset.csv', parse_dates=[
    'review_creation_date',
    'review_answer_timestamp'
])
# Drop duplicate review_id rows, keeping the first occurrence
df_reviews = df_reviews.drop_duplicates(subset=['review_id'], keep='first')

df_payments = pd.read_csv('./e-commerce-data/olist_order_payments_dataset.csv')

# Get all unique order IDs
all_order_ids = df_orders['order_id'].unique()
total_orders = len(all_order_ids)
print(f"  Total orders to process: {total_orders}")

# Configuration for batch loading
BATCH_SIZE_MIN = 100
BATCH_SIZE_MAX = 200
DELAY_SECONDS = 5

# Process orders in batches
processed_orders = 0
batch_number = 1

while processed_orders < total_orders:
    # Determine batch size for this iteration
    remaining_orders = total_orders - processed_orders
    current_batch_size = min(
        random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX),
        remaining_orders
    )
    
    # Get order IDs for this batch
    start_idx = processed_orders
    end_idx = processed_orders + current_batch_size
    batch_order_ids = all_order_ids[start_idx:end_idx]
    
    print(f"\n--- Batch {batch_number} ---")
    print(f"Processing orders {start_idx + 1} to {end_idx} (batch size: {current_batch_size})")
    
    # Filter data for this batch
    batch_orders = df_orders[df_orders['order_id'].isin(batch_order_ids)]
    batch_order_items = df_order_items[df_order_items['order_id'].isin(batch_order_ids)]
    batch_reviews = df_reviews[df_reviews['order_id'].isin(batch_order_ids)]
    batch_payments = df_payments[df_payments['order_id'].isin(batch_order_ids)]
    
    # Insert batch data
    batch_orders.to_sql('orders', engine, index=False, if_exists='append')
    print(f"  Loaded {len(batch_orders)} orders")
    
    batch_order_items.to_sql('order_items', engine, index=False, if_exists='append')
    print(f"  Loaded {len(batch_order_items)} order items")
    
    batch_reviews.to_sql('order_reviews', engine, index=False, if_exists='append')
    print(f"  Loaded {len(batch_reviews)} reviews")
    
    batch_payments.to_sql('order_payments', engine, index=False, if_exists='append')
    print(f"  Loaded {len(batch_payments)} payments")
    
    processed_orders += current_batch_size
    batch_number += 1
    
    # Progress update
    progress_percent = (processed_orders / total_orders) * 100
    print(f"  Progress: {processed_orders}/{total_orders} orders ({progress_percent:.1f}%)")
    
    # Wait before next batch (except for the last batch)
    if processed_orders < total_orders:
        print(f"  Waiting {DELAY_SECONDS} seconds before next batch...")
        time.sleep(DELAY_SECONDS)

print("\n✅ Data loading complete!")
print(f"Total batches processed: {batch_number - 1}")
print(f"Total orders loaded: {processed_orders}")
