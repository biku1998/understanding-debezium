import pandas as pd
from sqlalchemy import create_engine, text
import os
import time

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

# --- Load & insert each CSV ---

print("Loading geolocation data...")
df_geo = pd.read_csv('./olist_geolocation_dataset.csv')
df_geo.to_sql('geolocation', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_geo)} geolocation records")

print("Loading customers data...")
df_customers = pd.read_csv('./olist_customers_dataset.csv')
df_customers.to_sql('customers', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_customers)} customer records")

print("Loading sellers data...")
df_sellers = pd.read_csv('./olist_sellers_dataset.csv')
df_sellers.to_sql('sellers', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_sellers)} seller records")

print("Loading product category translation data...")
df_cat_translation = pd.read_csv('./product_category_name_translation.csv')
df_cat_translation.to_sql('product_category_name_translation', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_cat_translation)} category translation records")

print("Loading products data...")
df_products = pd.read_csv('./olist_products_dataset.csv')
df_products = df_products.merge(df_cat_translation, on='product_category_name', how='left')
# Drop the product_category_name_english column as it doesn't exist in the database table
df_products = df_products.drop(columns=['product_category_name_english'])
df_products.to_sql('products', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_products)} product records")

print("Loading orders data...")
df_orders = pd.read_csv('./olist_orders_dataset.csv', parse_dates=[
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
])
df_orders.to_sql('orders', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_orders)} order records")

print("Loading order items data...")
df_order_items = pd.read_csv('./olist_order_items_dataset.csv', parse_dates=['shipping_limit_date'])
df_order_items.to_sql('order_items', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_order_items)} order item records")

print("Loading order reviews data...")
df_reviews = pd.read_csv('./olist_order_reviews_dataset.csv', parse_dates=[
    'review_creation_date',
    'review_answer_timestamp'
])
# Drop duplicate review_id rows, keeping the first occurrence
df_reviews = df_reviews.drop_duplicates(subset=['review_id'], keep='first')
df_reviews.to_sql('order_reviews', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_reviews)} review records")

print("Loading order payments data...")
df_payments = pd.read_csv('./olist_order_payments_dataset.csv')
df_payments.to_sql('order_payments', engine, index=False, if_exists='append')
print(f"  Loaded {len(df_payments)} payment records")

print("✅ Data loading complete!")
