-- Create analytics database schema

CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    seller_id VARCHAR(50),
    product_id VARCHAR(50),
    order_item_id INT,
    order_status VARCHAR(50),
    order_purchase_date DATE,
    delivery_date DATE,
    estimated_delivery_date DATE,
    price NUMERIC(10, 2),
    freight_value NUMERIC(10, 2),
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value NUMERIC(10, 2),
    review_score INT
);

CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    city VARCHAR(100),
    state CHAR(2)
);

CREATE TABLE dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    state CHAR(2)
);

CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100),
    category_name_english VARCHAR(100),
    name_length INT,
    description_length INT,
    photos_qty INT,
    weight_g INT,
    length_cm INT,
    height_cm INT,
    width_cm INT
);

CREATE TABLE dim_time (
    date DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    weekday VARCHAR(10)
);

-- Join & filter fields
CREATE INDEX idx_fact_orders_customer_id ON fact_orders(customer_id);
CREATE INDEX idx_fact_orders_seller_id ON fact_orders(seller_id);
CREATE INDEX idx_fact_orders_product_id ON fact_orders(product_id);

-- Common filters
CREATE INDEX idx_fact_orders_order_status ON fact_orders(order_status);
CREATE INDEX idx_fact_orders_purchase_date ON fact_orders(order_purchase_date);
CREATE INDEX idx_fact_orders_review_score ON fact_orders(review_score);

-- For geographic filtering
CREATE INDEX idx_dim_customers_state ON dim_customers(state);
CREATE INDEX idx_dim_customers_city ON dim_customers(city);

-- For seller-level aggregations
CREATE INDEX idx_dim_sellers_state ON dim_sellers(state);
CREATE INDEX idx_dim_sellers_city ON dim_sellers(city);

-- For category analytics
CREATE INDEX idx_dim_products_category_name ON dim_products(category_name);
CREATE INDEX idx_dim_products_category_name_english ON dim_products(category_name_english);

-- For fast time-series aggregations
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_time_weekday ON dim_time(weekday);


-- Add unique constraints for ON CONFLICT operations
ALTER TABLE fact_orders ADD CONSTRAINT unique_fact_orders_order_id UNIQUE(order_id);
ALTER TABLE dim_customers ADD CONSTRAINT unique_dim_customers_customer_id UNIQUE(customer_id);
ALTER TABLE dim_sellers ADD CONSTRAINT unique_dim_sellers_seller_id UNIQUE(seller_id);
ALTER TABLE dim_products ADD CONSTRAINT unique_dim_products_product_id UNIQUE(product_id);
ALTER TABLE dim_time ADD CONSTRAINT unique_dim_time_date UNIQUE(date);