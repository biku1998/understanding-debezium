-- Create analytics database schema
-- Note: Database is already created by POSTGRES_DB environment variable

-- Create analytics tables
CREATE TABLE user_analytics (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(10,2) DEFAULT 0,
    last_order_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE product_analytics (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    name VARCHAR(255),
    total_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(10,2) DEFAULT 0,
    average_price DECIMAL(10,2) DEFAULT 0,
    last_sale_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_analytics (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    user_id INTEGER,
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    items_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_user_analytics_user_id ON user_analytics(user_id);
CREATE INDEX idx_product_analytics_product_id ON product_analytics(product_id);
CREATE INDEX idx_order_analytics_order_id ON order_analytics(order_id);
CREATE INDEX idx_order_analytics_user_id ON order_analytics(user_id);

-- Add unique constraints for ON CONFLICT operations
ALTER TABLE user_analytics ADD CONSTRAINT unique_user_analytics_user_id UNIQUE(user_id);
ALTER TABLE product_analytics ADD CONSTRAINT unique_product_analytics_product_id UNIQUE(product_id);
ALTER TABLE order_analytics ADD CONSTRAINT unique_order_analytics_order_id UNIQUE(order_id); 