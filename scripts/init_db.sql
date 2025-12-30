-- Database initialization script for Feature Store
-- Creates tables for storing customer transaction features

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw transactions table (mirror of CSV data)
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    gender VARCHAR(10),
    age INTEGER,
    marital_status VARCHAR(20),
    state_names VARCHAR(50),
    segment VARCHAR(50),
    employees_status VARCHAR(50),
    payment_method VARCHAR(50),
    referral VARCHAR(10),
    amount_spent DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_state (state_names),
    INDEX idx_segment (segment)
);

-- Customer aggregated features table
CREATE TABLE IF NOT EXISTS customer_features (
    feature_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_segment VARCHAR(50),
    state_names VARCHAR(50),
    
    -- Transaction metrics
    total_transactions INTEGER,
    total_amount_spent DECIMAL(15, 2),
    avg_transaction_amount DECIMAL(10, 2),
    min_transaction_amount DECIMAL(10, 2),
    max_transaction_amount DECIMAL(10, 2),
    std_transaction_amount DECIMAL(10, 2),
    
    -- Time-based metrics
    first_transaction_date DATE,
    last_transaction_date DATE,
    days_active INTEGER,
    avg_days_between_transactions DECIMAL(10, 2),
    
    -- Demographic metrics
    avg_age DECIMAL(5, 2),
    gender_distribution JSONB,
    marital_status_distribution JSONB,
    
    -- Behavioral metrics
    payment_method_distribution JSONB,
    referral_rate DECIMAL(5, 4),
    employment_status_distribution JSONB,
    
    -- Feature metadata
    feature_version VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_segment (customer_segment),
    INDEX idx_state_features (state_names),
    INDEX idx_created_at (created_at)
);

-- Time-series aggregated features (quarterly)
CREATE TABLE IF NOT EXISTS quarterly_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    customer_segment VARCHAR(50),
    state_names VARCHAR(50),
    
    -- Quarterly metrics
    quarterly_transactions INTEGER,
    quarterly_revenue DECIMAL(15, 2),
    avg_quarterly_transaction DECIMAL(10, 2),
    unique_customers INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    
    -- Growth metrics
    revenue_growth_rate DECIMAL(10, 4),
    transaction_growth_rate DECIMAL(10, 4),
    customer_retention_rate DECIMAL(10, 4),
    
    -- Feature metadata
    feature_version VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(year, quarter, customer_segment, state_names),
    INDEX idx_quarter (year, quarter),
    INDEX idx_segment_quarter (customer_segment, year, quarter)
);

-- Payment method features
CREATE TABLE IF NOT EXISTS payment_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_method VARCHAR(50) NOT NULL,
    customer_segment VARCHAR(50),
    
    -- Payment metrics
    total_transactions INTEGER,
    total_amount DECIMAL(15, 2),
    avg_transaction_amount DECIMAL(10, 2),
    usage_percentage DECIMAL(5, 4),
    
    -- Feature metadata
    feature_version VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_payment_method (payment_method),
    INDEX idx_segment_payment (customer_segment, payment_method)
);

-- State-level geographic features
CREATE TABLE IF NOT EXISTS state_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state_names VARCHAR(50) NOT NULL,
    
    -- State metrics
    total_customers INTEGER,
    total_transactions INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_customer_value DECIMAL(10, 2),
    avg_transaction_size DECIMAL(10, 2),
    
    -- Demographics
    avg_customer_age DECIMAL(5, 2),
    top_segment VARCHAR(50),
    top_payment_method VARCHAR(50),
    
    -- Feature metadata
    feature_version VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(state_names, feature_version),
    INDEX idx_state_revenue (state_names, total_revenue DESC)
);

-- Data pipeline execution log
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- 'started', 'completed', 'failed'
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    execution_time_seconds INTEGER,
    error_message TEXT,
    metadata JSONB,
    
    INDEX idx_pipeline_name (pipeline_name),
    INDEX idx_execution_date (execution_date DESC),
    INDEX idx_status (status)
);

-- Create view for ML model training
CREATE OR REPLACE VIEW ml_training_features AS
SELECT 
    cf.customer_segment,
    cf.state_names,
    cf.total_transactions,
    cf.total_amount_spent,
    cf.avg_transaction_amount,
    cf.std_transaction_amount,
    cf.days_active,
    cf.avg_days_between_transactions,
    cf.avg_age,
    cf.referral_rate,
    sf.total_revenue as state_total_revenue,
    sf.avg_customer_value as state_avg_customer_value,
    qf.revenue_growth_rate as quarterly_growth_rate,
    qf.customer_retention_rate as retention_rate
FROM customer_features cf
LEFT JOIN state_features sf ON cf.state_names = sf.state_names
LEFT JOIN quarterly_features qf ON cf.customer_segment = qf.customer_segment 
    AND cf.state_names = qf.state_names
WHERE cf.feature_version = 'v1'
ORDER BY cf.total_amount_spent DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataeng;

-- Insert initial pipeline log entry
INSERT INTO pipeline_execution_log (pipeline_name, status, metadata)
VALUES ('database_initialization', 'completed', '{"message": "Database schema created successfully"}');
