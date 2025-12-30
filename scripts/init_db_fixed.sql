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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_date ON raw_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_state ON raw_transactions(state_names);
CREATE INDEX IF NOT EXISTS idx_segment ON raw_transactions(segment);

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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_segment ON customer_features(customer_segment);
CREATE INDEX IF NOT EXISTS idx_customer_state ON customer_features(state_names);
CREATE INDEX IF NOT EXISTS idx_customer_created_at ON customer_features(created_at);

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
    qoq_revenue_growth DECIMAL(5, 4),
    qoq_transaction_growth DECIMAL(5, 4),
    
    -- Created timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quarterly_year_quarter ON quarterly_features(year, quarter);
CREATE INDEX IF NOT EXISTS idx_quarterly_segment ON quarterly_features(customer_segment);

-- Payment method aggregated features
CREATE TABLE IF NOT EXISTS payment_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_method VARCHAR(50) NOT NULL,
    customer_segment VARCHAR(50),
    
    -- Payment metrics
    total_transactions INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_transaction_amount DECIMAL(10, 2),
    usage_count INTEGER,
    usage_percentage DECIMAL(5, 4),
    
    -- Created timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_payment_method ON payment_features(payment_method);

-- State-level aggregated features
CREATE TABLE IF NOT EXISTS state_features (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state_names VARCHAR(50) NOT NULL,
    
    -- State metrics
    total_transactions INTEGER,
    total_revenue DECIMAL(15, 2),
    avg_transaction_amount DECIMAL(10, 2),
    unique_customers INTEGER,
    
    -- Top performers
    top_segment VARCHAR(50),
    top_payment_method VARCHAR(50),
    
    -- Created timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_state_name ON state_features(state_names);

-- Pipeline execution log table
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_date ON pipeline_execution_log(execution_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_status ON pipeline_execution_log(pipeline_name, status);

-- ML Training Features View
CREATE OR REPLACE VIEW ml_training_features AS
SELECT 
    cf.feature_id,
    cf.customer_segment,
    cf.state_names,
    cf.total_transactions,
    cf.total_amount_spent,
    cf.avg_transaction_amount,
    cf.days_active,
    cf.avg_age,
    cf.referral_rate,
    qf.qoq_revenue_growth,
    pf.usage_percentage as payment_method_diversity,
    sf.total_revenue as state_revenue
FROM customer_features cf
LEFT JOIN quarterly_features qf ON cf.state_names = qf.state_names
LEFT JOIN payment_features pf ON cf.customer_segment = pf.customer_segment
LEFT JOIN state_features sf ON cf.state_names = sf.state_names;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataeng;
GRANT USAGE ON SCHEMA public TO dataeng;
