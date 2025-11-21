-- Supabase Tables for Cooling The Cloud Production System
-- Run this in your Supabase SQL Editor

-- 1. Weather Data Table
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    station VARCHAR(10),
    timestamp TIMESTAMP NOT NULL,
    temperature_f DECIMAL(5,2),
    humidity_percent DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data(DATE(timestamp));

-- 2. Electricity Prices Table
CREATE TABLE IF NOT EXISTS electricity_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    hour INTEGER CHECK (hour >= 0 AND hour < 24),
    price_per_mwh DECIMAL(10,2),
    rate_type VARCHAR(20), -- 'peak', 'off-peak', 'super-off-peak'
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON electricity_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_prices_date ON electricity_prices(DATE(timestamp));

-- 3. Water Prices Table
CREATE TABLE IF NOT EXISTS water_prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    price_per_thousand_gallons DECIMAL(10,4),
    tier INTEGER DEFAULT 1,
    source VARCHAR(100),
    seasonal_multiplier DECIMAL(4,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_water_prices_date ON water_prices(date);

-- 4. Optimization Summary Table
CREATE TABLE IF NOT EXISTS optimization_summary (
    run_id UUID PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    run_name VARCHAR(200),
    total_cost DECIMAL(12,2),
    electricity_cost DECIMAL(12,2),
    water_cost DECIMAL(12,2),
    baseline_cost DECIMAL(12,2),
    cost_savings DECIMAL(12,2),
    cost_savings_percent DECIMAL(6,2),
    total_water_usage_gallons DECIMAL(12,2),
    peak_demand_mw DECIMAL(8,2),
    water_saved_gallons DECIMAL(12,2),
    carbon_avoided_tons DECIMAL(10,4),
    optimization_status VARCHAR(50),
    solver_name VARCHAR(50),
    solver_time DECIMAL(10,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_opt_summary_timestamp ON optimization_summary(run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_opt_summary_status ON optimization_summary(optimization_status);

-- 5. Optimization Results (Hourly Details) Table
CREATE TABLE IF NOT EXISTS optimization_results (
    id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES optimization_summary(run_id) ON DELETE CASCADE,
    hour INTEGER CHECK (hour >= 0 AND hour < 24),
    batch_load_mw DECIMAL(8,2),
    cooling_mode VARCHAR(20), -- 'water' or 'electric'
    temperature_f DECIMAL(5,2),
    electricity_price DECIMAL(10,2),
    water_usage_gallons DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_opt_results_run_id ON optimization_results(run_id);
CREATE INDEX IF NOT EXISTS idx_opt_results_hour ON optimization_results(hour);

-- 6. EIA Interchange Data (Already exists, but ensure indexes)
CREATE INDEX IF NOT EXISTS idx_eia_interchange_period ON eia_interchange(period);
CREATE INDEX IF NOT EXISTS idx_eia_interchange_fromba ON eia_interchange(fromba);
CREATE INDEX IF NOT EXISTS idx_eia_interchange_toba ON eia_interchange(toba);
CREATE INDEX IF NOT EXISTS idx_eia_interchange_date ON eia_interchange(DATE(period));

-- 7. EIA Arizona Price Data (Already exists, but ensure indexes)
CREATE INDEX IF NOT EXISTS idx_eia_az_price_period ON eia_az_price(period_month);
CREATE INDEX IF NOT EXISTS idx_eia_az_price_sector ON eia_az_price(sectorid);

-- Grant necessary permissions (adjust as needed)
GRANT ALL ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO authenticated;

-- Create a view for easy access to latest optimization
CREATE OR REPLACE VIEW latest_optimization AS
SELECT
    os.*,
    COUNT(or_detail.id) as hourly_records,
    SUM(CASE WHEN or_detail.cooling_mode = 'water' THEN 1 ELSE 0 END) as water_cooling_hours,
    SUM(CASE WHEN or_detail.cooling_mode = 'electric' THEN 1 ELSE 0 END) as electric_cooling_hours
FROM optimization_summary os
LEFT JOIN optimization_results or_detail ON os.run_id = or_detail.run_id
WHERE os.optimization_status = 'completed'
GROUP BY os.run_id
ORDER BY os.run_timestamp DESC
LIMIT 1;

-- Create a view for daily statistics
CREATE OR REPLACE VIEW daily_optimization_stats AS
SELECT
    DATE(run_timestamp) as date,
    COUNT(*) as runs,
    AVG(cost_savings) as avg_savings,
    SUM(cost_savings) as total_savings,
    AVG(cost_savings_percent) as avg_savings_percent,
    AVG(total_water_usage_gallons) as avg_water_usage,
    AVG(peak_demand_mw) as avg_peak_demand,
    SUM(carbon_avoided_tons) as total_carbon_avoided
FROM optimization_summary
WHERE optimization_status = 'completed'
GROUP BY DATE(run_timestamp)
ORDER BY date DESC;

-- Create a materialized view for performance (refresh periodically)
CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_optimization_summary AS
SELECT
    DATE_TRUNC('month', run_timestamp) as month,
    COUNT(*) as total_runs,
    SUM(cost_savings) as total_savings,
    AVG(cost_savings) as avg_daily_savings,
    AVG(cost_savings_percent) as avg_savings_percent,
    SUM(total_water_usage_gallons) as total_water_usage,
    AVG(peak_demand_mw) as avg_peak_demand,
    SUM(carbon_avoided_tons) as total_carbon_avoided
FROM optimization_summary
WHERE optimization_status = 'completed'
GROUP BY DATE_TRUNC('month', run_timestamp)
ORDER BY month DESC;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_monthly_opt_summary_month ON monthly_optimization_summary(month);

-- Function to refresh materialized view (call periodically)
CREATE OR REPLACE FUNCTION refresh_monthly_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_optimization_summary;
END;
$$ LANGUAGE plpgsql;

-- Sample data insertion for testing (remove in production)
-- INSERT INTO water_prices (date, price_per_thousand_gallons, source, seasonal_multiplier)
-- VALUES
--     ('2024-01-01', 3.24, 'Phoenix Water Department', 1.0),
--     ('2024-06-01', 3.24, 'Phoenix Water Department', 1.15),  -- Summer rate
--     ('2024-10-01', 3.24, 'Phoenix Water Department', 1.0);

-- Verify tables are created
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
    AND table_name IN (
        'weather_data',
        'electricity_prices',
        'water_prices',
        'optimization_summary',
        'optimization_results'
    )
ORDER BY table_name;