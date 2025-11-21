-- Create tables for Cooling The Cloud project
-- Run this in Supabase SQL Editor

-- Electricity Prices Table
CREATE TABLE IF NOT EXISTS electricity_prices (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    hour INTEGER CHECK (hour >= 0 AND hour <= 23),
    price_per_mwh DECIMAL(10, 2) NOT NULL,
    rate_type TEXT CHECK (rate_type IN ('peak', 'off-peak', 'super-off-peak')),
    source TEXT DEFAULT 'inferred',
    grid_demand_mw DECIMAL(10, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(timestamp, source)
);

-- Create index for faster queries
CREATE INDEX idx_electricity_prices_timestamp ON electricity_prices(timestamp);
CREATE INDEX idx_electricity_prices_hour ON electricity_prices(hour);

-- Water Prices Table
CREATE TABLE IF NOT EXISTS water_prices (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    price_per_thousand_gallons DECIMAL(10, 4) NOT NULL DEFAULT 3.24,
    tier INTEGER DEFAULT 1,
    source TEXT DEFAULT 'Arizona Water Resources',
    seasonal_multiplier DECIMAL(5, 3) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, tier)
);

-- Create index for date queries
CREATE INDEX idx_water_prices_date ON water_prices(date);

-- Optimization Results Table (hourly results)
CREATE TABLE IF NOT EXISTS optimization_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    run_timestamp TIMESTAMPTZ NOT NULL,
    hour INTEGER CHECK (hour >= 0 AND hour <= 23),

    -- Load and demand
    batch_load_mw DECIMAL(10, 2),
    total_load_mw DECIMAL(10, 2),

    -- Cooling decisions
    cooling_mode TEXT CHECK (cooling_mode IN ('water', 'electric', 'hybrid', 'free')),
    chiller_stages INTEGER DEFAULT 0,
    water_cooling_active BOOLEAN DEFAULT FALSE,

    -- Storage
    storage_level_mwh DECIMAL(10, 2),
    storage_charge_mw DECIMAL(10, 2),
    storage_discharge_mw DECIMAL(10, 2),

    -- Costs and usage
    hourly_cost DECIMAL(12, 2),
    electricity_cost DECIMAL(12, 2),
    water_cost DECIMAL(12, 2),
    water_usage_gallons DECIMAL(12, 2),

    -- Environmental conditions
    temperature_f DECIMAL(5, 2),
    electricity_price DECIMAL(10, 2),

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for querying
CREATE INDEX idx_optimization_results_run_id ON optimization_results(run_id);
CREATE INDEX idx_optimization_results_timestamp ON optimization_results(run_timestamp);
CREATE INDEX idx_optimization_results_hour ON optimization_results(hour);

-- Optimization Summary Table (run summaries)
CREATE TABLE IF NOT EXISTS optimization_summary (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_name TEXT,

    -- Cost metrics
    total_cost DECIMAL(12, 2),
    electricity_cost DECIMAL(12, 2),
    water_cost DECIMAL(12, 2),
    baseline_cost DECIMAL(12, 2),
    cost_savings DECIMAL(12, 2),
    cost_savings_percent DECIMAL(5, 2),

    -- Resource usage
    total_water_usage_gallons DECIMAL(12, 2),
    peak_demand_mw DECIMAL(10, 2),
    average_load_mw DECIMAL(10, 2),

    -- Environmental metrics
    water_saved_gallons DECIMAL(12, 2),
    carbon_avoided_tons DECIMAL(10, 2),

    -- Configuration (JSON)
    configuration JSONB,

    -- Weather summary
    max_temperature_f DECIMAL(5, 2),
    min_temperature_f DECIMAL(5, 2),
    avg_temperature_f DECIMAL(5, 2),

    -- Performance metrics
    solver_time_seconds DECIMAL(10, 3),
    optimization_status TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for timestamp queries
CREATE INDEX idx_optimization_summary_timestamp ON optimization_summary(run_timestamp);
CREATE INDEX idx_optimization_summary_run_id ON optimization_summary(run_id);

-- Enable Row Level Security (RLS)
ALTER TABLE electricity_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE water_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE optimization_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE optimization_summary ENABLE ROW LEVEL SECURITY;

-- Create policies for anonymous access (read-only for prices, full access for results)
-- Electricity prices - everyone can read
CREATE POLICY "Allow anonymous read electricity_prices" ON electricity_prices
    FOR SELECT
    USING (true);

-- Water prices - everyone can read
CREATE POLICY "Allow anonymous read water_prices" ON water_prices
    FOR SELECT
    USING (true);

-- Optimization results - full access for anonymous users
CREATE POLICY "Allow anonymous full access optimization_results" ON optimization_results
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- Optimization summary - full access for anonymous users
CREATE POLICY "Allow anonymous full access optimization_summary" ON optimization_summary
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- Insert for electricity and water prices (for seeding data)
CREATE POLICY "Allow anonymous insert electricity_prices" ON electricity_prices
    FOR INSERT
    WITH CHECK (true);

CREATE POLICY "Allow anonymous insert water_prices" ON water_prices
    FOR INSERT
    WITH CHECK (true);

-- Grant permissions to anon role
GRANT USAGE ON SCHEMA public TO anon;
GRANT ALL ON ALL TABLES IN SCHEMA public TO anon;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO anon;

-- Add some comments for documentation
COMMENT ON TABLE electricity_prices IS 'Hourly electricity prices for Arizona data centers';
COMMENT ON TABLE water_prices IS 'Daily water pricing tiers for industrial use';
COMMENT ON TABLE optimization_results IS 'Hourly optimization results for each run';
COMMENT ON TABLE optimization_summary IS 'Summary statistics for each optimization run';