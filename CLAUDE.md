# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an optimization system for Arizona data center operations, focusing on minimizing total operating costs while managing electricity demand and water conservation. It was developed for the IISE Hackathon with the theme "Electricity in and to Arizona".

The system optimizes a 50MW Phoenix data center by:
- Shifting flexible computational loads away from peak electricity hours (3-8 PM)
- Dynamically switching between water-based cooling (efficient but uses water) and electric chillers (no water but high energy)
- Using real public data from EIA (electricity) and NOAA (weather)

## Architecture

### Core Optimization Models
- **`model/optimizer.py`** - Full Pyomo model with complex constraints and multi-objective optimization (supports advanced solvers)
- **`model/optimizer_linear.py`** - Linearized version for GLPK compatibility (guaranteed to solve in hackathon environments)
- **`model/data_interface.py`** - Handles all data input, accepting CSV, JSON, DataFrames, or raw lists, with automatic validation and fallback to synthetic Phoenix summer data

### API Architecture
- **`api_server.py`** - Flask REST API server with endpoints:
  - `/api/optimize` - Run optimization with custom parameters
  - `/api/history` - Get optimization run history from database
  - `/api/period-summary` - Get summary statistics for specified period
  - `/api/monthly-breakdown` - Get monthly cost breakdown
  - `/api/daily-trends` - Get daily optimization trends
  - `/api/real-time-data` - Get real-time monitoring data
- **`api/index.py`** - Vercel-compatible API endpoints (same functionality)

### Frontend Applications
- **`cooling-cloud-react/`** - React 18 + Vite frontend with TailwindCSS
- **`streamlit_app.py`** - Interactive web dashboard for parameter tuning
- **`streamlit_app_advanced.py`** - Advanced dashboard with database integration
- **`streamlit_app_clean.py`** - Simplified version for quick demos

### Data Layer
- **`data/supabase_interface.py`** - Supabase/PostgreSQL database integration
- **`data/api/store_to_postgres.py`** - Store optimization results to database
- **`scripts/fetch_eia.py`** - Fetch EIA electricity grid data
- **`scripts/fetch_prices.py`** - Fetch electricity pricing data
- **`scripts/fetch_water_index.py`** - Fetch water usage index data

## Key Commands

### Installation
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Install GLPK solver (required)
# Ubuntu/WSL: sudo apt-get install glpk-utils
# Mac: brew install glpk
# Windows: Download from https://www.gnu.org/software/glpk/

# Verify GLPK installation
python -c "from pyomo.opt import SolverFactory; print(SolverFactory('glpk').available())"
```

### Running Tests
```bash
# Quick GLPK solver test
python tests/test_linear.py

# Integration test (all components)
python tests/test_integration.py

# Production system test
python tests/test_production_system.py

# Optimizer scaling test
python tests/test_optimizer_scaling.py

# Run specific test with pytest (if using pytest)
pytest tests/test_specific.py::test_function_name -v
```

### Running the Optimization
```bash
# Demo mode with synthetic Phoenix data (no API keys needed)
python main.py --demo

# With real data files
python main.py --electricity-data <eia_file> --weather-data <noaa_file> --solver glpk

# With Supabase integration
python optimize_with_real_data.py

# Export results
python main.py --demo --export

# Different solvers (fallback chain: requested → highs → glpk → cbc → ipopt)
python main.py --demo --solver glpk      # Most reliable for hackathon
python main.py --demo --solver highs     # Fast, modern
python main.py --demo --solver gurobi    # If commercial license available
```

### Running Web Interfaces

#### Streamlit Dashboards
```bash
# Basic interactive dashboard
streamlit run streamlit_app.py

# Advanced dashboard with database
streamlit run streamlit_app_advanced.py

# Clean/simplified version
streamlit run streamlit_app_clean.py
```

#### React Frontend
```bash
cd cooling-cloud-react
npm install
npm run dev     # Development server at localhost:5173
npm run build   # Production build
npm run preview # Preview production build
```

#### Flask API Server
```bash
# Local development
python api_server.py  # Runs on port 5000

# With custom port
python run_local_api.py
```

### Data Operations
```bash
# Fetch EIA data (requires API key in .env or --api-key)
python scripts/fetch_eia.py --api-key YOUR_KEY --save eia_data.json

# Fetch electricity prices
python scripts/fetch_prices.py

# Fetch water index data
python scripts/fetch_water_index.py

# Store results to PostgreSQL/Supabase
python data/api/store_to_postgres.py

# Explore Supabase data
python explore_supabase_data.py

# Check database schema
python check_database_schema.py
```

### Visualization
```bash
# Generate optimization visualizations
python visualization/dashboard.py

# HTML test dashboards (pre-generated)
# Open test_dashboard.html or test_dashboard_summary.html in browser
```

## Arizona-Specific Parameters

The system uses hardcoded Arizona utility rates and constraints:
- **Peak hours**: 3-8 PM at $0.15/kWh (APS Schedule E-32)
- **Off-peak**: $0.05/kWh
- **Super off-peak**: 10 PM-6 AM at $0.03/kWh
- **Phoenix water cost**: $3.24 per 1,000 gallons
- **Temperature range**: 75-120°F (validated for Phoenix summer)
- **Data center specs**: 50MW total, 30MW critical (must-run), 20MW flexible

## Solver Configuration

The code attempts solvers in this priority order: requested → highs → glpk → cbc → ipopt

For hackathon environments, use `--solver glpk` with `model/optimizer_linear.py` for guaranteed compatibility. The linear model simplifies constraints while preserving the core optimization problem.

## Data Input Formats

The `DataInterface` class accepts:
- **CSV files** with columns: `Hour`, `Temperature`, `ElectricityPrice`
- **JSON** with keys: `hours`, `temperatures`, `prices`
- **Pandas DataFrames** or raw Python lists
- Automatically generates realistic Phoenix summer data if no input provided

## Environment Variables

Optional `.env` file configuration:
```bash
# Supabase (for database features)
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_anon_key

# EIA API (for real electricity data)
EIA_API_KEY=your_eia_api_key

# NOAA API (for weather data)
NOAA_API_TOKEN=your_noaa_token
```

Note: Demo mode works without any API keys using synthetic Phoenix data.

## Testing Strategy

- **`tests/test_linear.py`** - Creates synthetic Phoenix summer day (75-118°F sine curve) to test GLPK solver
- **`tests/test_integration.py`** - Validates all components work together
- **`tests/test_production_system.py`** - Tests production-ready system with database
- **`tests/test_optimizer_scaling.py`** - Tests optimizer performance at different scales
- Tests use synthetic data for reproducibility and validate Phoenix-specific ranges

## Current Status

- Core optimization models: Complete
- Data interfaces: Complete
- Flask API: Complete with 7 endpoints
- Streamlit dashboards: 3 versions available
- React frontend: Landing page and basic routing implemented
- Database integration: Supabase/PostgreSQL ready
- Real data integration: Scripts ready (requires API keys)
- GitHub Actions: Daily data fetch workflow configured