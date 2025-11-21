# Project Structure Guide

## Directory Layout

```
Cooling-The-Cloud/
│
├── main.py                 # Main entry point for the optimization
├── requirements.txt        # Python dependencies
├── .env.example           # Environment variables template
├── .gitignore             # Git ignore rules
│
├── data/                  # All data-related code and files
│   ├── api/              # TEAM MEMBER 1: API data fetching
│   │   ├── eia_client.py          # EIA API client
│   │   ├── grid_data_fetcher.py   # Grid demand fetcher
│   │   └── price_calculator.py    # Demand-to-price converter
│   │
│   ├── csv/              # TEAM MEMBER 2: CSV data processing
│   │   ├── weather_downloader.py  # NOAA data downloader
│   │   ├── weather_processor.py   # Temperature data processor
│   │   └── cooling_efficiency.py  # Temperature-efficiency mapping
│   │
│   └── sample/           # Sample data for testing
│       ├── sample_prices.json     # Example electricity prices
│       └── sample_weather.csv     # Example weather data
│
├── model/                 # YOU: Optimization model
│   ├── optimizer.py              # Core Pyomo/GEKKO model
│   ├── constraints.py            # Model constraints
│   ├── data_interface.py         # Integration with data sources
│   └── solver_config.py          # Solver configuration
│
├── visualization/         # YOU: Results visualization
│   ├── dashboard.py              # Dash/Plotly dashboard
│   ├── plots.py                  # Individual plot functions
│   └── report_generator.py       # PDF/HTML report creator
│
├── config/               # Configuration files
│   ├── .env.example             # Template for environment variables
│   ├── model_params.yaml        # Model parameters
│   └── api_keys.json            # API keys (DO NOT COMMIT)
│
├── scripts/              # Utility scripts
│   ├── run_optimization.sh      # Main execution script
│   ├── download_data.py         # Data download helper
│   └── test_integration.py      # Integration test script
│
├── tests/                # Test files
│   ├── test_optimizer.py         # Model tests
│   ├── test_data_api.py          # API tests
│   └── test_data_csv.py          # CSV processing tests
│
└── docs/                 # Documentation
    ├── API_GUIDE.md             # API integration guide
    ├── MODEL_EQUATIONS.md       # Mathematical formulation
    └── PRESENTATION.md          # Hackathon presentation notes
```

## File Naming Convention

- Python files: `snake_case.py`
- Data files: `YYYY-MM-DD_description.csv`
- Config files: `lowercase.yaml` or `.env`
- Documentation: `UPPERCASE.md`

## Git Branch Structure

```
main                    # Production-ready code
├── feature/api-data    # Team Member 1 branch
├── feature/csv-data    # Team Member 2 branch
└── feature/optimization # Your branch
```

## Key Files by Team Member

### TEAM MEMBER 1 (API) Creates:
- `data/api/eia_client.py`
- `data/api/grid_data_fetcher.py`
- `data/api/price_calculator.py`
- `tests/test_data_api.py`

### TEAM MEMBER 2 (CSV) Creates:
- `data/csv/weather_downloader.py`
- `data/csv/weather_processor.py`
- `data/csv/cooling_efficiency.py`
- `tests/test_data_csv.py`

### YOU (Optimization) Create:
- `model/optimizer.py`
- `model/constraints.py`
- `model/data_interface.py`
- `visualization/dashboard.py`
- `visualization/plots.py`
- `main.py`

## Data Flow

1. **Input**: Raw data from APIs and CSV files
2. **Processing**: Data cleaning and transformation in `data/` modules
3. **Integration**: Data interface in `model/data_interface.py`
4. **Optimization**: Core model in `model/optimizer.py`
5. **Output**: Visualization in `visualization/` and results export

## Environment Variables (.env file)

```bash
# API Keys
EIA_API_KEY=your_key_here
NOAA_API_TOKEN=your_token_here

# Model Parameters
DEFAULT_DATE=2024-07-15
SOLVER=glpk

# Paths
DATA_DIR=./data/sample
OUTPUT_DIR=./output
```

## Quick Start Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

# Run optimization
python main.py --date 2024-07-15

# Start dashboard
python visualization/dashboard.py

# Full pipeline
./scripts/run_optimization.sh
```