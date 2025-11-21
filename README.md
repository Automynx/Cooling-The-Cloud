# CoolTheCloud - Weather Data Pipeline for Cooling System Optimization

A Python application that fetches hourly weather data from Phoenix Sky Harbor Airport (ASOS station) and stores it in Supabase for cooling system optimization. The system helps determine when to use evaporative cooling vs. electric chillers based on temperature and humidity conditions.

## Project Overview

This hackathon project optimizes cooling costs by intelligently switching between:
- **Evaporative Cooling**: Energy-efficient when humidity is low (< 30%)
- **Electric Chillers**: Required when humidity is high (during monsoon seasons)

The system fetches temperature and relative humidity data to make informed decisions about which cooling method is most efficient.

## Features

- Fetches hourly weather data from Iowa Environmental Mesonet ASOS API
- Stores data in Supabase for real-time analysis
- Analyzes cooling efficiency based on weather conditions
- Batch processing for historical data collection
- Incremental updates to avoid duplicate data
- Comprehensive logging and error handling
- Cooling system recommendations based on physics

## Prerequisites

- Python 3.8 or higher
- Supabase account with project created
- Internet connection for API access

## Installation

1. Clone the repository or download the project files

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Create the Supabase table:
   - Option 1: Run `python main.py --create-table` to generate SQL
   - Option 2: Copy the SQL below and execute in Supabase SQL editor:

```sql
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    station VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_f REAL,
    humidity_percent REAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(station, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_weather_data_timestamp ON weather_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_data_station ON weather_data(station);
```

## Configuration

The project is pre-configured with your Supabase credentials in `config.py`:
- Supabase URL: Already set
- Supabase API Key: Already set
- Station: Phoenix Sky Harbor (PHX)

To use environment variables instead (recommended for production):
1. Create a `.env` file:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

2. The application will automatically load from `.env` if present

## Usage

### Basic Usage - Fetch One Year of Historical Data

```bash
python main.py
```

This fetches data from August 1, 2024 to August 1, 2025 (default range).

### Custom Date Range

```bash
python main.py --start-date 2024-01-01 --end-date 2024-11-19
```

### Fetch Latest 24 Hours

```bash
python main.py --latest
```

### View Database Summary

```bash
python main.py --summary
```

### Advanced Options

```bash
# Adjust batch size (days per API call)
python main.py --batch-days 7

# Set logging level
python main.py --log-level DEBUG

# Generate table creation SQL
python main.py --create-table
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-date` | Start date (YYYY-MM-DD) | 2024-08-01 |
| `--end-date` | End date (YYYY-MM-DD) | 2025-08-01 |
| `--batch-days` | Days per batch | 30 |
| `--latest` | Fetch only last 24 hours | False |
| `--summary` | Show database summary | False |
| `--create-table` | Generate table SQL | False |
| `--log-level` | Logging level | INFO |

## Data Fields

The pipeline collects:
- **tmpf**: Air Temperature in Fahrenheit
  - Determines cooling system workload
  - Higher temperature = Higher electricity cost

- **relh**: Relative Humidity Percentage
  - Determines evaporative cooling effectiveness
  - Low humidity (< 30%) = Evaporative cooling works great
  - High humidity (> 30%) = Switch to electric chillers

## Cooling System Logic

The system analyzes weather conditions to recommend:

1. **No Cooling**: Temperature < 85°F
2. **Evaporative Cooling**: Temperature > 85°F AND Humidity < 30%
3. **Hybrid System**: Temperature > 85°F AND Humidity 30-50%
4. **Electric Chillers**: Temperature > 85°F AND Humidity > 50%

## Project Structure

```
CoolTheCloud/
├── main.py              # Main orchestration script
├── weather_fetcher.py   # Iowa Mesonet API client
├── database.py          # Supabase operations
├── utils.py            # Helper functions and logging
├── config.py           # Configuration settings
├── requirements.txt    # Python dependencies
├── logs/               # Application logs
└── README.md          # This file
```

## API Information

- **Data Source**: Iowa Environmental Mesonet (IEM)
- **API Endpoint**: https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py
- **Station**: PHX (Phoenix Sky Harbor Airport)
- **Network**: AZ_ASOS
- **Data Type**: Hourly ASOS observations

## Troubleshooting

### No Data Returned
- Check internet connection
- Verify date range is not in the future
- Check logs in `logs/weather_pipeline.log`

### Database Connection Issues
- Verify Supabase credentials in config.py
- Ensure table is created with proper schema
- Check Supabase project is active

### Duplicate Data
- The system uses UNIQUE constraint on (station, timestamp)
- Duplicates are automatically handled via UPSERT

## Performance

- Fetches ~8,760 hourly records per year
- Processes data in 30-day batches to avoid timeouts
- Typical execution time: 2-5 minutes for one year of data
- Storage: ~350KB per year of data

## Future Enhancements

1. **Real-time Updates**: Schedule hourly updates using cron or task scheduler
2. **Multiple Stations**: Expand to other Arizona airports
3. **Predictive Analytics**: Use ML to predict optimal cooling schedules
4. **Cost Calculations**: Integrate electricity rates for cost optimization
5. **Dashboard**: Create visualization dashboard for monitoring
6. **Alerts**: Send notifications when switching cooling systems

## Example Output

```
==================================================
WEATHER DATA SUMMARY
==================================================

Station: Phoenix Sky Harbor (PHX)
Total Records: 8,760
Date Range: 2024-08-01 to 2025-08-01

Temperature (°F):
  Mean: 77.3
  Min:  38.0
  Max:  118.0
  Std:  18.5

Humidity (%):
  Mean: 31.2
  Min:  5.0
  Max:  95.0
  Std:  21.3

Cooling Analysis:
  Hours needing cooling: 4,890 (55.8%)
  Hours suitable for evap cooling: 3,421 (70.0%)
==================================================
```

## License

This project is created for hackathon purposes.

## Support

For issues or questions:
1. Check the logs in `logs/weather_pipeline.log`
2. Verify your Supabase configuration
3. Ensure Python dependencies are installed

## Acknowledgments

- Iowa State University for the Mesonet ASOS data
- Supabase for database hosting
- Hackathon organizers