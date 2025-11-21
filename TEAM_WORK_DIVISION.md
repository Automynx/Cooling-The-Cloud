# Team Work Division - Cooling The Cloud Hackathon

## Team Structure (3 Members)

### TEAM MEMBER 1: API Data Engineer
**Focus: Real-time Electricity Grid Data**

**Responsibilities:**
1. Set up EIA (Energy Information Administration) API access
2. Fetch Southwest region electricity demand data
3. Create pricing model based on demand curves

**Files to Create:**
- `data/api/eia_client.py` - API client for EIA data
- `data/api/grid_data_fetcher.py` - Fetches hourly grid demand
- `data/api/price_calculator.py` - Converts demand to pricing tiers

**Output Interface:**
- Function: `get_hourly_electricity_prices(date) -> dict`
- Returns: `{hour: price_per_kwh}` for 24 hours

---

### TEAM MEMBER 2: CSV Data Analyst
**Focus: Weather & Historical Data**

**Responsibilities:**
1. Download NOAA weather data for Phoenix Sky Harbor
2. Process temperature data (hourly dry bulb temperatures)
3. Create cooling efficiency curves based on temperature

**Files to Create:**
- `data/csv/weather_downloader.py` - Downloads NOAA data
- `data/csv/weather_processor.py` - Cleans and processes CSV
- `data/csv/cooling_efficiency.py` - Temperature to efficiency mapping

**Output Interface:**
- Function: `get_hourly_temperatures(date) -> dict`
- Function: `get_cooling_efficiency(temperature) -> dict`
- Returns: `{hour: temperature}` and `{water_cooling_eff, chiller_eff}`

---

### YOU: Optimization Engineer & Integrator
**Focus: Core Model & System Integration**

**Responsibilities:**
1. Build optimization model using Pyomo/GEKKO
2. Integrate data from both team members
3. Create visualization dashboard
4. Coordinate final integration

**Files to Create:**
- `requirements.txt` - Project dependencies
- `model/optimizer.py` - Core optimization model
- `model/constraints.py` - Model constraints and objectives
- `model/data_interface.py` - Integration layer for both data sources
- `visualization/dashboard.py` - Results visualization
- `main.py` - Main execution script

---

## Communication Protocol

### Data Exchange Format
```python
# Standard data structure for hourly data (24 hours)
hourly_data = {
    'electricity_prices': [price_0, price_1, ..., price_23],
    'temperatures': [temp_0, temp_1, ..., temp_23],
    'demand': [demand_0, demand_1, ..., demand_23]
}
```

### Git Branch Strategy
- `main` - Final integrated code
- `feature/api-data` - Team Member 1
- `feature/csv-data` - Team Member 2
- `feature/optimization` - You

### Integration Points
1. **Hour 2**: Data teams provide sample data format
2. **Hour 4**: You provide model interface requirements
3. **Hour 6**: Integration testing begins
4. **Hour 8**: Final optimization and visualization

---

## Key Model Parameters (For Reference)

### Data Center Specifications
- **Capacity**: 50 MW
- **Base Load**: 30 MW (critical, always on)
- **Flexible Load**: 20 MW (batch processing, shiftable)
- **Cooling Requirement**: 15 MW equivalent

### Cooling Modes
1. **Water Cooling (Evaporative)**
   - Energy: 0.5 kW per kW of cooling
   - Water: 2 gallons per minute per MW
   - Best when: Temperature > 85°F

2. **Chiller Cooling (Traditional AC)**
   - Energy: 1.2 kW per kW of cooling
   - Water: 0 gallons
   - Best when: Peak electricity prices

### Optimization Objectives
- Minimize: `Total Cost = Electricity Cost + Water Cost`
- Subject to:
  - Meet cooling requirements
  - Complete all batch jobs within 24 hours
  - Respect peak demand limits

---

## Quick Start Commands

```bash
# Install dependencies (You do this first)
pip install -r requirements.txt

# Team Member 1 test
python data/api/eia_client.py --test

# Team Member 2 test
python data/csv/weather_processor.py --test

# Run optimization (after integration)
python main.py --date 2024-07-15
```

---

## Success Criteria
- Model switches cooling modes based on temperature AND price
- Shifts at least 30% of batch load away from peak hours
- Achieves 15-25% cost reduction vs baseline
- Clear visualization showing decision points