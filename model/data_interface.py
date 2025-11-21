"""
Data Interface for Real Public Data Integration
Handles data from EIA (electricity) and NOAA (weather) sources
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime, timedelta
import os


class DataInterface:
    """
    Interface to handle real data from team members and prepare it for optimization.
    Flexible enough to handle various formats from EIA and NOAA.
    """

    def __init__(self):
        """Initialize data interface with Arizona-specific defaults."""
        # Arizona utility rate structure (APS Schedule E-32)
        self.peak_hours = list(range(15, 20))  # 3 PM - 8 PM
        self.peak_rate = 0.15  # $/kWh peak summer
        self.offpeak_rate = 0.05  # $/kWh off-peak summer
        self.super_offpeak_rate = 0.03  # $/kWh night rate

        # Default Phoenix summer temperature pattern if needed
        self.default_temp_pattern = self._generate_phoenix_pattern()

    def load_electricity_data(self,
                            data_source: Union[str, pd.DataFrame, Dict, List]) -> Dict:
        """
        Load electricity data from various formats your teammate might provide.

        Accepts:
        - CSV file path
        - DataFrame
        - JSON/Dict
        - List of hourly prices

        Returns:
            Dict with 'prices' and 'demand' arrays
        """
        electricity_data = {'prices': [], 'demand': [], 'timestamps': []}

        # Handle different input types
        if isinstance(data_source, str):
            if data_source.endswith('.csv'):
                df = pd.read_csv(data_source)
            elif data_source.endswith('.json'):
                with open(data_source, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        electricity_data = self._parse_eia_json(data)
                    else:
                        electricity_data['prices'] = data
            else:
                raise ValueError(f"Unsupported file format: {data_source}")

        elif isinstance(data_source, pd.DataFrame):
            electricity_data = self._parse_eia_dataframe(data_source)

        elif isinstance(data_source, dict):
            electricity_data = self._parse_eia_json(data_source)

        elif isinstance(data_source, list):
            electricity_data['prices'] = data_source

        # If no prices found, generate from demand or use TOU rates
        if not electricity_data['prices']:
            if electricity_data['demand']:
                electricity_data['prices'] = self._estimate_prices_from_demand(
                    electricity_data['demand']
                )
            else:
                electricity_data['prices'] = self._generate_tou_prices()

        # Ensure we have 24 hours of data
        electricity_data['prices'] = self._ensure_24_hours(electricity_data['prices'])

        return electricity_data

    def load_weather_data(self,
                         data_source: Union[str, pd.DataFrame, Dict, List]) -> List[float]:
        """
        Load weather data from various formats your teammate might provide.

        Accepts:
        - CSV file path (NOAA format)
        - DataFrame
        - JSON/Dict
        - List of hourly temperatures

        Returns:
            List of 24 hourly temperatures in Fahrenheit
        """
        temperatures = []

        # Handle different input types
        if isinstance(data_source, str):
            if data_source.endswith('.csv'):
                df = pd.read_csv(data_source)
                temperatures = self._parse_noaa_csv(df)
            elif data_source.endswith('.json'):
                with open(data_source, 'r') as f:
                    data = json.load(f)
                    temperatures = self._extract_temperatures(data)
            else:
                raise ValueError(f"Unsupported file format: {data_source}")

        elif isinstance(data_source, pd.DataFrame):
            temperatures = self._parse_noaa_dataframe(data_source)

        elif isinstance(data_source, dict):
            temperatures = self._extract_temperatures(data_source)

        elif isinstance(data_source, list):
            temperatures = data_source

        # Ensure we have 24 hours of data
        temperatures = self._ensure_24_hours(temperatures)

        # Validate temperature ranges for Phoenix
        temperatures = self._validate_phoenix_temperatures(temperatures)

        return temperatures

    def _parse_eia_json(self, data: Dict) -> Dict:
        """Parse EIA JSON format."""
        result = {'prices': [], 'demand': [], 'timestamps': []}

        # Try different possible JSON structures from EIA
        if 'response' in data:
            if 'data' in data['response']:
                for item in data['response']['data']:
                    if 'value' in item:
                        result['demand'].append(item['value'])
                    if 'price' in item:
                        result['prices'].append(item['price'])

        elif 'data' in data:
            if isinstance(data['data'], list):
                for item in data['data']:
                    if isinstance(item, dict):
                        if 'demand' in item:
                            result['demand'].append(item['demand'])
                        if 'price' in item:
                            result['prices'].append(item['price'])
                    else:
                        result['demand'].append(item)

        elif 'prices' in data:
            result['prices'] = data['prices']

        elif 'demand' in data:
            result['demand'] = data['demand']

        return result

    def _parse_eia_dataframe(self, df: pd.DataFrame) -> Dict:
        """Parse EIA DataFrame format."""
        result = {'prices': [], 'demand': [], 'timestamps': []}

        # Look for common column names
        price_cols = ['price', 'prices', 'lmp', 'electricity_price', 'rate']
        demand_cols = ['demand', 'load', 'mw', 'consumption']

        for col in df.columns:
            col_lower = col.lower()
            if any(p in col_lower for p in price_cols):
                result['prices'] = df[col].tolist()
                break
            if any(d in col_lower for d in demand_cols):
                result['demand'] = df[col].tolist()

        return result

    def _parse_noaa_csv(self, df: pd.DataFrame) -> List[float]:
        """Parse NOAA CSV format."""
        temperatures = []

        # Common NOAA column names
        temp_cols = ['HourlyDryBulbTemperature', 'TEMP', 'Temperature',
                    'DryBulbTemp', 'temperature', 'temp_f']

        for col in df.columns:
            if any(t in col for t in temp_cols):
                temperatures = df[col].tolist()
                break

        # Convert to Fahrenheit if needed (check if values are too low)
        if temperatures and max(temperatures) < 50:
            # Likely in Celsius
            temperatures = [t * 9/5 + 32 for t in temperatures]

        return temperatures

    def _parse_noaa_dataframe(self, df: pd.DataFrame) -> List[float]:
        """Parse NOAA DataFrame format."""
        return self._parse_noaa_csv(df)

    def _extract_temperatures(self, data: Union[Dict, List]) -> List[float]:
        """Extract temperatures from various JSON structures."""
        if isinstance(data, list):
            return data

        temperatures = []

        # Try different possible structures
        if 'temperatures' in data:
            temperatures = data['temperatures']
        elif 'data' in data:
            if isinstance(data['data'], list):
                temperatures = data['data']
            elif isinstance(data['data'], dict) and 'temperatures' in data['data']:
                temperatures = data['data']['temperatures']
        elif 'observations' in data:
            for obs in data['observations']:
                if 'temperature' in obs:
                    temperatures.append(obs['temperature'])

        return temperatures

    def _estimate_prices_from_demand(self, demand: List[float]) -> List[float]:
        """
        Estimate electricity prices from demand using typical correlation.
        Higher demand = higher prices.
        """
        if not demand:
            return self._generate_tou_prices()

        # Normalize demand
        min_demand = min(demand)
        max_demand = max(demand)
        range_demand = max_demand - min_demand if max_demand > min_demand else 1

        prices = []
        for h, d in enumerate(demand):
            # Base price from demand level
            normalized = (d - min_demand) / range_demand
            base_price = self.offpeak_rate + (self.peak_rate - self.offpeak_rate) * normalized

            # Apply time-of-use multiplier
            if h in self.peak_hours:
                price = base_price * 1.5
            elif h in range(22, 24) or h in range(0, 6):
                price = base_price * 0.6
            else:
                price = base_price

            prices.append(price * 1000)  # Convert to $/MWh

        return prices

    def _generate_tou_prices(self) -> List[float]:
        """Generate time-of-use prices based on APS rate schedule."""
        prices = []
        for h in range(24):
            if h in self.peak_hours:  # Peak: 3-8 PM
                price = self.peak_rate
            elif h in range(22, 24) or h in range(0, 6):  # Super off-peak
                price = self.super_offpeak_rate
            else:  # Off-peak
                price = self.offpeak_rate

            # Add some variation
            variation = np.random.uniform(0.95, 1.05)
            prices.append(price * variation * 1000)  # Convert to $/MWh

        return prices

    def _generate_phoenix_pattern(self) -> List[float]:
        """Generate typical Phoenix summer temperature pattern."""
        # Phoenix July average: Low 84°F at 5 AM, High 106°F at 5 PM
        temperatures = []
        for h in range(24):
            # Sine wave pattern
            base = 95  # Average temperature
            amplitude = 15  # Half of daily range
            phase = (h - 5) * np.pi / 12  # Minimum at 5 AM
            temp = base + amplitude * np.sin(phase - np.pi/2)

            # Add slight random variation
            temp += np.random.uniform(-2, 2)
            temperatures.append(max(75, min(120, temp)))  # Cap at reasonable limits

        return temperatures

    def _ensure_24_hours(self, data: List) -> List:
        """Ensure we have exactly 24 hours of data."""
        if not data:
            return [0] * 24

        if len(data) == 24:
            return data

        if len(data) > 24:
            # Take first 24 hours
            return data[:24]

        # Less than 24 hours - need to fill
        if len(data) < 24:
            # Repeat pattern to fill 24 hours
            repeated = data * (24 // len(data) + 1)
            return repeated[:24]

        return data

    def _validate_phoenix_temperatures(self, temperatures: List[float]) -> List[float]:
        """Validate and adjust temperatures to be realistic for Phoenix."""
        validated = []
        for temp in temperatures:
            # Phoenix records: Min ever 16°F, Max ever 122°F
            # Typical summer: 75°F - 115°F
            if temp < 50:  # Probably wrong units or winter data
                temp = 85  # Use typical low
            elif temp > 125:  # Unrealistic
                temp = 115  # Cap at typical max

            validated.append(temp)

        return validated

    def prepare_optimization_data(self,
                                  electricity_source: Union[str, pd.DataFrame, Dict, List],
                                  weather_source: Union[str, pd.DataFrame, Dict, List],
                                  date: Optional[str] = None) -> Dict:
        """
        Main method to prepare all data for optimization.

        Args:
            electricity_source: Electricity data from team member
            weather_source: Weather data from team member
            date: Optional date string for the optimization period

        Returns:
            Dict with all data needed for optimization
        """
        # Load both data sources
        elec_data = self.load_electricity_data(electricity_source)
        temperatures = self.load_weather_data(weather_source)

        # Prepare final dataset
        optimization_data = {
            'temperatures': temperatures,
            'electricity_prices': elec_data['prices'],
            'grid_demand': elec_data.get('demand', None),
            'date': date or datetime.now().strftime('%Y-%m-%d'),
            'metadata': {
                'peak_hours': self.peak_hours,
                'max_temp': max(temperatures),
                'min_temp': min(temperatures),
                'avg_price': np.mean(elec_data['prices']),
                'price_range': max(elec_data['prices']) - min(elec_data['prices'])
            }
        }

        # Validate data quality
        self._validate_data(optimization_data)

        return optimization_data

    def _validate_data(self, data: Dict) -> None:
        """Validate that data is reasonable for optimization."""
        # Check temperatures
        if max(data['temperatures']) < 70:
            print("WARNING: Maximum temperature seems low for Phoenix summer")
        if min(data['temperatures']) > 100:
            print("WARNING: Minimum temperature seems high even for Phoenix")

        # Check prices
        if max(data['electricity_prices']) < 10:
            print("WARNING: Electricity prices seem too low (should be $/MWh)")
        if min(data['electricity_prices']) < 0:
            raise ValueError("Negative electricity prices detected")

        # Check data completeness
        if len(data['temperatures']) != 24:
            raise ValueError(f"Expected 24 hours of temperature data, got {len(data['temperatures'])}")
        if len(data['electricity_prices']) != 24:
            raise ValueError(f"Expected 24 hours of price data, got {len(data['electricity_prices'])}")

    def export_to_model_format(self, optimization_data: Dict) -> Tuple[List, List, Optional[List]]:
        """
        Export data in exact format needed by optimizer.

        Returns:
            Tuple of (temperatures, prices, demand)
        """
        return (
            optimization_data['temperatures'],
            optimization_data['electricity_prices'],
            optimization_data.get('grid_demand', None)
        )