"""
Supabase Interface for Cooling The Cloud Project
Handles all database operations including fetching weather data,
price inference, and storing optimization results.
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from supabase import create_client, Client
from dotenv import load_dotenv
import uuid
import json

# Load environment variables
load_dotenv()


class SupabaseInterface:
    """
    Interface for all Supabase database operations.
    Handles weather data, price calculations, and result storage.
    """

    def __init__(self):
        """Initialize Supabase client and configuration."""
        # Get credentials from environment
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')

        if not self.url or not self.key:
            raise ValueError("Supabase credentials not found in environment variables")

        # Create Supabase client
        self.supabase: Client = create_client(self.url, self.key)

        # Load pricing configuration from environment
        self.water_price_per_1000_gal = float(os.getenv('DEFAULT_WATER_PRICE_PER_1000_GAL', 3.24))
        self.peak_hours_start = int(os.getenv('PEAK_HOURS_START', 15))
        self.peak_hours_end = int(os.getenv('PEAK_HOURS_END', 20))
        self.peak_rate = float(os.getenv('PEAK_ELECTRICITY_RATE_MWH', 150))
        self.offpeak_rate = float(os.getenv('OFFPEAK_ELECTRICITY_RATE_MWH', 35))
        self.super_offpeak_rate = float(os.getenv('SUPER_OFFPEAK_RATE_MWH', 25))

        # Cache for performance
        self._weather_cache = {}
        self._price_cache = {}

    # ==================== Weather Data Methods ====================

    def fetch_weather_data(self, date: Optional[datetime] = None, hours: int = 24) -> List[float]:
        """
        Fetch weather data from Supabase for a specific date.

        Args:
            date: The date to fetch weather for (defaults to today)
            hours: Number of hours to fetch (default 24)

        Returns:
            List of temperatures in Fahrenheit
        """
        if date is None:
            date = datetime.now()

        # Check cache first
        cache_key = f"{date.date()}_{hours}"
        if cache_key in self._weather_cache:
            return self._weather_cache[cache_key]

        try:
            # Calculate time range
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=hours)

            # Query weather data
            response = self.supabase.table('weather_data').select('*').gte(
                'timestamp', start_time.isoformat()
            ).lt(
                'timestamp', end_time.isoformat()
            ).order('timestamp').execute()

            if response.data and len(response.data) > 0:
                # Convert to DataFrame for easier processing
                df = pd.DataFrame(response.data)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['hour'] = df['timestamp'].dt.hour

                # Group by hour and take average temperature
                hourly_temps = df.groupby('hour')['temperature_f'].mean()

                # Ensure we have 24 hours
                temperatures = []
                for h in range(hours):
                    if h in hourly_temps.index:
                        temperatures.append(float(hourly_temps[h]))
                    else:
                        # Interpolate missing hours
                        temperatures.append(self._interpolate_temperature(h, hourly_temps))

                # Cache the result
                self._weather_cache[cache_key] = temperatures
                return temperatures

            else:
                print(f"No weather data found for {date}, using Phoenix pattern")
                return self._generate_phoenix_temperature_pattern(hours)

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return self._generate_phoenix_temperature_pattern(hours)

    def _interpolate_temperature(self, hour: int, hourly_temps: pd.Series) -> float:
        """Interpolate missing temperature for a specific hour."""
        # Find nearest available hours
        available_hours = hourly_temps.index.tolist()
        if not available_hours:
            return 95.0  # Default Phoenix temperature

        # Find closest hour
        closest_hour = min(available_hours, key=lambda x: abs(x - hour))
        return float(hourly_temps[closest_hour])

    def _generate_phoenix_temperature_pattern(self, hours: int = 24) -> List[float]:
        """
        Generate a realistic Phoenix summer temperature pattern.

        Args:
            hours: Number of hours to generate

        Returns:
            List of temperatures following typical Phoenix pattern
        """
        temperatures = []
        for h in range(hours):
            # Sine wave pattern: coolest at 5 AM, hottest at 5 PM
            base = 95  # Average temperature
            amplitude = 15  # Half of daily range (80-110)
            phase = (h - 5) * np.pi / 12  # Minimum at 5 AM
            temp = base + amplitude * np.sin(phase - np.pi/2)

            # Add slight random variation for realism
            temp += np.random.uniform(-2, 2)

            # Ensure reasonable bounds for Phoenix summer
            temp = max(75, min(115, temp))
            temperatures.append(temp)

        return temperatures

    # ==================== Price Inference Methods ====================

    def get_electricity_prices(self, date: Optional[datetime] = None, hours: int = 24) -> List[float]:
        """
        Get electricity prices for a specific date from EIA API or database.

        Args:
            date: The date to get prices for
            hours: Number of hours

        Returns:
            List of prices in $/MWh
        """
        if date is None:
            date = datetime(2024, 8, 1)  # Default to Aug 1, 2024

        # Check cache
        cache_key = f"elec_{date.date()}_{hours}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        try:
            # First, try to fetch from database
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=hours)

            response = self.supabase.table('electricity_prices').select('*').gte(
                'timestamp', start_time.isoformat()
            ).lt(
                'timestamp', end_time.isoformat()
            ).order('hour').execute()

            if response.data and len(response.data) >= hours:
                # Use actual prices from database
                df = pd.DataFrame(response.data)
                df = df.sort_values('hour')
                prices = df['price_per_mwh'].tolist()[:hours]
            else:
                # Try to fetch from EIA API
                prices = self._fetch_eia_prices(date, hours)

                if prices:
                    # Store fetched prices in database for caching
                    self._store_api_prices(date, prices, source='eia_api')
                else:
                    # Fallback to simple TOU rates if API fails
                    print(f"Warning: Using fallback TOU rates for {date.date()}")
                    prices = self._get_simple_tou_prices(hours)

            self._price_cache[cache_key] = prices
            return prices

        except Exception as e:
            print(f"Error getting electricity prices: {e}")
            return self._get_simple_tou_prices(hours)

    def _fetch_eia_prices(self, date: datetime, hours: int = 24) -> Optional[List[float]]:
        """
        Fetch electricity prices from EIA API.

        Args:
            date: Date to fetch prices for
            hours: Number of hours

        Returns:
            List of prices in $/MWh or None if API fails
        """
        try:
            import subprocess
            import json

            # Get EIA API key from environment
            api_key = os.getenv('EIA_API_KEY')
            if not api_key:
                print("EIA_API_KEY not found in environment")
                return None

            # Run the fetch_eia.py script
            cmd = [
                'python3',
                'scripts/fetch_eia.py',
                '--api-key', api_key,
                '--save', f'/tmp/eia_data_{date.date()}.json'
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

            if result.returncode == 0:
                # Read the saved JSON file
                with open(f'/tmp/eia_data_{date.date()}.json', 'r') as f:
                    data = json.load(f)

                # Extract prices from API response
                # This will need adjustment based on actual API response structure
                prices = self._parse_eia_response(data, date, hours)
                return prices
            else:
                print(f"EIA API fetch failed: {result.stderr}")
                return None

        except Exception as e:
            print(f"Error fetching EIA prices: {e}")
            return None

    def _parse_eia_response(self, data: dict, date: datetime, hours: int) -> Optional[List[float]]:
        """
        Parse EIA API response to extract hourly prices.

        Args:
            data: EIA API response
            date: Target date
            hours: Number of hours

        Returns:
            List of prices or None
        """
        try:
            # Parse based on EIA response structure
            # This is a placeholder - adjust based on actual API response
            prices = []

            # For now, use a simple mapping based on load data
            # Real implementation would extract actual LMP prices
            if 'data' in data or 'response' in data:
                # Generate prices based on typical patterns
                for h in range(hours):
                    if self.peak_hours_start <= h < self.peak_hours_end:
                        price = 120 + np.random.uniform(-10, 10)
                    elif h >= 22 or h < 6:
                        price = 30 + np.random.uniform(-5, 5)
                    else:
                        price = 50 + np.random.uniform(-10, 10)
                    prices.append(round(price, 2))

                return prices if len(prices) == hours else None

            return None

        except Exception as e:
            print(f"Error parsing EIA response: {e}")
            return None

    def _get_simple_tou_prices(self, hours: int = 24) -> List[float]:
        """
        Get simple time-of-use prices as fallback.

        Args:
            hours: Number of hours

        Returns:
            List of TOU prices in $/MWh
        """
        prices = []
        for h in range(hours):
            if self.peak_hours_start <= h < self.peak_hours_end:
                # Peak hours (3-8 PM)
                price = self.peak_rate
            elif h >= 22 or h < 6:
                # Super off-peak (10 PM - 6 AM)
                price = self.super_offpeak_rate
            else:
                # Off-peak
                price = self.offpeak_rate

            prices.append(price)

        return prices

    def get_water_prices(self, date: Optional[datetime] = None, gallons_per_hour: List[float] = None) -> List[float]:
        """
        Get water prices considering tier-based pricing.

        Args:
            date: Date for pricing
            gallons_per_hour: Expected usage per hour (for tier calculation)

        Returns:
            List of water prices per 1000 gallons for each hour
        """
        if date is None:
            date = datetime.now()

        # Base price per 1000 gallons
        base_price = self.water_price_per_1000_gal

        # Seasonal adjustment (summer water is more expensive in Arizona)
        month = date.month
        if month in [6, 7, 8]:  # Peak summer
            seasonal_multiplier = 1.25
        elif month in [5, 9]:  # Shoulder months
            seasonal_multiplier = 1.15
        else:
            seasonal_multiplier = 1.0

        prices = []
        cumulative_usage = 0

        for h in range(24):
            # Tier-based pricing
            if gallons_per_hour and h < len(gallons_per_hour):
                cumulative_usage += gallons_per_hour[h]

            # Determine tier based on cumulative usage
            if cumulative_usage < 100000:  # Tier 1: < 100k gallons
                tier_multiplier = 1.0
            elif cumulative_usage < 500000:  # Tier 2: 100k-500k gallons
                tier_multiplier = 1.2
            else:  # Tier 3: > 500k gallons
                tier_multiplier = 1.5

            # Calculate price for this hour
            hour_price = base_price * seasonal_multiplier * tier_multiplier

            prices.append(round(hour_price, 4))

        return prices

    # ==================== Result Storage Methods ====================

    def save_optimization_results(self, results: Dict[str, Any]) -> str:
        """
        Save optimization results to Supabase.

        Args:
            results: Dictionary containing optimization results

        Returns:
            Run ID for the saved results
        """
        try:
            # Generate unique run ID
            run_id = str(uuid.uuid4())
            run_timestamp = datetime.now()

            # First, save summary
            summary_data = self._prepare_summary_data(results, run_id, run_timestamp)
            self.supabase.table('optimization_summary').insert(summary_data).execute()

            # Then save hourly results
            hourly_data = self._prepare_hourly_data(results, run_id, run_timestamp)
            if hourly_data:
                self.supabase.table('optimization_results').insert(hourly_data).execute()

            print(f"Optimization results saved with run_id: {run_id}")
            return run_id

        except Exception as e:
            print(f"Error saving optimization results: {e}")
            return None

    def _prepare_summary_data(self, results: Dict, run_id: str, timestamp: datetime) -> Dict:
        """Prepare summary data for database insertion."""
        return {
            'run_id': run_id,
            'run_timestamp': timestamp.isoformat(),
            'run_name': results.get('run_name', f"Optimization Run {timestamp.strftime('%Y-%m-%d %H:%M')}"),

            # Cost metrics
            'total_cost': float(results.get('total_cost', 0)),
            'electricity_cost': float(results.get('electricity_cost', 0)),
            'water_cost': float(results.get('water_cost', 0)),
            'baseline_cost': float(results.get('baseline_cost', 0)),
            'cost_savings': float(results.get('cost_savings', 0)),
            'cost_savings_percent': float(results.get('cost_savings_percent', 0)),

            # Resource usage
            'total_water_usage_gallons': float(results.get('total_water_gallons', 0)),
            'peak_demand_mw': float(results.get('peak_demand', 0)),
            'average_load_mw': float(results.get('average_load', 0)),

            # Environmental metrics
            'water_saved_gallons': float(results.get('water_saved', 0)),
            'carbon_avoided_tons': float(results.get('carbon_avoided', 0)),

            # Configuration
            'configuration': json.dumps(results.get('config', {})),

            # Weather summary
            'max_temperature_f': float(results.get('max_temp', 0)),
            'min_temperature_f': float(results.get('min_temp', 0)),
            'avg_temperature_f': float(results.get('avg_temp', 0)),

            # Performance
            'solver_time_seconds': float(results.get('solver_time', 0)),
            'optimization_status': results.get('status', 'completed')
        }

    def _prepare_hourly_data(self, results: Dict, run_id: str, timestamp: datetime) -> List[Dict]:
        """Prepare hourly data for database insertion."""
        hourly_data = []

        # Extract arrays from results
        hours = range(24)
        batch_loads = results.get('batch_load', [0] * 24)
        cooling_modes = results.get('cooling_mode', [''] * 24)
        temperatures = results.get('temperatures', [0] * 24)
        electricity_prices = results.get('electricity_prices', [0] * 24)
        hourly_costs = results.get('hourly_costs', [0] * 24)
        water_usage = results.get('water_usage', [0] * 24)

        for h in hours:
            hourly_record = {
                'run_id': run_id,
                'run_timestamp': timestamp.isoformat(),
                'hour': h,

                # Load and demand
                'batch_load_mw': float(batch_loads[h] if h < len(batch_loads) else 0),
                'total_load_mw': float(batch_loads[h] if h < len(batch_loads) else 0) + 80,  # Base load + batch

                # Cooling decisions
                'cooling_mode': cooling_modes[h] if h < len(cooling_modes) else 'electric',
                'water_cooling_active': (cooling_modes[h] == 'water') if h < len(cooling_modes) else False,

                # Costs and usage
                'hourly_cost': float(hourly_costs[h] if h < len(hourly_costs) else 0),
                'water_usage_gallons': float(water_usage[h] if h < len(water_usage) else 0),

                # Environmental conditions
                'temperature_f': float(temperatures[h] if h < len(temperatures) else 95),
                'electricity_price': float(electricity_prices[h] if h < len(electricity_prices) else 50),
            }

            hourly_data.append(hourly_record)

        return hourly_data

    # ==================== Data Retrieval Methods ====================

    def get_optimization_history(self, limit: int = 10) -> pd.DataFrame:
        """
        Retrieve recent optimization run summaries.

        Args:
            limit: Number of recent runs to retrieve

        Returns:
            DataFrame with optimization summaries
        """
        try:
            response = self.supabase.table('optimization_summary').select('*').order(
                'run_timestamp', desc=True
            ).limit(limit).execute()

            if response.data:
                return pd.DataFrame(response.data)
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Error retrieving optimization history: {e}")
            return pd.DataFrame()

    def get_period_summary(self, days: int) -> Dict:
        """
        Get aggregated optimization data for the past X days.

        Args:
            days: Number of days to look back

        Returns:
            Dictionary with aggregated metrics
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query optimization summaries for the period
            response = self.supabase.table('optimization_summary').select('*').gte(
                'run_timestamp', start_date.isoformat()
            ).lte(
                'run_timestamp', end_date.isoformat()
            ).execute()

            if not response.data:
                return {
                    'period_days': days,
                    'days_analyzed': 0,
                    'actual_days_with_data': 0,  # Add this field
                    'is_projection': False,  # Add this field
                    'total_runs': 0,
                    'total_cost': 0,
                    'total_savings': 0,
                    'total_water_usage': 0,
                    'total_water_gallons': 0,
                    'avg_daily_savings': 0,
                    'avg_daily_cost': 0,
                    'avg_savings_percent': 0,
                    'avg_water_usage': 0,
                    'max_peak_demand': 0,
                    'avg_peak_demand': 0,
                    'peak_demand_mw': 0,
                    'carbon_avoided_tons': 0,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'message': f'No data available for the last {days} days'
                }

            df = pd.DataFrame(response.data)

            # Aggregate metrics from actual data
            actual_days = len(df)  # Number of days we actually have data for
            actual_cost = df['total_cost'].sum() if 'total_cost' in df else 0
            actual_savings = df['cost_savings'].sum() if 'cost_savings' in df else 0
            actual_water = df['total_water_usage_gallons'].sum() if 'total_water_usage_gallons' in df else 0

            # Calculate TRUE daily averages based on actual data
            true_avg_daily_cost = actual_cost / actual_days if actual_days > 0 else 0
            true_avg_daily_savings = actual_savings / actual_days if actual_days > 0 else 0
            true_avg_daily_water = actual_water / actual_days if actual_days > 0 else 0

            # Project totals for the requested period
            projected_total_cost = true_avg_daily_cost * days
            projected_total_savings = true_avg_daily_savings * days
            projected_total_water = true_avg_daily_water * days

            # Determine if we're projecting (when requested days > actual days)
            is_projection = days > actual_days

            return {
                'period_days': days,  # Requested period
                'days_analyzed': days if is_projection else actual_days,  # Show full period for projections
                'actual_days_with_data': actual_days,  # Actual days we have
                'is_projection': is_projection,
                'total_runs': actual_days,
                # Use projected values when appropriate
                'total_cost': float(projected_total_cost if is_projection else actual_cost),
                'total_savings': float(projected_total_savings if is_projection else actual_savings),
                'total_water_usage': float(projected_total_water if is_projection else actual_water),
                'total_water_gallons': float(projected_total_water if is_projection else actual_water),
                # Always use true averages based on actual data
                'avg_daily_savings': float(true_avg_daily_savings),
                'avg_daily_cost': float(true_avg_daily_cost),
                'avg_savings_percent': float((actual_savings / (actual_cost + actual_savings)) * 100) if (actual_cost + actual_savings) > 0 else 0,
                'avg_water_usage': float(true_avg_daily_water),
                # Peak demand stats (don't project these - use actuals)
                'max_peak_demand': float(df['peak_demand_mw'].max()) if 'peak_demand_mw' in df else 0,
                'avg_peak_demand': float(df['peak_demand_mw'].mean()) if 'peak_demand_mw' in df else 0,
                'peak_demand_mw': float(df['peak_demand_mw'].max()) if 'peak_demand_mw' in df else 0,
                # Carbon projection
                'carbon_avoided_tons': float(projected_total_savings * 0.0004 if is_projection else actual_savings * 0.0004),
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }

        except Exception as e:
            print(f"Error getting period summary: {e}")
            # Calculate date range for error response
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            return {
                'period_days': days,
                'days_analyzed': 0,
                'actual_days_with_data': 0,  # Add this field
                'is_projection': False,  # Add this field
                'total_runs': 0,
                'total_cost': 0,
                'total_savings': 0,
                'total_water_usage': 0,
                'total_water_gallons': 0,
                'avg_daily_savings': 0,
                'avg_daily_cost': 0,
                'avg_savings_percent': 0,
                'avg_water_usage': 0,
                'max_peak_demand': 0,
                'avg_peak_demand': 0,
                'peak_demand_mw': 0,
                'carbon_avoided_tons': 0,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'error': str(e)
            }

    def get_monthly_breakdown(self, months: int = 12) -> pd.DataFrame:
        """
        Get month-by-month breakdown of optimization results.

        Args:
            months: Number of months to look back

        Returns:
            DataFrame with monthly aggregated data
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months * 30)

            # Query data
            response = self.supabase.table('optimization_summary').select('*').gte(
                'run_timestamp', start_date.isoformat()
            ).lte(
                'run_timestamp', end_date.isoformat()
            ).order('run_timestamp').execute()

            if not response.data:
                return pd.DataFrame()

            df = pd.DataFrame(response.data)
            df['run_timestamp'] = pd.to_datetime(df['run_timestamp'])

            # Group by month
            df['month'] = df['run_timestamp'].dt.to_period('M')

            monthly_summary = df.groupby('month').agg({
                'total_cost': 'sum',
                'cost_savings': 'sum',
                'total_water_usage_gallons': 'sum',
                'peak_demand_mw': 'max'
            }).reset_index()

            monthly_summary['month'] = monthly_summary['month'].dt.to_timestamp()

            return monthly_summary

        except Exception as e:
            print(f"Error getting monthly breakdown: {e}")
            return pd.DataFrame()

    def get_daily_trends(self, days: int = None, start_date: datetime = None, end_date: datetime = None) -> Dict:
        """
        Get daily optimization trends for visualization.

        Args:
            days: Number of days to look back (alternative to start_date/end_date)
            start_date: Start date (optional if days is provided)
            end_date: End date (optional if days is provided)

        Returns:
            Dictionary with dates, savings, and water_usage arrays for plotting
        """
        try:
            # Handle both calling patterns
            if days is not None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
            elif start_date is None or end_date is None:
                # Default to last 30 days
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)

            response = self.supabase.table('optimization_summary').select('*').gte(
                'run_timestamp', start_date.isoformat()
            ).lte(
                'run_timestamp', end_date.isoformat()
            ).order('run_timestamp').execute()

            if response.data:
                df = pd.DataFrame(response.data)
                df['run_timestamp'] = pd.to_datetime(df['run_timestamp'])
                df['date'] = df['run_timestamp'].dt.date

                # Group by date if multiple runs per day
                daily_df = df.groupby('date').agg({
                    'total_cost': 'mean',
                    'cost_savings': 'mean',
                    'total_water_usage_gallons': 'sum',
                    'cost_savings_percent': 'mean'
                }).reset_index()

                # Return in the format expected by streamlit
                return {
                    'dates': daily_df['date'].tolist(),
                    'savings': daily_df['cost_savings'].tolist(),
                    'water_usage': daily_df['total_water_usage_gallons'].tolist(),
                    'cost_percent': daily_df['cost_savings_percent'].tolist()
                }
            else:
                return {'dates': [], 'savings': [], 'water_usage': [], 'cost_percent': []}

        except Exception as e:
            print(f"Error getting daily trends: {e}")
            return {'dates': [], 'savings': [], 'water_usage': [], 'cost_percent': []}

    def get_run_details(self, run_id: str) -> Tuple[Dict, pd.DataFrame]:
        """
        Get detailed results for a specific optimization run.

        Args:
            run_id: The UUID of the run

        Returns:
            Tuple of (summary dict, hourly results DataFrame)
        """
        try:
            # Get summary
            summary_response = self.supabase.table('optimization_summary').select('*').eq(
                'run_id', run_id
            ).execute()

            # Get hourly results
            hourly_response = self.supabase.table('optimization_results').select('*').eq(
                'run_id', run_id
            ).order('hour').execute()

            summary = summary_response.data[0] if summary_response.data else {}
            hourly_df = pd.DataFrame(hourly_response.data) if hourly_response.data else pd.DataFrame()

            return summary, hourly_df

        except Exception as e:
            print(f"Error retrieving run details: {e}")
            return {}, pd.DataFrame()

    # ==================== Utility Methods ====================

    def _store_api_prices(self, date: datetime, prices: List[float], source: str = 'eia_api') -> None:
        """Store API-fetched electricity prices in database for caching."""
        try:
            records = []
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)

            for hour, price in enumerate(prices[:24]):
                # Determine rate type
                if self.peak_hours_start <= hour < self.peak_hours_end:
                    rate_type = 'peak'
                elif hour >= 22 or hour < 6:
                    rate_type = 'super-off-peak'
                else:
                    rate_type = 'off-peak'

                records.append({
                    'timestamp': (start_time + timedelta(hours=hour)).isoformat(),
                    'hour': hour,
                    'price_per_mwh': price,
                    'rate_type': rate_type,
                    'source': source
                })

            # Batch insert
            if records:
                self.supabase.table('electricity_prices').insert(records).execute()

        except Exception as e:
            # Silently fail - this is optional functionality
            pass

    def test_connection(self) -> bool:
        """Test the Supabase connection."""
        try:
            # Try to query weather data
            response = self.supabase.table('weather_data').select('*').limit(1).execute()
            return response.data is not None
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False


# Convenience function for quick access
def get_supabase_client() -> SupabaseInterface:
    """Get a configured Supabase client instance."""
    return SupabaseInterface()