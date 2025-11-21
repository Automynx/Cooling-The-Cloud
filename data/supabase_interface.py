"""
Supabase Interface for Production Data Center Optimization
Handles all database operations for real-time optimization
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
import json
from decimal import Decimal

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.api.store_to_postgres import connect_db


class SupabaseInterface:
    """Production interface for Supabase database operations."""

    def __init__(self):
        """Initialize Supabase connection."""
        self.conn = None
        self.connect()

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = connect_db()
            print("✅ Connected to Supabase database")
        except Exception as e:
            print(f"❌ Failed to connect to Supabase: {e}")
            raise

    def test_connection(self) -> bool:
        """Test if connection is active."""
        try:
            if self.conn:
                cur = self.conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                return True
        except:
            return False
        return False

    def ensure_connection(self):
        """Ensure connection is active, reconnect if needed."""
        if not self.test_connection():
            self.connect()

    def fetch_weather_data(self, date: datetime, hours: int = 24) -> List[float]:
        """
        Fetch real weather data from database or generate Phoenix pattern.

        Args:
            date: Date to fetch data for
            hours: Number of hours to fetch

        Returns:
            List of hourly temperatures in Fahrenheit
        """
        self.ensure_connection()

        # First try to get real weather data if available
        try:
            query = """
            SELECT
                EXTRACT(HOUR FROM timestamp) as hour,
                AVG(temperature_f) as avg_temp
            FROM weather_data
            WHERE DATE(timestamp) = %s
            GROUP BY EXTRACT(HOUR FROM timestamp)
            ORDER BY hour
            LIMIT %s
            """

            cur = self.conn.cursor()
            cur.execute(query, (date.date(), hours))
            results = cur.fetchall()
            cur.close()

            if results and len(results) > 0:
                temperatures = [float(row[1]) for row in results]
                # Pad with Phoenix pattern if not enough hours
                while len(temperatures) < hours:
                    temperatures.append(self._generate_phoenix_temp(len(temperatures)))
                return temperatures[:hours]
        except Exception as e:
            print(f"Could not fetch weather data: {e}")

        # Fallback to realistic Phoenix pattern based on month
        return self._generate_phoenix_pattern(date, hours)

    def _generate_phoenix_pattern(self, date: datetime, hours: int = 24) -> List[float]:
        """Generate realistic Phoenix temperature pattern based on month."""
        month = date.month

        # Phoenix monthly averages (high/low)
        monthly_temps = {
            1: (67, 45),   # January
            2: (71, 49),   # February
            3: (77, 54),   # March
            4: (85, 60),   # April
            5: (94, 69),   # May
            6: (104, 79),  # June
            7: (106, 84),  # July
            8: (104, 83),  # August
            9: (98, 77),   # September
            10: (88, 65),  # October
            11: (76, 53),  # November
            12: (66, 45),  # December
        }

        high, low = monthly_temps.get(month, (95, 75))

        temperatures = []
        for hour in range(hours):
            # Sine wave pattern with minimum at 5 AM, maximum at 5 PM
            base = (high + low) / 2
            amplitude = (high - low) / 2
            phase = (hour - 5) * np.pi / 12
            temp = base + amplitude * np.sin(phase - np.pi/2)
            # Add slight random variation
            temp += np.random.uniform(-2, 2)
            temperatures.append(max(low - 5, min(high + 5, temp)))

        return temperatures

    def _generate_phoenix_temp(self, hour: int, month: int = 8) -> float:
        """Generate single hour Phoenix temperature."""
        pattern = self._generate_phoenix_pattern(datetime(2024, month, 1), 24)
        return pattern[hour % 24]

    def get_electricity_prices(self, date: datetime, hours: int = 24) -> List[float]:
        """
        Fetch real electricity prices or calculate from interchange data.

        Args:
            date: Date to fetch prices for
            hours: Number of hours to fetch

        Returns:
            List of hourly prices in $/MWh
        """
        self.ensure_connection()

        # First check if we have direct price data
        try:
            query = """
            SELECT
                hour,
                price_per_mwh
            FROM electricity_prices
            WHERE DATE(timestamp) = %s
            ORDER BY hour
            LIMIT %s
            """

            cur = self.conn.cursor()
            cur.execute(query, (date.date(), hours))
            results = cur.fetchall()
            cur.close()

            if results and len(results) >= hours:
                return [float(row[1]) for row in results]
        except:
            pass

        # Calculate prices from interchange data
        try:
            # Get Arizona average price
            price_query = """
            SELECT AVG(price_per_mwh) as avg_price
            FROM eia_az_price
            WHERE sectorid = 'ALL'
            """

            cur = self.conn.cursor()
            cur.execute(price_query)
            result = cur.fetchone()
            base_price = float(result[0]) if result and result[0] else 128.4
            cur.close()

            # Get interchange patterns for price variation
            interchange_query = """
            SELECT
                EXTRACT(HOUR FROM period) as hour,
                AVG(value) as avg_interchange
            FROM eia_interchange
            WHERE DATE(period) = %s
                AND (fromba IN ('AZPS', 'SRP', 'TEPC')
                     OR toba IN ('AZPS', 'SRP', 'TEPC'))
            GROUP BY EXTRACT(HOUR FROM period)
            ORDER BY hour
            """

            cur = self.conn.cursor()
            cur.execute(interchange_query, (date.date(),))
            interchange_data = cur.fetchall()
            cur.close()

            # Generate prices based on interchange patterns
            prices = []
            for hour in range(hours):
                # Find interchange for this hour
                interchange = 0
                for h, val in interchange_data:
                    if int(h) == hour:
                        interchange = float(val) if val else 0
                        break

                # Calculate price based on hour and interchange
                if 15 <= hour < 20:  # Peak hours 3-8 PM
                    price_mult = 1.3 + (abs(interchange) / 10000) * 0.2
                elif hour >= 22 or hour < 6:  # Off-peak
                    price_mult = 0.6 + (abs(interchange) / 10000) * 0.1
                else:  # Mid-peak
                    price_mult = 1.0 + (abs(interchange) / 10000) * 0.15

                prices.append(base_price * price_mult)

            return prices

        except Exception as e:
            print(f"Error calculating prices from interchange: {e}")

        # Fallback to time-of-use pattern with Arizona rates
        return self._generate_tou_prices(hours)

    def _generate_tou_prices(self, hours: int = 24) -> List[float]:
        """Generate time-of-use prices based on Arizona rate structure."""
        prices = []
        for hour in range(hours):
            if 15 <= hour < 20:  # Peak: 3-8 PM
                price = 167  # Summer peak rate
            elif hour >= 22 or hour < 6:  # Super off-peak
                price = 77  # Night rate
            else:  # Off-peak
                price = 128  # Day rate

            # Add small variation
            price += np.random.uniform(-2, 2)
            prices.append(price)

        return prices

    def get_water_prices(self, date: datetime) -> List[float]:
        """
        Get water prices for the specified date.

        Args:
            date: Date to fetch water prices for

        Returns:
            List of 24 hourly water prices
        """
        self.ensure_connection()

        try:
            query = """
            SELECT price_per_thousand_gallons, seasonal_multiplier
            FROM water_prices
            WHERE date <= %s
            ORDER BY date DESC
            LIMIT 1
            """

            cur = self.conn.cursor()
            cur.execute(query, (date.date(),))
            result = cur.fetchone()
            cur.close()

            if result:
                base_price = float(result[0])
                multiplier = float(result[1]) if result[1] else 1.0
                price = base_price * multiplier
            else:
                # Default Phoenix water rate
                price = 3.24

            # Return same price for all hours
            return [price] * 24

        except Exception as e:
            print(f"Error fetching water prices: {e}")
            return [3.24] * 24  # Default rate

    def save_optimization_results(self, results: Dict) -> Optional[str]:
        """
        Save optimization results to database.

        Args:
            results: Dictionary containing optimization results

        Returns:
            Run ID if successful, None otherwise
        """
        self.ensure_connection()

        try:
            run_id = str(uuid.uuid4())

            # Prepare data with proper type conversions
            summary_data = {
                'run_id': run_id,
                'run_timestamp': datetime.now(),
                'run_name': f"Optimization {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                'total_cost': float(results.get('total_cost', 0)),
                'electricity_cost': float(results.get('electricity_cost', 0)),
                'water_cost': float(results.get('water_cost', 0)),
                'baseline_cost': float(results.get('baseline_cost', 0)),
                'cost_savings': float(results.get('cost_savings', 0)),
                'cost_savings_percent': float(results.get('cost_savings_percent', 0)),
                'total_water_usage_gallons': float(results.get('total_water_gallons', 0)),
                'peak_demand_mw': float(results.get('peak_demand', 0)),
                'water_saved_gallons': float(results.get('water_saved', 0)),
                'carbon_avoided_tons': float(results.get('carbon_avoided', 0)),
                'optimization_status': 'completed'
            }

            # Insert summary
            summary_query = """
            INSERT INTO optimization_summary (
                run_id, run_timestamp, run_name,
                total_cost, electricity_cost, water_cost,
                baseline_cost, cost_savings, cost_savings_percent,
                total_water_usage_gallons, peak_demand_mw,
                water_saved_gallons, carbon_avoided_tons,
                optimization_status
            ) VALUES (
                %(run_id)s, %(run_timestamp)s, %(run_name)s,
                %(total_cost)s, %(electricity_cost)s, %(water_cost)s,
                %(baseline_cost)s, %(cost_savings)s, %(cost_savings_percent)s,
                %(total_water_usage_gallons)s, %(peak_demand_mw)s,
                %(water_saved_gallons)s, %(carbon_avoided_tons)s,
                %(optimization_status)s
            )
            """

            cur = self.conn.cursor()
            cur.execute(summary_query, summary_data)

            # Save hourly details if available
            if 'hourly_data' in results:
                for hour_data in results['hourly_data']:
                    detail_query = """
                    INSERT INTO optimization_results (
                        run_id, run_timestamp, hour, batch_load_mw, total_load_mw,
                        cooling_mode, water_cooling_active,
                        hourly_cost, electricity_cost, water_cost,
                        water_usage_gallons, temperature_f, electricity_price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    water_cooling = hour_data.get('water_cooling', 0)
                    elec_cost = hour_data.get('electricity_cost', 0)
                    water_cost = hour_data.get('water_cost', 0)

                    cur.execute(detail_query, (
                        run_id,
                        datetime.now(),  # run_timestamp
                        hour_data['hour'],
                        float(hour_data.get('batch_load_mw', 0)),
                        float(hour_data.get('total_load_mw', 0)),
                        'water' if water_cooling else 'electric',
                        bool(water_cooling),  # water_cooling_active
                        float(elec_cost + water_cost),  # hourly_cost
                        float(elec_cost),  # electricity_cost
                        float(water_cost),  # water_cost
                        float(water_cooling * 120),  # water_usage_gallons
                        float(hour_data.get('temperature', 0)),
                        float(hour_data.get('electricity_price', 0))
                    ))

            self.conn.commit()
            cur.close()

            return run_id

        except Exception as e:
            print(f"Error saving optimization results: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def get_optimization_history(self, limit: int = 10) -> pd.DataFrame:
        """
        Retrieve recent optimization runs.

        Args:
            limit: Number of recent runs to retrieve

        Returns:
            DataFrame with optimization history
        """
        self.ensure_connection()

        try:
            query = """
            SELECT
                run_id,
                run_timestamp,
                run_name,
                total_cost,
                cost_savings,
                cost_savings_percent,
                total_water_usage_gallons,
                peak_demand_mw,
                carbon_avoided_tons,
                optimization_status
            FROM optimization_summary
            ORDER BY run_timestamp DESC
            LIMIT %s
            """

            df = pd.read_sql_query(query, self.conn, params=(limit,))
            return df

        except Exception as e:
            print(f"Error fetching optimization history: {e}")
            return pd.DataFrame()

    def get_period_summary(self, days: int) -> Dict:
        """
        Get summary statistics for a period.

        Args:
            days: Number of days to summarize

        Returns:
            Dictionary with period statistics
        """
        self.ensure_connection()

        try:
            query = """
            SELECT
                COUNT(*) as runs,
                SUM(cost_savings) as total_savings,
                AVG(cost_savings) as avg_daily_savings,
                AVG(cost_savings_percent) as avg_savings_percent,
                SUM(total_water_usage_gallons) as total_water_usage,
                AVG(total_water_usage_gallons) as avg_water_usage,
                MAX(peak_demand_mw) as max_peak_demand,
                AVG(peak_demand_mw) as avg_peak_demand,
                SUM(carbon_avoided_tons) as total_carbon_avoided
            FROM optimization_summary
            WHERE run_timestamp >= CURRENT_DATE - INTERVAL '%s days'
                AND optimization_status = 'completed'
            """

            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, (days,))
            result = cur.fetchone()
            cur.close()

            if result and result['runs'] > 0:
                # Check if we need to project
                actual_days = result['runs']
                is_projection = actual_days < days

                if is_projection:
                    # Project values to full period
                    projection_factor = days / actual_days
                    total_savings = float(result['avg_daily_savings'] or 0) * days
                    total_water = float(result['avg_water_usage'] or 0) * days
                else:
                    total_savings = float(result['total_savings'] or 0)
                    total_water = float(result['total_water_usage'] or 0)

                return {
                    'days_analyzed': days,
                    'actual_days_with_data': actual_days,
                    'is_projection': is_projection,
                    'total_savings': total_savings,
                    'avg_daily_savings': float(result['avg_daily_savings'] or 0),
                    'avg_savings_percent': float(result['avg_savings_percent'] or 0),
                    'total_water_usage': total_water,
                    'avg_water_usage': float(result['avg_water_usage'] or 0),
                    'max_peak_demand': float(result['max_peak_demand'] or 0),
                    'avg_peak_demand': float(result['avg_peak_demand'] or 0),
                    'total_carbon_avoided': float(result['total_carbon_avoided'] or 0) * (projection_factor if is_projection else 1)
                }

            return {'total_savings': 0}

        except Exception as e:
            print(f"Error getting period summary: {e}")
            return {'total_savings': 0}

    def get_monthly_breakdown(self, months: int = 6) -> pd.DataFrame:
        """
        Get monthly breakdown of optimization results.

        Args:
            months: Number of months to retrieve

        Returns:
            DataFrame with monthly statistics
        """
        self.ensure_connection()

        try:
            query = """
            SELECT
                DATE_TRUNC('month', run_timestamp) as month,
                COUNT(*) as runs,
                SUM(cost_savings) as cost_savings,
                AVG(cost_savings_percent) as avg_savings_percent,
                SUM(total_water_usage_gallons) as water_usage,
                AVG(peak_demand_mw) as avg_peak_demand
            FROM optimization_summary
            WHERE run_timestamp >= CURRENT_DATE - INTERVAL '%s months'
                AND optimization_status = 'completed'
            GROUP BY DATE_TRUNC('month', run_timestamp)
            ORDER BY month DESC
            """

            df = pd.read_sql_query(query, self.conn, params=(months,))
            return df

        except Exception as e:
            print(f"Error getting monthly breakdown: {e}")
            return pd.DataFrame()

    def get_daily_trends(self, days: int = 30) -> Dict:
        """
        Get daily trend data for charts.

        Args:
            days: Number of days to retrieve

        Returns:
            Dictionary with trend data
        """
        self.ensure_connection()

        try:
            query = """
            SELECT
                DATE(run_timestamp) as date,
                AVG(cost_savings) as daily_savings,
                AVG(total_water_usage_gallons) as water_usage,
                AVG(peak_demand_mw) as peak_demand
            FROM optimization_summary
            WHERE run_timestamp >= CURRENT_DATE - INTERVAL '%s days'
                AND optimization_status = 'completed'
            GROUP BY DATE(run_timestamp)
            ORDER BY date
            """

            cur = self.conn.cursor()
            cur.execute(query, (days,))
            results = cur.fetchall()
            cur.close()

            if results:
                dates = [row[0] for row in results]
                savings = [float(row[1] or 0) for row in results]
                water = [float(row[2] or 0) for row in results]
                peak = [float(row[3] or 0) for row in results]

                return {
                    'dates': dates,
                    'savings': savings,
                    'water_usage': water,
                    'peak_demand': peak
                }

            return {'dates': [], 'savings': [], 'water_usage': [], 'peak_demand': []}

        except Exception as e:
            print(f"Error getting daily trends: {e}")
            return {'dates': [], 'savings': [], 'water_usage': [], 'peak_demand': []}

    def __del__(self):
        """Clean up database connection."""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass