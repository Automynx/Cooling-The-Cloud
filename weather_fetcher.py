"""
Weather data fetcher module for Iowa Environmental Mesonet ASOS API
"""

import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
import time
import logging
from io import StringIO

import config

logger = logging.getLogger(__name__)


class WeatherFetcher:
    """Fetches weather data from Iowa Environmental Mesonet ASOS API"""

    def __init__(self):
        self.base_url = config.IEM_BASE_URL
        self.station = config.STATION_CODE
        self.timeout = config.API_TIMEOUT
        self.max_retries = config.MAX_RETRIES
        self.retry_delay = config.RETRY_DELAY

    def build_url(self, start_date: datetime, end_date: datetime) -> str:
        """
        Build the API URL with required parameters

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch

        Returns:
            Complete API URL
        """
        params = {
            'station': self.station,
            'data': config.REQUIRED_FIELDS,  # Will be expanded to multiple data params
            'year1': start_date.year,
            'month1': start_date.month,
            'day1': start_date.day,
            'year2': end_date.year,
            'month2': end_date.month,
            'day2': end_date.day,
            'tz': 'UTC',
            'format': 'onlycomma',
            'latlon': 'no',
            'elev': 'no',
            'missing': 'null',
            'trace': 'null',
            'direct': 'no',
            'report_type': '2'  # Hourly data
        }

        # Build URL with multiple data parameters
        param_strings = []
        param_strings.append(f"station={params['station']}")

        # Add each data field as a separate parameter
        for field in config.REQUIRED_FIELDS:
            param_strings.append(f"data={field}")

        # Add date parameters
        param_strings.extend([
            f"year1={params['year1']}",
            f"month1={params['month1']}",
            f"day1={params['day1']}",
            f"year2={params['year2']}",
            f"month2={params['month2']}",
            f"day2={params['day2']}",
            f"tz={params['tz']}",
            f"format={params['format']}",
            f"latlon={params['latlon']}",
            f"elev={params['elev']}",
            f"missing={params['missing']}",
            f"trace={params['trace']}",
            f"direct={params['direct']}",
            f"report_type={params['report_type']}"
        ])

        url = f"{self.base_url}?{'&'.join(param_strings)}"
        logger.debug(f"Built URL: {url}")
        return url

    def fetch_data(self, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Fetch weather data for the specified date range

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch

        Returns:
            DataFrame with weather data or None if fetch fails
        """
        url = self.build_url(start_date, end_date)

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching data from {start_date.date()} to {end_date.date()} (attempt {attempt + 1}/{self.max_retries})")

                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()

                # Parse CSV data
                if response.text:
                    df = pd.read_csv(StringIO(response.text))

                    # Check if we got data
                    if df.empty:
                        logger.warning(f"No data returned for period {start_date.date()} to {end_date.date()}")
                        return None

                    # Clean column names (remove any extra spaces)
                    df.columns = df.columns.str.strip()

                    # Parse the 'valid' column as datetime
                    if 'valid' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['valid'], utc=True)
                        df = df.drop(columns=['valid'])
                    else:
                        logger.error("No 'valid' timestamp column in response")
                        return None

                    # Rename columns to match our database schema
                    column_mapping = {
                        'tmpf': 'temperature_f',
                        'relh': 'humidity_percent'
                    }
                    df = df.rename(columns=column_mapping)

                    # Add station column
                    df['station'] = self.station

                    # Select only required columns
                    required_cols = ['station', 'timestamp', 'temperature_f', 'humidity_percent']
                    df = df[required_cols]

                    # Convert null values to None for proper database handling
                    df = df.replace('null', None)
                    df = df.replace('', None)

                    # Convert numeric columns
                    df['temperature_f'] = pd.to_numeric(df['temperature_f'], errors='coerce')
                    df['humidity_percent'] = pd.to_numeric(df['humidity_percent'], errors='coerce')

                    # Remove completely null rows (where both temp and humidity are null)
                    df = df.dropna(subset=['temperature_f', 'humidity_percent'], how='all')

                    logger.info(f"Successfully fetched {len(df)} records")
                    return df
                else:
                    logger.warning("Empty response from API")
                    return None

            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)} (attempt {attempt + 1}/{self.max_retries})")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)

        logger.error(f"Failed to fetch data after {self.max_retries} attempts")
        return None

    def fetch_date_range_in_batches(self, start_date: datetime, end_date: datetime,
                                   batch_days: int = None) -> List[pd.DataFrame]:
        """
        Fetch weather data in batches to avoid timeouts

        Args:
            start_date: Start date for data fetch
            end_date: End date for data fetch
            batch_days: Number of days per batch

        Returns:
            List of DataFrames with weather data
        """
        if batch_days is None:
            batch_days = config.BATCH_SIZE_DAYS

        all_data = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=batch_days), end_date)

            logger.info(f"Fetching batch: {current_start.date()} to {current_end.date()}")
            df = self.fetch_data(current_start, current_end)

            if df is not None and not df.empty:
                all_data.append(df)
                logger.info(f"Batch complete: {len(df)} records fetched")
            else:
                logger.warning(f"No data for batch: {current_start.date()} to {current_end.date()}")

            # Add a small delay between batches to be respectful to the API
            time.sleep(1)

            current_start = current_end + timedelta(days=1)

        logger.info(f"Total batches fetched: {len(all_data)}")
        return all_data

    def get_latest_available_data(self, hours: int = 24) -> Optional[pd.DataFrame]:
        """
        Get the most recent available data

        Args:
            hours: Number of hours of recent data to fetch

        Returns:
            DataFrame with recent weather data
        """
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(hours=hours)

        logger.info(f"Fetching latest {hours} hours of data")
        return self.fetch_data(start_date, end_date)