"""
Database module for Supabase operations
"""

from supabase import create_client, Client
import pandas as pd
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

import config

logger = logging.getLogger(__name__)


class SupabaseManager:
    """Manages all interactions with Supabase database"""

    def __init__(self):
        """Initialize Supabase client"""
        try:
            self.client: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            self.table_name = config.TABLE_NAME
            logger.info("Successfully connected to Supabase")
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {str(e)}")
            raise

    def create_table_if_not_exists(self) -> bool:
        """
        Create the weather_data table if it doesn't exist
        Note: This might require admin privileges. Alternative is to create via Supabase dashboard

        Returns:
            True if successful, False otherwise
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            station VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            temperature_f REAL,
            humidity_percent REAL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(station, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp ON {self.table_name}(timestamp);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_station ON {self.table_name}(station);
        """

        try:
            # Note: Direct SQL execution might not be available in all Supabase setups
            # This is provided as reference - you might need to create the table via Supabase dashboard
            logger.info(f"Table {self.table_name} creation SQL generated (execute via Supabase SQL editor if needed):")
            logger.info(create_table_sql)
            return True
        except Exception as e:
            logger.error(f"Error with table creation: {str(e)}")
            return False

    def insert_data(self, df: pd.DataFrame) -> bool:
        """
        Insert weather data into Supabase

        Args:
            df: DataFrame containing weather data

        Returns:
            True if successful, False otherwise
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for insertion")
            return False

        try:
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')

            # Convert timestamp to ISO format string for Supabase
            for record in records:
                if pd.notna(record.get('timestamp')):
                    record['timestamp'] = record['timestamp'].isoformat()

                # Clean up None values for numeric fields
                if pd.isna(record.get('temperature_f')):
                    record['temperature_f'] = None
                if pd.isna(record.get('humidity_percent')):
                    record['humidity_percent'] = None

            # Insert data using upsert to handle duplicates
            response = self.client.table(self.table_name).upsert(
                records,
                on_conflict='station,timestamp'  # Handle duplicates based on unique constraint
            ).execute()

            logger.info(f"Successfully inserted/updated {len(records)} records")
            return True

        except Exception as e:
            logger.error(f"Failed to insert data: {str(e)}")
            return False

    def batch_insert(self, dataframes: List[pd.DataFrame], batch_size: int = 1000) -> int:
        """
        Insert multiple DataFrames in batches

        Args:
            dataframes: List of DataFrames to insert
            batch_size: Number of records per batch

        Returns:
            Total number of records inserted
        """
        total_inserted = 0

        for df in dataframes:
            if df.empty:
                continue

            # Split large DataFrames into smaller batches
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]

                if self.insert_data(batch_df):
                    total_inserted += len(batch_df)
                    logger.info(f"Batch inserted: {len(batch_df)} records (Total: {total_inserted})")
                else:
                    logger.error(f"Failed to insert batch at index {i}")

        return total_inserted

    def get_latest_timestamp(self) -> Optional[datetime]:
        """
        Get the most recent timestamp in the database for the station

        Returns:
            Latest timestamp or None if no data exists
        """
        try:
            response = self.client.table(self.table_name).select('timestamp').eq(
                'station', config.STATION_CODE
            ).order('timestamp', desc=True).limit(1).execute()

            if response.data and len(response.data) > 0:
                timestamp_str = response.data[0]['timestamp']
                # Explicitly parse as UTC timezone-aware
                return pd.to_datetime(timestamp_str, utc=True)
            else:
                logger.info("No existing data found in database")
                return None

        except Exception as e:
            logger.error(f"Failed to get latest timestamp: {str(e)}")
            return None

    def get_data_count(self) -> int:
        """
        Get the total count of records for the station

        Returns:
            Number of records
        """
        try:
            response = self.client.table(self.table_name).select(
                'id', count='exact'
            ).eq('station', config.STATION_CODE).execute()

            return response.count if response.count else 0

        except Exception as e:
            logger.error(f"Failed to get data count: {str(e)}")
            return 0

    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the data in the database

        Returns:
            Dictionary with summary statistics
        """
        try:
            # Get date range
            response = self.client.table(self.table_name).select(
                'timestamp'
            ).eq('station', config.STATION_CODE).order(
                'timestamp', desc=False
            ).limit(1).execute()

            min_date = None
            if response.data and len(response.data) > 0:
                # Explicitly parse as UTC timezone-aware
                min_date = pd.to_datetime(response.data[0]['timestamp'], utc=True)

            max_date = self.get_latest_timestamp()
            count = self.get_data_count()

            # Get sample of recent data with values
            recent_data = self.client.table(self.table_name).select(
                'timestamp,temperature_f,humidity_percent'
            ).eq('station', config.STATION_CODE).order(
                'timestamp', desc=True
            ).limit(10).execute()

            summary = {
                'station': config.STATION_CODE,
                'total_records': count,
                'date_range': {
                    'start': min_date.isoformat() if min_date else None,
                    'end': max_date.isoformat() if max_date else None
                },
                'recent_samples': recent_data.data if recent_data.data else []
            }

            return summary

        except Exception as e:
            logger.error(f"Failed to get data summary: {str(e)}")
            return {
                'station': config.STATION_CODE,
                'error': str(e)
            }

    def query_data(self, start_date: datetime = None, end_date: datetime = None,
                  limit: int = None) -> Optional[pd.DataFrame]:
        """
        Query weather data from the database

        Args:
            start_date: Start date for query
            end_date: End date for query
            limit: Maximum number of records to return

        Returns:
            DataFrame with weather data or None if query fails
        """
        try:
            query = self.client.table(self.table_name).select('*').eq(
                'station', config.STATION_CODE
            )

            if start_date:
                query = query.gte('timestamp', start_date.isoformat())
            if end_date:
                query = query.lte('timestamp', end_date.isoformat())

            query = query.order('timestamp', desc=False)

            if limit:
                query = query.limit(limit)

            response = query.execute()

            if response.data:
                df = pd.DataFrame(response.data)
                # Explicitly parse as UTC timezone-aware
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to query data: {str(e)}")
            return None

    def delete_duplicates(self) -> int:
        """
        Remove duplicate entries (keeping the most recent created_at)

        Returns:
            Number of duplicates removed
        """
        # This would typically be done via SQL, but Supabase client has limitations
        # Consider implementing this via Supabase SQL editor if needed
        logger.info("Duplicate removal should be handled via unique constraints")
        return 0