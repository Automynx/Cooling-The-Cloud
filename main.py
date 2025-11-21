#!/usr/bin/env python3
"""
Main orchestration script for CoolTheCloud weather data pipeline
Fetches weather data from Iowa Environmental Mesonet and stores in Supabase
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
import pandas as pd

from weather_fetcher import WeatherFetcher
from database import SupabaseManager
import utils
import config


def main():
    """Main execution function"""

    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Fetch weather data for cooling system optimization'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=config.DEFAULT_START_DATE,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=config.DEFAULT_END_DATE,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--batch-days',
        type=int,
        default=config.BATCH_SIZE_DAYS,
        help='Number of days per batch'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='Fetch only the latest 24 hours of data'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show database summary without fetching new data'
    )
    parser.add_argument(
        '--create-table',
        action='store_true',
        help='Generate SQL for table creation'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default=config.LOG_LEVEL,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Set up logging
    logger = utils.setup_logging(log_level=args.log_level)

    # Validate environment
    if not utils.validate_environment():
        logger.error("Environment validation failed")
        sys.exit(1)

    logger.info("="*50)
    logger.info("CoolTheCloud Weather Data Pipeline")
    logger.info(f"Station: {config.STATION_NAME} ({config.STATION_CODE})")
    logger.info("="*50)

    try:
        # Initialize database manager
        db_manager = SupabaseManager()

        # Handle table creation request
        if args.create_table:
            logger.info("Generating table creation SQL...")
            db_manager.create_table_if_not_exists()
            print("\n" + "="*50)
            print("SUPABASE TABLE CREATION SQL")
            print("Copy and execute this in your Supabase SQL editor:")
            print("="*50)
            print(f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    station VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_f REAL,
    humidity_percent REAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(station, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_{config.TABLE_NAME}_timestamp ON {config.TABLE_NAME}(timestamp);
CREATE INDEX IF NOT EXISTS idx_{config.TABLE_NAME}_station ON {config.TABLE_NAME}(station);
            """)
            print("="*50)
            return

        # Handle summary request
        if args.summary:
            logger.info("Fetching database summary...")
            summary = db_manager.get_data_summary()

            print("\n" + "="*50)
            print("DATABASE SUMMARY")
            print("="*50)
            print(f"Station: {summary.get('station', 'N/A')}")
            print(f"Total Records: {summary.get('total_records', 0):,}")

            if summary.get('date_range'):
                date_range = summary['date_range']
                print(f"Date Range: {date_range.get('start', 'N/A')} to {date_range.get('end', 'N/A')}")

            if summary.get('recent_samples'):
                print("\nRecent Samples:")
                for sample in summary['recent_samples'][:5]:
                    print(f"  {sample['timestamp']}: {sample.get('temperature_f', 'N/A')}°F, {sample.get('humidity_percent', 'N/A')}%")

            print("="*50)
            return

        # Initialize weather fetcher
        fetcher = WeatherFetcher()

        # Determine date range
        if args.latest:
            # Fetch latest 24 hours
            logger.info("Fetching latest 24 hours of data...")
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(hours=24)
        else:
            # Parse provided date range
            start_date, end_date = utils.parse_date_range(args.start_date, args.end_date)
            logger.info(f"Date range: {start_date.date()} to {end_date.date()}")

            # Check if we should fetch incremental updates
            # Note: For now, commenting out the incremental logic to allow fetching historical data
            # latest_timestamp = db_manager.get_latest_timestamp()
            # if latest_timestamp:
            #     logger.info(f"Latest data in database: {latest_timestamp}")
            #     if latest_timestamp >= end_date:
            #         logger.info("Database already contains data up to the requested end date")
            #         return
            #     elif latest_timestamp > start_date:
            #         logger.info(f"Adjusting start date to {latest_timestamp + timedelta(hours=1)} for incremental update")
            #         start_date = latest_timestamp + timedelta(hours=1)

        # Calculate total days
        total_days = (end_date - start_date).days + 1
        logger.info(f"Fetching {total_days} days of data")

        # Start timing
        start_time = time.time()

        # Fetch data in batches
        logger.info(f"Fetching data in {args.batch_days}-day batches...")
        all_data = fetcher.fetch_date_range_in_batches(
            start_date, end_date, args.batch_days
        )

        if not all_data:
            logger.warning("No data fetched from API")
            return

        # Combine all DataFrames
        logger.info(f"Combining {len(all_data)} batches...")
        combined_df = pd.concat(all_data, ignore_index=True)

        # Remove duplicates if any
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['station', 'timestamp'])
        if initial_count > len(combined_df):
            logger.info(f"Removed {initial_count - len(combined_df)} duplicate records")

        # Print summary statistics
        utils.print_summary_statistics(combined_df)

        # Store in database
        logger.info("Storing data in Supabase...")
        total_inserted = db_manager.batch_insert([combined_df])

        # Calculate execution time
        execution_time = time.time() - start_time

        # Final summary
        logger.info("="*50)
        logger.info("EXECUTION COMPLETE")
        logger.info(f"Total records processed: {len(combined_df):,}")
        logger.info(f"Total records inserted: {total_inserted:,}")
        logger.info(f"Execution time: {utils.format_duration(execution_time)}")
        logger.info(f"Database now contains: {db_manager.get_data_count():,} records")
        logger.info("="*50)

        # Analyze cooling efficiency for recent data
        if not combined_df.empty and 'temperature_f' in combined_df.columns and 'humidity_percent' in combined_df.columns:
            recent_data = combined_df.tail(24).dropna(subset=['temperature_f', 'humidity_percent'])
            if not recent_data.empty:
                print("\nCOOLING SYSTEM ANALYSIS (Last 24 Hours):")
                print("-"*50)
                evap_hours = 0
                chiller_hours = 0
                no_cooling_hours = 0

                for _, row in recent_data.iterrows():
                    analysis = utils.analyze_cooling_efficiency(
                        row['temperature_f'],
                        row['humidity_percent']
                    )
                    if analysis['recommended_system'] == 'evaporative':
                        evap_hours += 1
                    elif analysis['recommended_system'] == 'electric_chiller':
                        chiller_hours += 1
                    else:
                        no_cooling_hours += 1

                print(f"Evaporative Cooling Recommended: {evap_hours} hours")
                print(f"Electric Chiller Recommended: {chiller_hours} hours")
                print(f"No Cooling Needed: {no_cooling_hours} hours")
                print("-"*50)

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()