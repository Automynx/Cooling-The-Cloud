"""
Utility functions and logging setup for the weather pipeline
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional
import os

import config


def setup_logging(log_level: str = None, log_file: str = None) -> logging.Logger:
    """
    Set up logging configuration

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Path to log file

    Returns:
        Configured logger instance
    """
    if log_level is None:
        log_level = config.LOG_LEVEL
    if log_file is None:
        log_file = config.LOG_FILE

    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    # Configure logging
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=config.LOG_FORMAT,
        handlers=handlers
    )

    logger = logging.getLogger()
    logger.info(f"Logging initialized at {log_level} level")

    return logger


def parse_date_range(start_str: str = None, end_str: str = None) -> Tuple[datetime, datetime]:
    """
    Parse date strings and return datetime objects

    Args:
        start_str: Start date string (YYYY-MM-DD)
        end_str: End date string (YYYY-MM-DD)

    Returns:
        Tuple of start and end datetime objects
    """
    if start_str is None:
        start_str = config.DEFAULT_START_DATE
    if end_str is None:
        end_str = config.DEFAULT_END_DATE

    try:
        # Parse dates and make them timezone-aware (UTC)
        start_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Check if end date is in the future (use UTC time)
        today = datetime.now(timezone.utc)
        if end_date > today:
            logging.warning(f"End date {end_date.date()} is in the future, adjusting to today {today.date()}")
            end_date = today

        # Ensure start date is before end date
        if start_date > end_date:
            raise ValueError(f"Start date {start_date.date()} is after end date {end_date.date()}")

        return start_date, end_date

    except ValueError as e:
        logging.error(f"Error parsing dates: {str(e)}")
        raise


def calculate_date_chunks(start_date: datetime, end_date: datetime,
                         chunk_days: int = None) -> list:
    """
    Calculate date chunks for batch processing

    Args:
        start_date: Start date
        end_date: End date
        chunk_days: Days per chunk

    Returns:
        List of (start, end) datetime tuples
    """
    if chunk_days is None:
        chunk_days = config.BATCH_SIZE_DAYS

    chunks = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def analyze_cooling_efficiency(temperature_f: float, humidity_percent: float) -> dict:
    """
    Analyze cooling system efficiency based on weather conditions

    Args:
        temperature_f: Temperature in Fahrenheit
        humidity_percent: Relative humidity percentage

    Returns:
        Dictionary with cooling recommendations
    """
    analysis = {
        'temperature_f': temperature_f,
        'humidity_percent': humidity_percent,
        'cooling_needed': False,
        'evap_cooling_effective': False,
        'recommended_system': 'none',
        'efficiency_score': 0.0,
        'notes': []
    }

    # Check if cooling is needed
    if temperature_f > config.EVAP_COOLING_TEMP_THRESHOLD:
        analysis['cooling_needed'] = True

        # Check if evaporative cooling would be effective
        if humidity_percent < config.EVAP_COOLING_HUMIDITY_THRESHOLD:
            analysis['evap_cooling_effective'] = True
            analysis['recommended_system'] = 'evaporative'

            # Calculate efficiency score (lower humidity = higher efficiency)
            efficiency = 1.0 - (humidity_percent / 100.0)
            analysis['efficiency_score'] = round(efficiency * 100, 1)

            analysis['notes'].append(
                f"Evaporative cooling highly effective (efficiency: {analysis['efficiency_score']}%)"
            )
        elif humidity_percent < 50:
            analysis['evap_cooling_effective'] = True
            analysis['recommended_system'] = 'hybrid'

            efficiency = 0.5 * (1.0 - (humidity_percent / 100.0))
            analysis['efficiency_score'] = round(efficiency * 100, 1)

            analysis['notes'].append(
                f"Evaporative cooling moderately effective (efficiency: {analysis['efficiency_score']}%)"
            )
        else:
            analysis['recommended_system'] = 'electric_chiller'
            analysis['notes'].append(
                f"High humidity ({humidity_percent}%) - use electric chillers"
            )
    else:
        analysis['notes'].append(
            f"Temperature below threshold ({temperature_f}°F < {config.EVAP_COOLING_TEMP_THRESHOLD}°F)"
        )

    return analysis


def print_summary_statistics(df) -> None:
    """
    Print summary statistics for a DataFrame of weather data

    Args:
        df: DataFrame with weather data
    """
    if df.empty:
        print("No data to summarize")
        return

    print("\n" + "="*50)
    print("WEATHER DATA SUMMARY")
    print("="*50)

    print(f"\nStation: {config.STATION_NAME} ({config.STATION_CODE})")
    print(f"Total Records: {len(df):,}")

    if 'timestamp' in df.columns:
        print(f"Date Range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    if 'temperature_f' in df.columns:
        temp_stats = df['temperature_f'].describe()
        print(f"\nTemperature (°F):")
        print(f"  Mean: {temp_stats['mean']:.1f}")
        print(f"  Min:  {temp_stats['min']:.1f}")
        print(f"  Max:  {temp_stats['max']:.1f}")
        print(f"  Std:  {temp_stats['std']:.1f}")

    if 'humidity_percent' in df.columns:
        humidity_stats = df['humidity_percent'].describe()
        print(f"\nHumidity (%):")
        print(f"  Mean: {humidity_stats['mean']:.1f}")
        print(f"  Min:  {humidity_stats['min']:.1f}")
        print(f"  Max:  {humidity_stats['max']:.1f}")
        print(f"  Std:  {humidity_stats['std']:.1f}")

    # Cooling analysis
    if 'temperature_f' in df.columns and 'humidity_percent' in df.columns:
        valid_data = df.dropna(subset=['temperature_f', 'humidity_percent'])

        cooling_needed = valid_data[valid_data['temperature_f'] > config.EVAP_COOLING_TEMP_THRESHOLD]
        evap_effective = cooling_needed[cooling_needed['humidity_percent'] < config.EVAP_COOLING_HUMIDITY_THRESHOLD]

        print(f"\nCooling Analysis:")
        print(f"  Hours needing cooling: {len(cooling_needed):,} ({100*len(cooling_needed)/len(valid_data):.1f}%)")
        print(f"  Hours suitable for evap cooling: {len(evap_effective):,} ({100*len(evap_effective)/max(len(cooling_needed), 1):.1f}%)")

    print("="*50 + "\n")


def validate_environment() -> bool:
    """
    Validate that the environment is properly configured

    Returns:
        True if environment is valid, False otherwise
    """
    issues = []

    # Check Supabase configuration
    if not config.SUPABASE_URL:
        issues.append("SUPABASE_URL is not configured")
    if not config.SUPABASE_KEY:
        issues.append("SUPABASE_KEY is not configured")

    # Check if logs directory is writable
    log_dir = os.path.dirname(config.LOG_FILE)
    if log_dir and not os.access(log_dir, os.W_OK):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except:
            issues.append(f"Cannot create/write to logs directory: {log_dir}")

    if issues:
        print("Environment validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        return False

    return True


def get_utc_now() -> datetime:
    """
    Get current UTC time as timezone-aware datetime

    Returns:
        Current UTC time with timezone info
    """
    return datetime.now(timezone.utc)


def make_timezone_aware(dt: datetime) -> datetime:
    """
    Convert naive datetime to UTC timezone-aware

    Args:
        dt: Datetime object (naive or aware)

    Returns:
        Timezone-aware datetime in UTC
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt