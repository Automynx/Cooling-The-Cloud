"""
Configuration module for CoolTheCloud weather data pipeline
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://kupkxnvmqwzatlbxbphv.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt1cGt4bnZtcXd6YXRsYnhicGh2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjM2NzkyNDgsImV4cCI6MjA3OTI1NTI0OH0.bPIB6BG73XTzEyZp1O66qAWRdmBJKzI3DLauTVSNIIQ")

# Weather Station Configuration
STATION_CODE = "PHX"  # Phoenix Sky Harbor Airport
STATION_NAME = "Phoenix Sky Harbor"

# Database Configuration
TABLE_NAME = "weather_data"

# Iowa Environmental Mesonet API Configuration
IEM_BASE_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
IEM_NETWORK = "AZ_ASOS"

# Data Collection Settings
DEFAULT_START_DATE = "2024-08-01"  # Historical data start
DEFAULT_END_DATE = "2025-08-01"    # Historical data end
BATCH_SIZE_DAYS = 30  # Process data in monthly chunks
API_TIMEOUT = 30  # Seconds
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "logs/weather_pipeline.log"

# Data Fields
REQUIRED_FIELDS = ["tmpf", "relh"]  # Temperature (F) and Relative Humidity (%)
FIELD_DESCRIPTIONS = {
    "tmpf": "Air Temperature in Fahrenheit",
    "relh": "Relative Humidity Percentage"
}

# Cooling System Thresholds (for reference)
EVAP_COOLING_TEMP_THRESHOLD = 85  # Temperature above which cooling is needed
EVAP_COOLING_HUMIDITY_THRESHOLD = 30  # Humidity below which evaporative cooling is effective