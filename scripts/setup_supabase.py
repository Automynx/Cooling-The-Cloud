#!/usr/bin/env python3
"""
Setup script to create Supabase tables and seed initial data
Run this to set up your Supabase database for the Cooling The Cloud project
"""

import sys
import os
from datetime import datetime, timedelta
import random

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()


def get_supabase_client():
    """Create Supabase client"""
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')

    if not url or not key:
        raise ValueError("Supabase credentials not found in .env file")

    return create_client(url, key)


def check_tables(supabase: Client):
    """Check which tables already exist"""
    print("\n" + "="*60)
    print("CHECKING EXISTING TABLES")
    print("="*60)

    tables_to_check = [
        'weather_data',
        'electricity_prices',
        'water_prices',
        'optimization_results',
        'optimization_summary'
    ]

    existing = []
    missing = []

    for table in tables_to_check:
        try:
            response = supabase.table(table).select("*").limit(1).execute()
            existing.append(table)
            print(f"✅ Table '{table}' exists")
        except:
            missing.append(table)
            print(f"❌ Table '{table}' not found")

    return existing, missing


def seed_sample_weather_data(supabase: Client):
    """Seed sample weather data if needed"""
    print("\n" + "="*60)
    print("SEEDING SAMPLE WEATHER DATA")
    print("="*60)

    try:
        # Check if weather data already exists
        response = supabase.table('weather_data').select("*").limit(1).execute()

        if response.data:
            print("ℹ️ Weather data already exists, skipping seed")
            return True

        # Generate sample data for the last 7 days
        records = []
        start_date = datetime.now() - timedelta(days=7)

        for day in range(7):
            date = start_date + timedelta(days=day)

            for hour in range(24):
                timestamp = date.replace(hour=hour, minute=0, second=0, microsecond=0)

                # Generate realistic Phoenix temperature pattern
                base = 95
                amplitude = 15
                phase = (hour - 5) * 3.14159 / 12
                temp = base + amplitude * (phase - 3.14159/2)

                # Add some random variation
                temp += random.uniform(-3, 3)
                temp = max(75, min(115, temp))

                # Humidity inversely related to temperature
                humidity = max(5, min(30, 100 - temp + random.uniform(-5, 5)))

                records.append({
                    'station': 'PHX',
                    'timestamp': timestamp.isoformat(),
                    'temperature_f': round(temp, 1),
                    'humidity_percent': round(humidity, 2)
                })

        # Insert in batches
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            supabase.table('weather_data').insert(batch).execute()

        print(f"✅ Seeded {len(records)} weather records")
        return True

    except Exception as e:
        print(f"❌ Error seeding weather data: {e}")
        return False


def seed_sample_prices(supabase: Client):
    """Seed sample electricity and water prices"""
    print("\n" + "="*60)
    print("SEEDING SAMPLE PRICE DATA")
    print("="*60)

    try:
        # Seed electricity prices for today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        elec_records = []

        for hour in range(24):
            timestamp = today + timedelta(hours=hour)

            # Determine price based on time of day
            if 15 <= hour < 20:  # Peak hours
                price = 150 + random.uniform(-10, 10)
                rate_type = 'peak'
            elif hour >= 22 or hour < 6:  # Super off-peak
                price = 25 + random.uniform(-5, 5)
                rate_type = 'super-off-peak'
            else:  # Off-peak
                price = 35 + random.uniform(-5, 5)
                rate_type = 'off-peak'

            elec_records.append({
                'timestamp': timestamp.isoformat(),
                'hour': hour,
                'price_per_mwh': round(price, 2),
                'rate_type': rate_type,
                'source': 'seed_data'
            })

        # Try to insert electricity prices
        try:
            supabase.table('electricity_prices').insert(elec_records).execute()
            print(f"✅ Seeded {len(elec_records)} electricity price records")
        except:
            print("ℹ️ Electricity prices may already exist for today")

        # Seed water prices
        water_record = {
            'date': today.date().isoformat(),
            'price_per_thousand_gallons': 3.24,
            'tier': 1,
            'source': 'Arizona Water Resources',
            'seasonal_multiplier': 1.15  # Summer rate
        }

        try:
            supabase.table('water_prices').insert(water_record).execute()
            print("✅ Seeded water price data")
        except:
            print("ℹ️ Water price may already exist for today")

        return True

    except Exception as e:
        print(f"❌ Error seeding price data: {e}")
        return False


def main():
    """Main setup function"""
    print("\n" + "#"*60)
    print("# COOLING THE CLOUD - SUPABASE SETUP SCRIPT")
    print("#"*60)

    print("\nThis script will:")
    print("1. Check for existing tables")
    print("2. Seed sample weather data if needed")
    print("3. Seed sample price data if needed")

    print("\n⚠️ IMPORTANT: Make sure you have run the SQL script")
    print("   (scripts/create_tables.sql) in Supabase first!")

    input("\nPress Enter to continue...")

    try:
        # Get Supabase client
        supabase = get_supabase_client()
        print("✅ Connected to Supabase")

        # Check tables
        existing, missing = check_tables(supabase)

        if missing:
            print("\n⚠️ Missing tables detected!")
            print("Please run the following SQL in your Supabase SQL Editor:")
            print(f"   File: scripts/create_tables.sql")
            print(f"   Missing tables: {', '.join(missing)}")
            return False

        # Seed data
        if 'weather_data' in existing:
            seed_sample_weather_data(supabase)

        if 'electricity_prices' in existing and 'water_prices' in existing:
            seed_sample_prices(supabase)

        print("\n" + "="*60)
        print("✅ SETUP COMPLETE!")
        print("="*60)
        print("\nYour Supabase database is ready to use.")
        print("You can now:")
        print("1. Run the test script: python test_supabase_integration.py")
        print("2. Start the Streamlit app: streamlit run streamlit_app.py")

        return True

    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)