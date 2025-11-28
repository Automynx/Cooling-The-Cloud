#!/usr/bin/env python3
"""
Explore Supabase Database - See what tables and data we have

Dev utility script to explore database tables, structure, and sample data.
Useful for new developers to understand the data schema.
Run from repo root: python scripts/dev/explore_supabase_data.py
"""

import os
import sys
import psycopg2
from psycopg2 import sql
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import json

# Add repo root to path (go up 2 levels from scripts/dev/)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.api.store_to_postgres import connect_db

load_dotenv()

def get_all_tables(conn):
    """Get list of all tables in the database."""
    query = """
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename;
    """
    cur = conn.cursor()
    cur.execute(query)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables

def get_table_structure(conn, table_name):
    """Get the structure of a table."""
    query = """
    SELECT
        column_name,
        data_type,
        character_maximum_length,
        is_nullable
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position;
    """
    cur = conn.cursor()
    cur.execute(query, (table_name,))
    columns = cur.fetchall()
    cur.close()
    return columns

def get_table_sample(conn, table_name, limit=10):
    """Get sample data from a table."""
    try:
        # Get count
        count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
        cur = conn.cursor()
        cur.execute(count_query)
        count = cur.fetchone()[0]

        # Get sample
        sample_query = sql.SQL("SELECT * FROM {} LIMIT %s").format(sql.Identifier(table_name))
        df = pd.read_sql_query(sample_query, conn, params=(limit,))

        cur.close()
        return count, df
    except Exception as e:
        print(f"Error reading {table_name}: {e}")
        return 0, pd.DataFrame()

def get_date_range(conn, table_name, date_column):
    """Get the date range of data in a table."""
    try:
        query = sql.SQL("""
            SELECT
                MIN({date_col}) as min_date,
                MAX({date_col}) as max_date,
                COUNT(DISTINCT DATE({date_col})) as unique_days
            FROM {table}
        """).format(
            date_col=sql.Identifier(date_column),
            table=sql.Identifier(table_name)
        )
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        return result
    except Exception as e:
        return None, None, 0

def main():
    print("=" * 80)
    print("SUPABASE DATABASE EXPLORER")
    print("=" * 80)

    # Connect to database
    print("\n🔌 Connecting to Supabase...")
    try:
        conn = connect_db()
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    # Get all tables
    print("\n📊 Finding all tables...")
    tables = get_all_tables(conn)
    print(f"Found {len(tables)} tables:")
    for i, table in enumerate(tables, 1):
        print(f"  {i}. {table}")

    print("\n" + "=" * 80)
    print("DETAILED TABLE ANALYSIS")
    print("=" * 80)

    # Analyze each table
    all_data_info = {}
    for table in tables:
        print(f"\n📋 TABLE: {table}")
        print("-" * 40)

        # Get structure
        columns = get_table_structure(conn, table)
        print("Columns:")
        for col_name, data_type, max_len, nullable in columns:
            nullable_str = "" if nullable == 'YES' else " NOT NULL"
            print(f"  - {col_name}: {data_type}{nullable_str}")

        # Get sample data
        count, sample_df = get_table_sample(conn, table, 5)
        print(f"\nRow count: {count:,}")

        if count > 0:
            print("\nSample data (first 5 rows):")
            print(sample_df.to_string())

            # Check for date columns and get date range
            date_columns = ['period', 'period_date', 'period_month', 'timestamp', 'created_at']
            for date_col in date_columns:
                if date_col in sample_df.columns:
                    min_date, max_date, unique_days = get_date_range(conn, table, date_col)
                    if min_date:
                        print(f"\nDate range ({date_col}):")
                        print(f"  From: {min_date}")
                        print(f"  To: {max_date}")
                        print(f"  Unique days: {unique_days}")
                    break

        # Store info for summary
        all_data_info[table] = {
            'count': count,
            'columns': [col[0] for col in columns],
            'sample': sample_df.head(3).to_dict('records') if count > 0 else []
        }

    print("\n" + "=" * 80)
    print("KEY DATA TABLES FOR OPTIMIZATION")
    print("=" * 80)

    # Identify key tables for optimization
    key_tables = {
        'eia_interchange': 'Electricity interchange data between balancing authorities',
        'eia_az_price': 'Arizona electricity prices',
        'water_price_index': 'Water price index data',
        'weather_data': 'Temperature and weather conditions',
        'optimization_results': 'Previous optimization results'
    }

    for table_name, description in key_tables.items():
        if table_name in tables:
            count = all_data_info[table_name]['count']
            print(f"\n✅ {table_name}: {description}")
            print(f"   Records: {count:,}")
            if count > 0:
                print(f"   Columns: {', '.join(all_data_info[table_name]['columns'][:5])}")
        else:
            print(f"\n❌ {table_name}: Not found (would contain: {description})")

    # Check specifically for Arizona data
    print("\n" + "=" * 80)
    print("ARIZONA-SPECIFIC DATA CHECK")
    print("=" * 80)

    if 'eia_interchange' in tables:
        query = """
        SELECT
            fromba,
            toba,
            COUNT(*) as record_count,
            MIN(period) as earliest,
            MAX(period) as latest
        FROM eia_interchange
        WHERE fromba IN ('AZPS', 'SRP', 'TEPC')
           OR toba IN ('AZPS', 'SRP', 'TEPC')
        GROUP BY fromba, toba
        ORDER BY record_count DESC
        LIMIT 10;
        """
        print("\nTop Arizona interchange connections:")
        az_df = pd.read_sql_query(query, conn)
        print(az_df.to_string())

    if 'eia_az_price' in tables:
        query = """
        SELECT
            sectorid,
            COUNT(*) as months,
            AVG(price_per_mwh) as avg_price,
            MIN(period_month) as earliest,
            MAX(period_month) as latest
        FROM eia_az_price
        GROUP BY sectorid
        ORDER BY sectorid;
        """
        print("\nArizona electricity prices by sector:")
        price_df = pd.read_sql_query(query, conn)
        print(price_df.to_string())

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total_records = sum(info['count'] for info in all_data_info.values())
    print(f"\n📊 Total records across all tables: {total_records:,}")
    print(f"📋 Total tables: {len(tables)}")

    # Identify what data is available for optimization
    print("\n🎯 Available for optimization:")
    if 'eia_interchange' in tables and all_data_info['eia_interchange']['count'] > 0:
        print("  ✅ Electricity interchange data")
    if 'eia_az_price' in tables and all_data_info['eia_az_price']['count'] > 0:
        print("  ✅ Electricity price data")
    if 'water_price_index' in tables and all_data_info['water_price_index']['count'] > 0:
        print("  ✅ Water price data")

    # Close connection
    conn.close()

    print("\n✅ Analysis complete!")
    print("\nNext steps:")
    print("1. Use this real data in the optimizer")
    print("2. Run optimization with actual prices and interchange data")
    print("3. Compare results with baseline scenarios")

if __name__ == "__main__":
    main()