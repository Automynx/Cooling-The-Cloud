#!/usr/bin/env python3
"""
Check actual database schema in Supabase
"""

import sys
import os
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data.api.store_to_postgres import connect_db

load_dotenv()

def check_schema():
    conn = connect_db()
    cur = conn.cursor()

    # Check optimization_results columns
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'optimization_results'
    ORDER BY ordinal_position;
    """

    cur.execute(query)
    columns = cur.fetchall()

    print("optimization_results table columns:")
    print("-" * 50)
    for col_name, data_type, nullable in columns:
        print(f"{col_name:30} {data_type:20} {nullable}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    check_schema()