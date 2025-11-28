#!/usr/bin/env python3
"""
Production System Test - Verify all Supabase connections and real data
Run this to ensure your production system is fully operational
"""

import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add repo root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.supabase_interface import SupabaseInterface
from model.data_interface import DataInterface
from model.optimizer_linear import LinearDataCenterOptimizer

load_dotenv()


def test_supabase_connection():
    """Test basic Supabase connection."""
    print("\n" + "="*60)
    print("1. TESTING SUPABASE CONNECTION")
    print("="*60)

    try:
        interface = SupabaseInterface()
        if interface.test_connection():
            print("✅ Successfully connected to Supabase")
            return interface
        else:
            print("❌ Failed to connect to Supabase")
            return None
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None


def test_data_fetching(interface):
    """Test fetching real data from Supabase."""
    print("\n" + "="*60)
    print("2. TESTING DATA FETCHING")
    print("="*60)

    test_date = datetime(2024, 8, 1)  # August 1, 2024

    # Test weather data
    print("\n📊 Fetching weather data...")
    try:
        temperatures = interface.fetch_weather_data(test_date, hours=24)
        print(f"✅ Got {len(temperatures)} hours of temperature data")
        print(f"   Range: {min(temperatures):.1f}°F - {max(temperatures):.1f}°F")
    except Exception as e:
        print(f"❌ Failed to fetch weather data: {e}")

    # Test electricity prices
    print("\n💡 Fetching electricity prices...")
    try:
        prices = interface.get_electricity_prices(test_date, hours=24)
        print(f"✅ Got {len(prices)} hours of price data")
        print(f"   Range: ${min(prices):.2f} - ${max(prices):.2f}/MWh")
        print(f"   Average: ${sum(prices)/len(prices):.2f}/MWh")
    except Exception as e:
        print(f"❌ Failed to fetch electricity prices: {e}")

    # Test water prices
    print("\n💧 Fetching water prices...")
    try:
        water_prices = interface.get_water_prices(test_date)
        print(f"✅ Got water price: ${water_prices[0]:.2f}/1000 gallons")
    except Exception as e:
        print(f"❌ Failed to fetch water prices: {e}")

    return temperatures, prices


def test_optimization(temperatures, prices):
    """Test running optimization with real data."""
    print("\n" + "="*60)
    print("3. TESTING OPTIMIZATION ENGINE")
    print("="*60)

    try:
        # Initialize optimizer with Supabase
        optimizer = LinearDataCenterOptimizer(use_supabase=True)
        print("✅ Optimizer initialized with Supabase support")

        # Build model
        print("\n🏗️ Building optimization model...")
        model = optimizer.build_model(temperatures, prices)
        print("✅ Model built successfully")

        # Solve
        print("\n🔍 Solving with HiGHS solver...")
        results = optimizer.solve(solver_name='highs')

        if results:
            print("✅ Optimization completed successfully!")

            print("\n📊 OPTIMIZATION RESULTS:")
            print(f"   Total Cost: ${results['summary']['total_cost']:,.2f}")
            print(f"   Daily Savings: ${results['savings']['daily_savings']:,.2f}")
            print(f"   Savings Percent: {results['savings']['percentage_saved']:.1f}%")
            print(f"   Water Used: {results['environmental']['water_used_gallons']:,.0f} gallons")
            print(f"   Peak Demand: {results['summary']['peak_demand_mw']:.1f} MW")

            return results
        else:
            print("❌ Optimization failed")
            return None

    except Exception as e:
        print(f"❌ Optimization error: {e}")
        return None


def test_result_saving(interface, results):
    """Test saving results to Supabase."""
    print("\n" + "="*60)
    print("4. TESTING RESULT STORAGE")
    print("="*60)

    try:
        run_id = interface.save_optimization_results(results)
        if run_id:
            print(f"✅ Results saved to Supabase with run_id: {run_id}")
            return run_id
        else:
            print("❌ Failed to save results")
            return None
    except Exception as e:
        print(f"❌ Error saving results: {e}")
        return None


def test_history_retrieval(interface):
    """Test retrieving optimization history."""
    print("\n" + "="*60)
    print("5. TESTING HISTORY RETRIEVAL")
    print("="*60)

    try:
        # Get recent runs
        history_df = interface.get_optimization_history(limit=5)
        if not history_df.empty:
            print(f"✅ Retrieved {len(history_df)} recent optimization runs")
            print("\nRecent runs:")
            for _, row in history_df.iterrows():
                print(f"   {row['run_timestamp']}: ${row['cost_savings']:.2f} saved ({row['cost_savings_percent']:.1f}%)")
        else:
            print("ℹ️ No optimization history found (this is normal for first run)")

        # Get period summary
        period_summary = interface.get_period_summary(30)
        if period_summary.get('total_savings', 0) > 0:
            print(f"\n✅ Last 30 days summary:")
            print(f"   Total Savings: ${period_summary['total_savings']:,.2f}")
            print(f"   Average Daily: ${period_summary['avg_daily_savings']:,.2f}")
        else:
            print("\nℹ️ No period data available yet")

    except Exception as e:
        print(f"❌ Error retrieving history: {e}")


def test_integrated_system():
    """Test the complete integrated system."""
    print("\n" + "="*60)
    print("6. TESTING INTEGRATED SYSTEM")
    print("="*60)

    try:
        # Use DataInterface for integrated test
        data_interface = DataInterface(use_supabase=True)
        print("✅ DataInterface initialized")

        # Prepare optimization data
        opt_data = data_interface.prepare_optimization_data(
            date=datetime(2024, 8, 1),
            use_supabase=True
        )
        print(f"✅ Prepared optimization data from {opt_data['source']}")
        print(f"   Date: {opt_data['date']}")
        print(f"   Max Temp: {opt_data['metadata']['max_temp']:.1f}°F")
        print(f"   Avg Price: ${opt_data['metadata']['avg_price']:.2f}/MWh")

        # Run optimization with Supabase
        optimizer = LinearDataCenterOptimizer(use_supabase=True)
        results = optimizer.optimize_with_supabase(
            date=datetime(2024, 8, 1),
            solver_name='highs'
        )

        if results:
            print("✅ Full integrated optimization successful!")
            print(f"   Saved as run_id: {results.get('run_id', 'Not saved')}")
            return True
        else:
            print("❌ Integrated optimization failed")
            return False

    except Exception as e:
        print(f"❌ Integration error: {e}")
        return False


def main():
    """Main test sequence."""
    print("\n" + "#"*60)
    print("# COOLING THE CLOUD - PRODUCTION SYSTEM TEST")
    print("#"*60)

    print("\nThis test will verify:")
    print("1. Supabase database connection")
    print("2. Real data fetching")
    print("3. Optimization engine")
    print("4. Result storage")
    print("5. History retrieval")
    print("6. Full system integration")

    # Test 1: Connection
    interface = test_supabase_connection()
    if not interface:
        print("\n❌ Cannot proceed without database connection")
        print("Please check your .env file has correct Supabase credentials")
        return False

    # Test 2: Data fetching
    temperatures, prices = test_data_fetching(interface)

    # Test 3: Optimization
    if temperatures and prices:
        results = test_optimization(temperatures, prices)
    else:
        print("\n⚠️ Using fallback data for optimization test")
        temperatures = [95 + i for i in range(24)]
        prices = [100 + i*5 for i in range(24)]
        results = test_optimization(temperatures, prices)

    # Test 4: Save results
    if results:
        run_id = test_result_saving(interface, results)

    # Test 5: History
    test_history_retrieval(interface)

    # Test 6: Full integration
    success = test_integrated_system()

    # Summary
    print("\n" + "#"*60)
    print("# TEST SUMMARY")
    print("#"*60)

    if success:
        print("\n🎉 ALL SYSTEMS OPERATIONAL!")
        print("Your production system is ready for use:")
        print("\n1. Run optimization script:")
        print("   python optimize_with_real_data.py")
        print("\n2. Launch Streamlit dashboard:")
        print("   streamlit run streamlit_app_advanced.py")
        print("\n3. All features enabled:")
        print("   ✅ Real-time data from Supabase")
        print("   ✅ Optimization with HiGHS solver")
        print("   ✅ Result storage and history")
        print("   ✅ Historical analysis and trends")
    else:
        print("\n⚠️ Some tests failed. Please check:")
        print("1. Database tables exist (run scripts/create_tables.sql)")
        print("2. .env file has correct credentials")
        print("3. HiGHS solver is installed (pip install highspy)")

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)