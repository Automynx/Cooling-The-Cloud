#!/usr/bin/env python3
"""Test linear optimization model with GLPK"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model.optimizer_linear import LinearDataCenterOptimizer
import numpy as np


def test_linear_optimization():
    """Test the linear model with GLPK."""
    print("="*60)
    print("TESTING LINEAR OPTIMIZATION MODEL WITH GLPK")
    print("="*60)

    # Create test data
    hours = list(range(24))
    temperatures = []
    prices = []

    for h in hours:
        # Temperature pattern (Phoenix summer)
        base = 95
        amplitude = 15
        phase = (h - 5) * np.pi / 12
        temp = base + amplitude * np.sin(phase - np.pi/2)
        temp += np.random.uniform(-2, 2)
        temperatures.append(max(75, min(118, temp)))

        # Electricity prices (time-of-use)
        if 15 <= h < 20:  # Peak hours 3-8 PM
            price = np.random.uniform(140, 160)
        elif 22 <= h or h < 6:  # Off-peak
            price = np.random.uniform(30, 40)
        else:
            price = np.random.uniform(50, 70)
        prices.append(price)

    print(f"\n📊 Data Summary:")
    print(f"   Temperature range: {min(temperatures):.1f}°F - {max(temperatures):.1f}°F")
    print(f"   Price range: ${min(prices):.2f} - ${max(prices):.2f}/MWh")

    # Initialize and build model
    optimizer = LinearDataCenterOptimizer()
    print("\n🔧 Building linear optimization model...")
    model = optimizer.build_model(temperatures, prices)
    print("✅ Model built successfully!")

    # Solve
    print("\n🔍 Solving with GLPK...")
    try:
        results = optimizer.solve(solver_name='glpk')

        # Display results
        print("\n" + "="*60)
        print("                 OPTIMIZATION RESULTS")
        print("="*60)

        print(f"\n💰 COST SUMMARY:")
        print(f"   Total Cost: ${results['summary']['total_cost']:,.2f}")
        print(f"   Daily Savings: ${results['savings']['daily_savings']:,.2f}")
        print(f"   Annual Savings: ${results['savings']['annual_savings']:,.2f}")
        print(f"   Savings Rate: {results['savings']['percentage_saved']:.1f}%")

        print(f"\n💧 WATER USAGE:")
        print(f"   Water Used: {results['environmental']['water_used_gallons']:,.0f} gallons")
        print(f"   Water Saved: {results['environmental']['water_saved_gallons']:,.0f} gallons")

        print(f"\n⚡ ELECTRICITY:")
        print(f"   Peak Demand: {results['summary']['peak_demand_mw']:.1f} MW")
        print(f"   Peak Reduction: {results['environmental']['peak_reduction_mw']:.1f} MW")

        # Show hourly schedule (first 6 hours)
        print(f"\n📅 HOURLY SCHEDULE (first 6 hours):")
        print("Hour | Batch Load | Cooling Mode | Temperature | Price")
        print("-----|------------|--------------|-------------|-------")
        for h in range(6):
            data = results['hourly_data'][h]
            cooling = "Water" if data['water_cooling'] else "Chiller"
            print(f" {h:2d}  |  {data['batch_load_mw']:5.1f} MW  |   {cooling:7s}   |   {data['temperature']:5.1f}°F  | ${data['electricity_price']:.0f}")

        # Show peak hours
        print(f"\n🔥 PEAK HOURS (3-8 PM):")
        print("Hour | Batch Load | Cooling Mode | Temperature | Price")
        print("-----|------------|--------------|-------------|-------")
        for h in range(15, 20):
            data = results['hourly_data'][h]
            cooling = "Water" if data['water_cooling'] else "Chiller"
            print(f" {h:2d}  |  {data['batch_load_mw']:5.1f} MW  |   {cooling:7s}   |   {data['temperature']:5.1f}°F  | ${data['electricity_price']:.0f}")

        print("\n✅ SUCCESS! Optimization completed with GLPK!")
        return True

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = test_linear_optimization()

    if success:
        print("\n🎉 Linear model works with GLPK!")
        print("Ready for the hackathon!")
    else:
        print("\n⚠️ Check solver installation")