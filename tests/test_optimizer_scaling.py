#!/usr/bin/env python3
"""Test the optimizer with 2000MW scaling."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.optimizer_linear import LinearDataCenterOptimizer
from datetime import datetime
import numpy as np

def test_2000mw_optimization():
    """Test that the optimizer works with 2000MW capacity."""
    print("\n" + "="*60)
    print("Testing 2000MW Optimization with Scaling Fix")
    print("="*60)

    # Initialize with 2000MW capacity
    optimizer = LinearDataCenterOptimizer(use_supabase=False, capacity_mw=2000.0)

    print(f"✓ Optimizer initialized with {optimizer.requested_capacity_mw}MW capacity")
    print(f"  Internal model scale: {optimizer.total_capacity_mw}MW")
    print(f"  Scale factor: {optimizer.scale_factor}x")

    # Create test data for Arizona summer
    hours = 24
    temperatures = [
        85, 82, 80, 78, 76, 78, 82, 87,  # Morning
        92, 96, 99, 102, 104, 106, 107, 108,  # Midday/Afternoon
        107, 105, 102, 98, 94, 90, 87, 85  # Evening/Night
    ]

    # Realistic Arizona electricity prices ($/MWh)
    electricity_prices = [
        45, 42, 40, 38, 40, 45, 55, 65,  # Morning ramp
        70, 75, 80, 85, 90, 95, 100, 110,  # Peak hours
        105, 95, 85, 75, 65, 55, 50, 45  # Evening decline
    ]

    print("\nTest conditions:")
    print(f"  Max temperature: {max(temperatures)}°F")
    print(f"  Min temperature: {min(temperatures)}°F")
    print(f"  Peak price: ${max(electricity_prices)}/MWh")
    print(f"  Off-peak price: ${min(electricity_prices)}/MWh")

    # Build model
    print("\nBuilding optimization model...")
    model = optimizer.build_model(temperatures, electricity_prices)
    print("✓ Model built successfully")

    # Solve
    print("\nSolving optimization...")
    try:
        results = optimizer.solve(solver_name='highs')

        if results:
            print("\n" + "="*60)
            print("OPTIMIZATION RESULTS (2000MW Scale)")
            print("="*60)

            print("\nCapacity Information:")
            print(f"  Total capacity: {results['summary'].get('capacity_mw', 2000)}MW")
            print(f"  Peak demand: {results['summary']['peak_demand_mw']:.1f}MW")
            print(f"  Utilization: {results['summary']['peak_demand_mw']/2000*100:.1f}%")

            print("\nCost Summary:")
            print(f"  Total cost: ${results['summary']['total_cost']:,.2f}")
            print(f"  Baseline cost: ${results['summary']['baseline_cost']:,.2f}")
            print(f"  Daily savings: ${results['savings']['daily_savings']:,.2f}")
            print(f"  Savings percentage: {results['savings']['percentage_saved']:.1f}%")
            print(f"  Annual savings: ${results['savings']['annual_savings']:,.0f}")

            print("\nEnvironmental Impact:")
            print(f"  Water used: {results['environmental']['water_used_gallons']:,.0f} gallons")
            print(f"  Water saved: {results['environmental']['water_saved_gallons']:,.0f} gallons")
            print(f"  Carbon avoided: {results['environmental']['carbon_avoided_tons']:.2f} tons")

            print("\nLoad Distribution (Sample Hours):")
            for h in [6, 12, 15, 20]:  # Morning, noon, peak, evening
                data = results['hourly_data'][h]
                print(f"  Hour {h:02d}: {data['total_load_mw']:.1f}MW " +
                      f"(Batch: {data['batch_load_mw']:.1f}MW, " +
                      f"Cooling: {'Water' if data['water_cooling'] else 'Electric'}, " +
                      f"Temp: {data['temperature']:.0f}°F)")

            print("\n✅ Optimization completed successfully at 2000MW scale!")
            return True

        else:
            print("❌ No results returned from optimization")
            return False

    except Exception as e:
        print(f"\n❌ Optimization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_2000mw_optimization()

    if success:
        print("\n" + "="*60)
        print("✅ SCALING TEST PASSED - Ready for production!")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("❌ SCALING TEST FAILED - Check errors above")
        print("="*60)