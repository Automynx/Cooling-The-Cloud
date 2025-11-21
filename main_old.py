#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main entry point for Arizona Data Center Optimization
IISE Hackathon - Cooling the Cloud

Usage:
    python main.py --electricity-data <path> --weather-data <path>
    python main.py --demo  # Run with sample data

Team: Cooling the Cloud
Theme: Electricity in Arizona
"""

import os
import sys
import argparse
import json
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model.optimizer import ArizonaDataCenterOptimizer
from model.data_interface import DataInterface
import pandas as pd
import numpy as np


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Arizona Data Center Optimization - Cooling the Cloud"
    )
    parser.add_argument(
        "--electricity-data",
        type=str,
        help="Path to electricity data file (CSV/JSON from EIA)"
    )
    parser.add_argument(
        "--weather-data",
        type=str,
        help="Path to weather data file (CSV/JSON from NOAA)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date for optimization (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run with demonstration data"
    )
    parser.add_argument(
        "--solver",
        type=str,
        default="glpk",
        choices=["glpk", "cbc", "gurobi", "cplex"],
        help="Optimization solver to use"
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export results to CSV"
    )

    args = parser.parse_args()

    print("""
    TPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPW
    Q           COOLING THE CLOUD - OPTIMIZATION TOOL          Q
    Q                                                          Q
    Q  Arizona Data Center Electricity & Cooling Optimization  Q
    Q              IISE Hackathon Submission                   Q
    ZPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP]
    """)

    # Initialize components
    data_interface = DataInterface()
    optimizer = ArizonaDataCenterOptimizer()

    # Load data based on arguments
    if args.demo:
        print("= Running in DEMO mode with sample data...")
        electricity_data, weather_data = create_demo_data()
    else:
        if not args.electricity_data or not args.weather_data:
            print("ERROR: Please provide both --electricity-data and --weather-data")
            print("   Or use --demo for demonstration mode")
            sys.exit(1)

        print(f"=� Loading electricity data from: {args.electricity_data}")
        electricity_data = args.electricity_data

        print(f"<!  Loading weather data from: {args.weather_data}")
        weather_data = args.weather_data

    # Prepare data for optimization
    print("=' Preparing data for optimization...")
    try:
        optimization_data = data_interface.prepare_optimization_data(
            electricity_source=electricity_data,
            weather_source=weather_data,
            date=args.date
        )

        print(f" Data loaded successfully!")
        print(f"   - Date: {optimization_data['date']}")
        print(f"   - Temperature range: {optimization_data['metadata']['min_temp']:.1f}�F - "
              f"{optimization_data['metadata']['max_temp']:.1f}�F")
        print(f"   - Electricity price range: ${optimization_data['metadata']['avg_price']:.2f}/MWh")

    except Exception as e:
        print(f"L Error loading data: {e}")
        sys.exit(1)

    # Extract data for model
    temperatures, prices, demand = data_interface.export_to_model_format(optimization_data)

    # Build optimization model
    print("\n<�  Building optimization model...")
    print(f"   - Decision variables: 10+")
    print(f"   - Constraints: 10+")
    print(f"   - Multi-objective optimization")

    try:
        model = optimizer.build_model(
            temperatures=temperatures,
            electricity_prices=prices,
            grid_demand=demand
        )
        print(" Model built successfully!")

    except Exception as e:
        print(f"L Error building model: {e}")
        sys.exit(1)

    # Solve optimization
    print(f"\n= Solving optimization with {args.solver.upper()} solver...")
    print("   This may take a few moments...")

    try:
        results = optimizer.solve(solver_name=args.solver, time_limit=300)
        print(" Optimization completed successfully!")

    except Exception as e:
        print(f"L Error solving optimization: {e}")
        print("   Trying fallback solver...")
        try:
            results = optimizer.solve(solver_name='glpk', time_limit=300)
            print(" Optimization completed with GLPK solver!")
        except:
            print("L Could not solve optimization. Please check solver installation.")
            sys.exit(1)

    # Display results
    print("\n" + "="*60)
    print("                    OPTIMIZATION RESULTS")
    print("="*60)

    print("\n=� COST SAVINGS:")
    print(f"   Daily Savings: ${results['savings']['daily_savings']:,.2f}")
    print(f"   Annual Savings: ${results['savings']['annual_savings']:,.2f}")
    print(f"   Savings Rate: {results['savings']['percentage_saved']:.1f}%")

    print("\n=� WATER CONSERVATION:")
    print(f"   Water Used: {results['environmental']['water_used_gallons']:,.0f} gallons/day")
    print(f"   Water Saved: {results['environmental']['water_saved_gallons']:,.0f} gallons/day")
    print(f"   Equivalent to: {results['environmental']['water_saved_gallons']/325851:.1f} Olympic pools/year")

    print("\n� GRID IMPACT:")
    print(f"   Peak Demand: {results['summary']['peak_demand_mw']:.1f} MW")
    print(f"   Peak Reduction: {results['environmental']['peak_reduction_mw']:.1f} MW")
    print(f"   Equivalent to: Powering {results['environmental']['peak_reduction_mw']*1000:.0f} homes")

    print("\n<1 ENVIRONMENTAL BENEFIT:")
    print(f"   Carbon Avoided: {results['environmental']['carbon_avoided_tons']:.2f} tons CO2/day")
    print(f"   Annual Impact: {results['environmental']['carbon_avoided_tons']*365:.1f} tons CO2/year")
    print(f"   Equivalent to: {results['environmental']['carbon_avoided_tons']*365/4.6:.0f} cars off the road")

    # Save results to file
    results_file = f"results_{args.date.replace('-', '')}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n=� Results saved to: {results_file}")

    # Export to CSV if requested
    if args.export:
        export_file = f"optimization_results_{args.date.replace('-', '')}.csv"
        df = pd.DataFrame(results['hourly_data'])
        df.to_csv(export_file, index=False)
        print(f"=� Hourly data exported to: {export_file}")

    # Print detailed report
    print("\n" + optimizer.generate_report())

    print("\n( Optimization complete! Thank you for using Cooling the Cloud.")
    print("   For Arizona's sustainable data center future! <5")


def create_demo_data():
    """Create demonstration data for testing."""
    # Create realistic Phoenix summer day temperatures
    hours = list(range(24))
    temperatures = []
    for h in hours:
        base = 95
        amplitude = 15
        phase = (h - 5) * np.pi / 12
        temp = base + amplitude * np.sin(phase - np.pi/2)
        temp += np.random.uniform(-2, 2)
        temperatures.append(max(75, min(118, temp)))

    # Create time-of-use electricity prices (APS schedule)
    prices = []
    for h in hours:
        if 15 <= h < 20:  # Peak hours 3-8 PM
            price = np.random.uniform(140, 160)  # $/MWh
        elif 22 <= h or h < 6:  # Super off-peak
            price = np.random.uniform(30, 40)
        else:  # Off-peak
            price = np.random.uniform(50, 70)
        prices.append(price)

    return prices, temperatures


if __name__ == "__main__":
    main()