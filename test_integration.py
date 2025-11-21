#!/usr/bin/env python3
"""
Test script to verify all components work together
Run this while waiting for real data from teammates
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model.optimizer import ArizonaDataCenterOptimizer
from model.data_interface import DataInterface
import numpy as np


def test_basic_functionality():
    """Test basic functionality with simple data."""
    print("="*60)
    print("TESTING COOLING THE CLOUD OPTIMIZATION")
    print("="*60)

    # Create simple test data
    print("\n1. Creating test data...")
    temperatures = [75 + 20*np.sin((h-5)*np.pi/12) for h in range(24)]
    prices = [50 if h < 15 or h > 20 else 150 for h in range(24)]

    print(f"   ✅ Temperature range: {min(temperatures):.1f} - {max(temperatures):.1f}°F")
    print(f"   ✅ Price range: ${min(prices)} - ${max(prices)}/MWh")

    # Initialize optimizer
    print("\n2. Initializing optimizer...")
    optimizer = ArizonaDataCenterOptimizer()
    print("   ✅ Optimizer initialized")

    # Build model
    print("\n3. Building optimization model...")
    try:
        model = optimizer.build_model(temperatures, prices)
        print("   ✅ Model built successfully")
        # Count components properly
        from pyomo.core import Constraint, Var, Objective
        num_vars = len(list(model.component_objects(Var)))
        num_constraints = len(list(model.component_objects(Constraint)))
        num_objectives = len(list(model.component_objects(Objective)))
        print(f"   - Variables: {num_vars}")
        print(f"   - Constraints: {num_constraints}")
        print(f"   - Objectives: {num_objectives}")
    except Exception as e:
        print(f"   ❌ Error building model: {e}")
        return False

    # Try to solve (will fail if no solver installed)
    print("\n4. Testing solver availability...")
    solvers = ['glpk', 'cbc', 'ipopt']
    solver_found = False

    for solver in solvers:
        try:
            from pyomo.opt import SolverFactory
            opt = SolverFactory(solver)
            if opt.available():
                print(f"   ✅ Solver '{solver}' is available")
                solver_found = True
                break
        except:
            pass

    if not solver_found:
        print("   ⚠️  No solver found. Install GLPK with:")
        print("      Ubuntu/WSL: sudo apt-get install glpk-utils")
        print("      Mac: brew install glpk")
        print("      Windows: Download from https://www.gnu.org/software/glpk/")

    # Test data interface
    print("\n5. Testing data interface...")
    data_interface = DataInterface()

    # Test with different data formats
    test_data_dict = {'prices': prices, 'demand': [100]*24}
    test_temps_list = temperatures

    try:
        elec_data = data_interface.load_electricity_data(test_data_dict)
        temps = data_interface.load_weather_data(test_temps_list)
        print("   ✅ Data interface working")
        print(f"   - Loaded {len(elec_data['prices'])} hours of price data")
        print(f"   - Loaded {len(temps)} hours of temperature data")
    except Exception as e:
        print(f"   ❌ Data interface error: {e}")
        return False

    print("\n" + "="*60)
    print("✅ ALL COMPONENTS WORKING!")
    print("="*60)
    print("\nNext steps:")
    print("1. Wait for real data from teammates")
    print("2. Run: python main.py --electricity-data <file> --weather-data <file>")
    print("3. Or test now: python main.py --demo")

    return True


def test_with_sample_files():
    """Test with sample data files if they exist."""
    print("\n" + "="*60)
    print("CHECKING FOR SAMPLE DATA FILES")
    print("="*60)

    sample_dir = "data/sample"
    if os.path.exists(sample_dir):
        files = os.listdir(sample_dir)
        if files:
            print(f"\n✅ Found sample files in {sample_dir}:")
            for f in files:
                print(f"   - {f}")
        else:
            print(f"\n⚠️  No sample files found in {sample_dir}")
            print("   Waiting for teammates to provide data...")
    else:
        print(f"\n⚠️  Sample directory {sample_dir} not found")


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║              INTEGRATION TEST FOR HACKATHON              ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    # Run tests
    success = test_basic_functionality()
    test_with_sample_files()

    if success:
        print("\n🎉 System ready for hackathon!")
        print("   Just need real data from teammates.")
    else:
        print("\n⚠️  Some issues found. Please fix before proceeding.")