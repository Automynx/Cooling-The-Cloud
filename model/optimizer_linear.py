"""
Linearized Optimization Model for Data Center (GLPK Compatible)
Simplified version that works with linear solvers
"""

import pyomo.environ as pyo
from pyomo.opt import SolverFactory
import numpy as np
from typing import Dict, List, Optional
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import data interface for Supabase integration
try:
    from model.data_interface import DataInterface
    DATA_INTERFACE_AVAILABLE = True
except ImportError:
    DATA_INTERFACE_AVAILABLE = False


class LinearDataCenterOptimizer:
    """Simplified linear optimizer for GLPK compatibility."""

    def __init__(self, use_supabase: bool = True):
        """Initialize with Arizona data center parameters.

        Args:
            use_supabase: Whether to use Supabase for data and result storage
        """
        self.total_capacity_mw = 50.0
        self.critical_load_mw = 30.0
        self.flexible_load_mw = 20.0
        self.cooling_capacity_mw = 15.0

        # Simplified cooling parameters
        self.water_cooling_energy = 7.5  # MW for water cooling
        self.chiller_energy = 18.0  # MW for chiller cooling
        self.water_usage_per_hour = 120  # gallons/hour when using water
        self.water_cost_per_gallon = 0.004

        self.model = None
        self.results = None

        # Initialize data interface if available
        self.data_interface = None
        if use_supabase and DATA_INTERFACE_AVAILABLE:
            try:
                self.data_interface = DataInterface(use_supabase=True)
            except Exception as e:
                print(f"Could not initialize data interface: {e}")
                self.data_interface = None

    def build_model(self,
                   temperatures: List[float],
                   electricity_prices: List[float],
                   grid_demand: Optional[List[float]] = None) -> pyo.ConcreteModel:
        """Build simplified linear model."""

        model = pyo.ConcreteModel()

        # Sets
        model.hours = pyo.RangeSet(0, 23)

        # Parameters
        model.temp = pyo.Param(model.hours, initialize=dict(enumerate(temperatures)))
        model.price = pyo.Param(model.hours, initialize=dict(enumerate(electricity_prices)))

        # Decision Variables
        # Flexible load at each hour
        model.batch_load = pyo.Var(model.hours, bounds=(0, self.flexible_load_mw))

        # Binary: use water cooling (1) or chiller (0)
        model.use_water = pyo.Var(model.hours, domain=pyo.Binary)

        # Total electricity consumption
        model.total_load = pyo.Var(model.hours, bounds=(0, 100))

        # Constraints
        # 1. Must complete all batch processing
        model.batch_completion = pyo.Constraint(
            expr=sum(model.batch_load[h] for h in model.hours) >= self.flexible_load_mw * 8
        )

        # 2. Calculate total electricity load (linearized)
        def load_calc_rule(model, h):
            # Base load + batch + cooling energy
            return model.total_load[h] == (
                self.critical_load_mw +
                model.batch_load[h] +
                model.use_water[h] * self.water_cooling_energy +
                (1 - model.use_water[h]) * self.chiller_energy
            )
        model.load_calculation = pyo.Constraint(model.hours, rule=load_calc_rule)

        # 3. Capacity limit
        def capacity_rule(model, h):
            return model.total_load[h] <= self.total_capacity_mw * 1.2
        model.capacity_limit = pyo.Constraint(model.hours, rule=capacity_rule)

        # 4. Prefer water cooling when hot (soft constraint via objective)
        # No hard constraint to keep it linear

        # Objective: Minimize total cost
        def objective_rule(model):
            electricity_cost = sum(
                model.total_load[h] * model.price[h] / 1000
                for h in model.hours
            )

            water_cost = sum(
                model.use_water[h] * self.water_usage_per_hour * self.water_cost_per_gallon
                for h in model.hours
            )

            # Add penalty for not using water cooling when hot
            temp_penalty = sum(
                (1 - model.use_water[h]) * max(0, model.temp[h] - 95) * 0.1
                for h in model.hours
            )

            return electricity_cost + water_cost + temp_penalty

        model.objective = pyo.Objective(rule=objective_rule, sense=pyo.minimize)

        self.model = model
        return model

    def solve(self, solver_name: str = 'glpk') -> Dict:
        """Solve the linear model."""
        if self.model is None:
            raise ValueError("Model not built. Call build_model() first.")

        solver = SolverFactory(solver_name)

        if solver.available():
            print(f"Solving with {solver_name}...")
            results = solver.solve(self.model, tee=False)

            if results.solver.termination_condition == pyo.TerminationCondition.optimal:
                print("✅ Optimal solution found!")
                self.results = self._extract_results()
                return self.results
            else:
                print(f"Solution status: {results.solver.termination_condition}")
        else:
            raise RuntimeError(f"Solver {solver_name} not available")

        return {}

    def _extract_results(self) -> Dict:
        """Extract results from solved model."""
        results = {
            'hourly_data': [],
            'summary': {},
            'savings': {},
            'environmental': {},
            # Arrays for Supabase storage
            'batch_load': [],
            'cooling_mode': [],
            'temperatures': [],
            'electricity_prices': [],
            'hourly_costs': [],
            'water_usage': []
        }

        total_elec_cost = 0
        total_water_cost = 0
        total_water_used = 0
        peak_demand = 0

        # Store temperature and price data
        temperatures = []
        prices = []

        for h in self.model.hours:
            hourly = {
                'hour': h,
                'batch_load_mw': pyo.value(self.model.batch_load[h]),
                'water_cooling': int(pyo.value(self.model.use_water[h])),
                'total_load_mw': pyo.value(self.model.total_load[h]),
                'electricity_price': pyo.value(self.model.price[h]),
                'temperature': pyo.value(self.model.temp[h])
            }

            elec_cost = hourly['total_load_mw'] * hourly['electricity_price'] / 1000
            water_cost = hourly['water_cooling'] * self.water_usage_per_hour * self.water_cost_per_gallon
            water_usage = hourly['water_cooling'] * self.water_usage_per_hour

            hourly['electricity_cost'] = elec_cost
            hourly['water_cost'] = water_cost

            total_elec_cost += elec_cost
            total_water_cost += water_cost
            total_water_used += water_usage
            peak_demand = max(peak_demand, hourly['total_load_mw'])

            # Store for arrays
            results['batch_load'].append(hourly['batch_load_mw'])
            results['cooling_mode'].append('water' if hourly['water_cooling'] else 'electric')
            temperatures.append(hourly['temperature'])
            prices.append(hourly['electricity_price'])
            results['hourly_costs'].append(elec_cost + water_cost)
            results['water_usage'].append(water_usage)

            results['hourly_data'].append(hourly)

        # Store temperature and price arrays
        results['temperatures'] = temperatures
        results['electricity_prices'] = prices

        # Calculate baseline (no optimization)
        baseline_cost = (self.critical_load_mw + self.flexible_load_mw/3 + self.chiller_energy) * 24 * 70 / 1000

        results['summary'] = {
            'total_cost': total_elec_cost + total_water_cost,
            'electricity_cost': total_elec_cost,
            'water_cost': total_water_cost,
            'peak_demand_mw': peak_demand,
            'baseline_cost': baseline_cost
        }

        results['savings'] = {
            'daily_savings': baseline_cost - results['summary']['total_cost'],
            'annual_savings': (baseline_cost - results['summary']['total_cost']) * 365,
            'percentage_saved': ((baseline_cost - results['summary']['total_cost']) / baseline_cost * 100)
        }

        results['environmental'] = {
            'water_used_gallons': total_water_used,
            'water_saved_gallons': max(0, 120 * 12 * 24 - total_water_used),
            'peak_reduction_mw': max(0, 50 - peak_demand),
            'carbon_avoided_tons': results['savings']['daily_savings'] * 0.0004
        }

        # Add metadata for Supabase
        results['total_cost'] = results['summary']['total_cost']
        results['electricity_cost'] = total_elec_cost
        results['water_cost'] = total_water_cost
        results['baseline_cost'] = baseline_cost
        results['cost_savings'] = results['savings']['daily_savings']
        results['cost_savings_percent'] = results['savings']['percentage_saved']
        results['total_water_gallons'] = total_water_used
        results['peak_demand'] = peak_demand
        results['average_load'] = sum(results['batch_load']) / len(results['batch_load']) + self.critical_load_mw
        results['water_saved'] = results['environmental']['water_saved_gallons']
        results['carbon_avoided'] = results['environmental']['carbon_avoided_tons']
        results['max_temp'] = max(temperatures)
        results['min_temp'] = min(temperatures)
        results['avg_temp'] = sum(temperatures) / len(temperatures)
        results['status'] = 'completed'
        results['solver_time'] = 0  # Will be updated if we track it

        return results

    def save_results_to_supabase(self) -> Optional[str]:
        """Save optimization results to Supabase.

        Returns:
            Run ID if saved successfully, None otherwise
        """
        if self.results and self.data_interface:
            try:
                run_id = self.data_interface.save_optimization_results(self.results)
                if run_id:
                    print(f"✅ Results saved to Supabase with run_id: {run_id}")
                    self.results['run_id'] = run_id
                return run_id
            except Exception as e:
                print(f"Error saving to Supabase: {e}")
                return None
        else:
            if not self.results:
                print("No results to save")
            if not self.data_interface:
                print("Supabase not configured")
            return None

    def optimize_with_supabase(self, date: Optional[datetime] = None, solver_name: str = 'glpk') -> Dict:
        """Run optimization using data from Supabase.

        Args:
            date: Date to optimize for (defaults to Aug 1, 2024)
            solver_name: Solver to use

        Returns:
            Optimization results dictionary
        """
        if date is None:
            date = datetime(2024, 8, 1)  # Default to Aug 1, 2024

        if not self.data_interface:
            print("Data interface not available")
            return {}
        else:
            # Get data from Supabase/API
            opt_data = self.data_interface.prepare_optimization_data(date=date)
            temperatures = opt_data['temperatures']
            prices = opt_data['electricity_prices']

        # Build and solve model
        self.build_model(temperatures, prices)
        results = self.solve(solver_name)

        # Save to Supabase if available
        if results and self.data_interface:
            self.save_results_to_supabase()

        return results

