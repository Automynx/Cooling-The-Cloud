"""
Linearized Optimization Model for Data Center (GLPK Compatible)
Simplified version that works with linear solvers
"""

import pyomo.environ as pyo
from pyomo.opt import SolverFactory
import numpy as np
from typing import Dict, List, Optional


class LinearDataCenterOptimizer:
    """Simplified linear optimizer for GLPK compatibility."""

    def __init__(self):
        """Initialize with Arizona data center parameters."""
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
            'environmental': {}
        }

        total_elec_cost = 0
        total_water_cost = 0
        total_water_used = 0
        peak_demand = 0

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

            hourly['electricity_cost'] = elec_cost
            hourly['water_cost'] = water_cost

            total_elec_cost += elec_cost
            total_water_cost += water_cost
            total_water_used += hourly['water_cooling'] * self.water_usage_per_hour
            peak_demand = max(peak_demand, hourly['total_load_mw'])

            results['hourly_data'].append(hourly)

        # Calculate baseline (no optimization)
        baseline_cost = (self.critical_load_mw + self.flexible_load_mw/3 + self.chiller_energy) * 24 * 70 / 1000

        results['summary'] = {
            'total_cost': total_elec_cost + total_water_cost,
            'electricity_cost': total_elec_cost,
            'water_cost': total_water_cost,
            'peak_demand_mw': peak_demand
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

        return results