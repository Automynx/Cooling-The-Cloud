"""
Vercel Serverless Function for API
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.optimizer_linear import LinearDataCenterOptimizer
from model.data_interface import DataInterface

app = Flask(__name__)
CORS(app)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'message': 'API running on Vercel'
    })

@app.route('/api/optimize', methods=['POST', 'OPTIONS'])
def run_optimization():
    """Run optimization with demo data."""
    if request.method == 'OPTIONS':
        return '', 200

    try:
        # Use demo data for Vercel deployment
        optimizer = LinearDataCenterOptimizer(use_supabase=False, capacity_mw=2000)
        data_interface = DataInterface(use_supabase=False)

        # Get demo data
        opt_data = data_interface.prepare_optimization_data(use_supabase=False)
        temperatures, prices, _ = data_interface.export_to_model_format(opt_data)

        # Run optimization
        model = optimizer.build_model(temperatures, prices)
        results = optimizer.solve(solver_name='highs')

        if results:
            return jsonify({
                'success': True,
                'results': {
                    'summary': results['summary'],
                    'savings': results['savings'],
                    'environmental': results['environmental'],
                    'hourly_data': results['hourly_data'],
                    'metadata': {
                        'source': 'demo',
                        'capacity_mw': 2000
                    }
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Optimization failed'
            }), 500

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Vercel requires the app to be exposed
handler = app