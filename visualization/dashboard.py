#!/usr/bin/env python3
"""
Interactive Dashboard for Data Center Optimization Results
IISE Hackathon - Cooling the Cloud
"""

import plotly.graph_objs as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np


def create_dashboard_plots(results, optimization_data=None):
    """Create all dashboard plots for the optimization results."""

    # Extract hourly data
    df = pd.DataFrame(results['hourly_data'])
    df['cooling_mode_text'] = df['water_cooling'].apply(lambda x: 'Water' if x else 'Chiller')

    # Create figure with subplots
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            'Load Distribution Over 24 Hours',
            'Cooling Mode Selection',
            'Hourly Costs Breakdown',
            'Temperature vs Cooling Decision',
            'Cost Savings Summary',
            'Environmental Impact'
        ),
        specs=[
            [{'type': 'bar'}, {'type': 'scatter'}],
            [{'type': 'bar'}, {'type': 'scatter'}],
            [{'type': 'indicator'}, {'type': 'indicator'}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.15
    )

    # Define colors
    colors = {
        'batch': '#1f77b4',  # Blue
        'base': '#ff7f0e',   # Orange
        'water': '#2ca02c',  # Green
        'chiller': '#d62728', # Red
        'temp': '#9467bd',   # Purple
        'price': '#8c564b'   # Brown
    }

    # 1. Load Distribution (Stacked Bar)
    fig.add_trace(
        go.Bar(
            x=df['hour'],
            y=[30] * 24,  # Base load
            name='Base Load',
            marker_color=colors['base'],
            text='30 MW',
            textposition='inside',
            hovertemplate='Hour %{x}<br>Base: 30 MW<extra></extra>'
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(
            x=df['hour'],
            y=df['batch_load_mw'],
            name='Batch Load',
            marker_color=colors['batch'],
            text=df['batch_load_mw'].round(1),
            textposition='outside',
            hovertemplate='Hour %{x}<br>Batch: %{y:.1f} MW<extra></extra>'
        ),
        row=1, col=1
    )

    # Add peak hours shading
    for h in range(15, 20):
        fig.add_vrect(
            x0=h-0.4, x1=h+0.4,
            fillcolor="red", opacity=0.1,
            layer="below", line_width=0,
            row=1, col=1
        )

    # 2. Cooling Mode Timeline
    fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['water_cooling'],
            mode='lines+markers',
            name='Cooling Mode',
            line=dict(color=colors['water'], width=3, shape='hv'),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(44, 160, 44, 0.3)',
            hovertemplate='Hour %{x}<br>Mode: %{text}<extra></extra>',
            text=df['cooling_mode_text']
        ),
        row=1, col=2
    )

    # Add temperature overlay
    fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=(df['temperature'] - 75) / 40,  # Normalize to 0-1
            mode='lines',
            name='Temperature',
            line=dict(color=colors['temp'], width=2, dash='dash'),
            yaxis='y2',
            hovertemplate='Hour %{x}<br>Temp: %{text}°F<extra></extra>',
            text=df['temperature'].round(1)
        ),
        row=1, col=2
    )

    # 3. Hourly Costs (Stacked Bar)
    fig.add_trace(
        go.Bar(
            x=df['hour'],
            y=df['electricity_cost'],
            name='Electricity Cost',
            marker_color=colors['price'],
            hovertemplate='Hour %{x}<br>Elec: $%{y:.2f}<extra></extra>'
        ),
        row=2, col=1
    )

    fig.add_trace(
        go.Bar(
            x=df['hour'],
            y=df['water_cost'],
            name='Water Cost',
            marker_color=colors['water'],
            hovertemplate='Hour %{x}<br>Water: $%{y:.2f}<extra></extra>'
        ),
        row=2, col=1
    )

    # 4. Temperature vs Cooling Decision (Scatter)
    fig.add_trace(
        go.Scatter(
            x=df['temperature'],
            y=df['batch_load_mw'],
            mode='markers',
            name='Load vs Temp',
            marker=dict(
                size=df['electricity_price'] / 10,
                color=df['water_cooling'],
                colorscale=['red', 'blue'],
                showscale=True,
                colorbar=dict(title="Cooling<br>Mode", x=1.15)
            ),
            text=[f"Hour {h}<br>Price: ${p:.0f}" for h, p in zip(df['hour'], df['electricity_price'])],
            hovertemplate='Temp: %{x:.1f}°F<br>Load: %{y:.1f} MW<br>%{text}<extra></extra>'
        ),
        row=2, col=2
    )

    # 5. Cost Savings Indicator
    fig.add_trace(
        go.Indicator(
            mode="number+delta+gauge",
            value=results['savings']['daily_savings'],
            delta={'reference': 0, 'valueformat': '.2f'},
            gauge={
                'axis': {'range': [0, 20]},
                'bar': {'color': colors['water']},
                'steps': [
                    {'range': [0, 5], 'color': "lightgray"},
                    {'range': [5, 15], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 10
                }
            },
            title={'text': "Daily Savings ($)"},
            domain={'y': [0, 1], 'x': [0, 1]}
        ),
        row=3, col=1
    )

    # 6. Environmental Impact
    water_saved_pools = results['environmental']['water_saved_gallons'] / 325851  # Olympic pool
    fig.add_trace(
        go.Indicator(
            mode="number+gauge",
            value=water_saved_pools,
            gauge={
                'axis': {'range': [0, 1]},
                'bar': {'color': colors['water']},
                'bgcolor': "lightblue",
                'steps': [
                    {'range': [0, 0.3], 'color': "lightblue"},
                    {'range': [0.3, 0.7], 'color': "cyan"}
                ]
            },
            title={'text': "Water Saved<br>(Olympic Pools/Year)"},
            domain={'y': [0, 1], 'x': [0, 1]}
        ),
        row=3, col=2
    )

    # Update layout
    fig.update_layout(
        title={
            'text': "🌵 Cooling the Cloud - Arizona Data Center Optimization Dashboard",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 24}
        },
        showlegend=True,
        height=900,
        barmode='stack',
        hovermode='x unified',
        template='plotly_white'
    )

    # Update axes
    fig.update_xaxes(title_text="Hour of Day", row=1, col=1)
    fig.update_yaxes(title_text="Load (MW)", row=1, col=1)

    fig.update_xaxes(title_text="Hour of Day", row=1, col=2)
    fig.update_yaxes(title_text="Cooling Mode", row=1, col=2)

    fig.update_xaxes(title_text="Hour of Day", row=2, col=1)
    fig.update_yaxes(title_text="Cost ($)", row=2, col=1)

    fig.update_xaxes(title_text="Temperature (°F)", row=2, col=2)
    fig.update_yaxes(title_text="Batch Load (MW)", row=2, col=2)

    return fig


def create_summary_table(results):
    """Create a summary table of key metrics."""

    metrics = [
        ["Daily Cost", f"${results['summary']['total_cost']:,.2f}"],
        ["Daily Savings", f"${results['savings']['daily_savings']:,.2f}"],
        ["Annual Savings", f"${results['savings']['annual_savings']:,.2f}"],
        ["Savings Rate", f"{results['savings']['percentage_saved']:.1f}%"],
        ["Peak Demand", f"{results['summary']['peak_demand_mw']:.1f} MW"],
        ["Water Used", f"{results['environmental']['water_used_gallons']:,.0f} gal"],
        ["Water Saved", f"{results['environmental']['water_saved_gallons']:,.0f} gal"],
        ["Carbon Avoided", f"{results['environmental']['carbon_avoided_tons']:.3f} tons/day"]
    ]

    df = pd.DataFrame(metrics, columns=['Metric', 'Value'])

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=['<b>Performance Metric</b>', '<b>Value</b>'],
            fill_color='paleturquoise',
            align='left',
            font=dict(size=14)
        ),
        cells=dict(
            values=[df['Metric'], df['Value']],
            fill_color='lavender',
            align='left',
            font=dict(size=12),
            height=30
        )
    )])

    fig.update_layout(
        title="Optimization Results Summary",
        height=400,
        margin=dict(l=0, r=0, t=40, b=0)
    )

    return fig


def save_dashboard(results, output_file='dashboard.html'):
    """Save dashboard as HTML file."""

    # Create all plots
    main_dashboard = create_dashboard_plots(results)
    summary_table = create_summary_table(results)

    # Combine into single HTML
    from plotly.offline import plot
    import plotly.io as pio

    # Save main dashboard
    main_dashboard.write_html(output_file)
    print(f"✅ Dashboard saved to {output_file}")

    # Save summary table
    summary_file = output_file.replace('.html', '_summary.html')
    summary_table.write_html(summary_file)
    print(f"✅ Summary saved to {summary_file}")

    return output_file


if __name__ == "__main__":
    # Test with sample results
    print("Testing dashboard with sample results...")

    # Create sample results
    sample_results = {
        'hourly_data': [
            {
                'hour': h,
                'batch_load_mw': 20 if h < 15 or h > 20 else 0,
                'water_cooling': 1 if h > 10 and h < 20 else 0,
                'total_load_mw': 50 + np.random.uniform(-5, 5),
                'electricity_price': 150 if 15 <= h < 20 else 50,
                'temperature': 95 + 15 * np.sin((h-5) * np.pi / 12),
                'electricity_cost': np.random.uniform(2, 10),
                'water_cost': np.random.uniform(0, 2) if h > 10 and h < 20 else 0
            }
            for h in range(24)
        ],
        'summary': {
            'total_cost': 81.77,
            'electricity_cost': 70.50,
            'water_cost': 11.27,
            'peak_demand_mw': 57.5
        },
        'savings': {
            'daily_savings': 10.07,
            'annual_savings': 3676.26,
            'percentage_saved': 11.0
        },
        'environmental': {
            'water_used_gallons': 2880,
            'water_saved_gallons': 31680,
            'peak_reduction_mw': 5.0,
            'carbon_avoided_tons': 0.004
        }
    }

    # Create and save dashboard
    output = save_dashboard(sample_results, 'test_dashboard.html')
    print(f"\n🎉 Dashboard created! Open {output} in your browser.")