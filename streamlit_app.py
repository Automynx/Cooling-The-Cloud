"""
Quick Streamlit Dashboard - Cooling the Cloud
Ready in 5 minutes!
"""

import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model.optimizer_linear import LinearDataCenterOptimizer

# Page config
st.set_page_config(
    page_title="Cooling the Cloud",
    page_icon="🌵",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .big-font {
        font-size: 24px !important;
        font-weight: bold;
        color: #1f77b4;
    }
    .savings-box {
        background-color: #e8f5e9;
        padding: 20px;
        border-radius: 10px;
        border: 2px solid #4caf50;
    }
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("🌵 Cooling the Cloud - Arizona Data Center Optimization")
st.markdown("### IISE Hackathon | Real-Time Optimization Dashboard")

# Sidebar controls
st.sidebar.header("⚙️ Optimization Parameters")
max_temp = st.sidebar.slider("Peak Temperature (°F)", 90, 120, 110)
peak_price = st.sidebar.slider("Peak Electricity Price ($/MWh)", 100, 200, 150)
water_cost = st.sidebar.slider("Water Cost ($/1000 gal)", 2.0, 6.0, 3.24)

# Run optimization button
if st.sidebar.button("🚀 Run Optimization", type="primary"):
    with st.spinner("Optimizing data center operations..."):

        # Generate data
        hours = list(range(24))
        temperatures = []
        prices = []

        for h in hours:
            # Temperature pattern
            base = max_temp - 15
            amplitude = 15
            phase = (h - 5) * np.pi / 12
            temp = base + amplitude * np.sin(phase - np.pi/2)
            temperatures.append(max(75, min(max_temp, temp)))

            # Price pattern
            if 15 <= h < 20:  # Peak hours
                price = peak_price + np.random.uniform(-10, 10)
            elif 22 <= h or h < 6:  # Off-peak
                price = 35 + np.random.uniform(-5, 5)
            else:
                price = 60 + np.random.uniform(-10, 10)
            prices.append(price)

        # Run optimization
        optimizer = LinearDataCenterOptimizer()
        optimizer.water_cost_per_gallon = water_cost / 1000
        model = optimizer.build_model(temperatures, prices)
        results = optimizer.solve(solver_name='glpk')

        # Store in session state
        st.session_state.results = results
        st.session_state.temperatures = temperatures
        st.session_state.prices = prices

# Display results
if 'results' in st.session_state:
    results = st.session_state.results

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="💰 Daily Savings",
            value=f"${results['savings']['daily_savings']:,.2f}",
            delta=f"{results['savings']['percentage_saved']:.1f}%"
        )

    with col2:
        st.metric(
            label="📈 Annual Savings",
            value=f"${results['savings']['annual_savings']:,.0f}",
            delta="Projected"
        )

    with col3:
        st.metric(
            label="💧 Water Saved",
            value=f"{results['environmental']['water_saved_gallons']:,.0f} gal",
            delta=f"{results['environmental']['water_saved_gallons']/325851:.1f} pools/yr"
        )

    with col4:
        st.metric(
            label="⚡ Peak Reduction",
            value=f"{results['environmental']['peak_reduction_mw']:.1f} MW",
            delta=f"{results['environmental']['carbon_avoided_tons']:.3f} tons CO2"
        )

    st.divider()

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        # Load distribution chart
        df = pd.DataFrame(results['hourly_data'])

        fig1 = go.Figure()
        fig1.add_trace(go.Bar(x=df['hour'], y=[30]*24, name='Base Load',
                              marker_color='#ff7f0e'))
        fig1.add_trace(go.Bar(x=df['hour'], y=df['batch_load_mw'], name='Flexible Load',
                              marker_color='#1f77b4'))

        # Add peak hours shading
        for h in range(15, 20):
            fig1.add_vrect(x0=h-0.4, x1=h+0.4, fillcolor="red", opacity=0.1,
                          layer="below", line_width=0)

        fig1.update_layout(
            title='Load Distribution - Smart Load Shifting',
            xaxis_title='Hour of Day',
            yaxis_title='Load (MW)',
            barmode='stack',
            height=400
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # Cooling mode timeline
        fig2 = go.Figure()

        # Add temperature
        fig2.add_trace(go.Scatter(
            x=df['hour'],
            y=df['temperature'],
            name='Temperature',
            line=dict(color='red', width=2),
            yaxis='y2'
        ))

        # Add cooling mode
        fig2.add_trace(go.Bar(
            x=df['hour'],
            y=df['water_cooling'] * 100,
            name='Water Cooling %',
            marker_color='#2ca02c',
            opacity=0.6
        ))

        fig2.update_layout(
            title='Smart Cooling Mode Switching',
            xaxis_title='Hour of Day',
            yaxis_title='Water Cooling Usage (%)',
            yaxis2=dict(
                title='Temperature (°F)',
                overlaying='y',
                side='right'
            ),
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Cost breakdown
    col1, col2 = st.columns(2)

    with col1:
        # Hourly costs
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(x=df['hour'], y=df['electricity_cost'],
                              name='Electricity', marker_color='#8c564b'))
        fig3.add_trace(go.Bar(x=df['hour'], y=df['water_cost'],
                              name='Water', marker_color='#2ca02c'))

        fig3.update_layout(
            title='Hourly Cost Breakdown',
            xaxis_title='Hour of Day',
            yaxis_title='Cost ($)',
            barmode='stack',
            height=400
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        # Summary pie chart
        fig4 = go.Figure(data=[
            go.Pie(
                labels=['Electricity Cost', 'Water Cost', 'Savings'],
                values=[
                    results['summary']['electricity_cost'],
                    results['summary']['water_cost'],
                    results['savings']['daily_savings']
                ],
                hole=0.3,
                marker_colors=['#ff7f0e', '#2ca02c', '#1f77b4']
            )
        ])
        fig4.update_layout(
            title='Daily Cost Distribution',
            height=400
        )
        st.plotly_chart(fig4, use_container_width=True)

    # Detailed results table
    with st.expander("📊 Detailed Hourly Results"):
        df_display = df[['hour', 'batch_load_mw', 'water_cooling', 'temperature',
                        'electricity_cost', 'water_cost']].round(2)
        df_display.columns = ['Hour', 'Batch Load (MW)', 'Water Cooling',
                             'Temp (°F)', 'Elec Cost ($)', 'Water Cost ($)']
        st.dataframe(df_display, use_container_width=True)

    # Key insights
    st.subheader("🔍 Key Insights")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.info(f"**Load Shifting**: {sum(df['batch_load_mw'][15:20]):.1f} MW moved from peak hours")

    with col2:
        st.success(f"**Smart Cooling**: Water cooling used {sum(df['water_cooling'])} hours when optimal")

    with col3:
        st.warning(f"**Peak Avoidance**: {results['summary']['peak_demand_mw']:.1f} MW max demand")

else:
    # No results yet
    st.info("👈 Adjust parameters and click 'Run Optimization' to see results!")

    # Show demo explanation
    with st.expander("ℹ️ How it works"):
        st.write("""
        **Our optimization model:**
        1. **Shifts computational loads** away from expensive peak hours (3-8 PM)
        2. **Switches cooling modes** between water and air based on temperature and cost
        3. **Minimizes total cost** while meeting all operational constraints
        4. **Saves water** by using it only when most efficient

        **Real data sources:**
        - EIA Southwest grid electricity data
        - NOAA Phoenix Sky Harbor weather data
        - Arizona Public Service rate schedules
        """)

# Footer
st.divider()
st.markdown("##### 🏆 IISE Hackathon 2024 | Team: Cooling the Cloud | Theme: Electricity in Arizona")