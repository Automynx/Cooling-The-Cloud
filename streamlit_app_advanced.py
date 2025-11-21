"""
Cooling the Cloud - Data Center Optimization Dashboard
Arizona Data Center Cooling Optimization Platform with Historical Analysis
"""

import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model.optimizer_linear import LinearDataCenterOptimizer
from model.data_interface import DataInterface

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
st.markdown("### 2025 IISE Optimization Hackathon | Real-Time Dashboard")

# Sidebar controls
st.sidebar.header("⚙️ Optimization Parameters")

# Data source selection
data_source = st.sidebar.selectbox(
    "📊 Data Source",
    ["🌐 Supabase (Real Data)", "🎲 Demo Data"],
    index=0
)

# Period selection for historical data
st.sidebar.subheader("📊 Historical Analysis Period")
period_selection = st.sidebar.selectbox(
    "Select Time Period",
    ["None", "Last 30 Days", "Last 6 Months", "Last Year"],
    index=0,
    help="View aggregated historical data for different time periods",
    key="period_selector"
)

# Date selection - Always visible for daily view
st.sidebar.subheader("📅 Daily Optimization")
default_date = datetime(2024, 8, 1).date()
selected_date = st.sidebar.date_input(
    "Select Date for Daily View",
    default_date,
    min_value=datetime(2024, 1, 1).date(),
    max_value=datetime(2024, 12, 31).date(),
    key="daily_date_selector"
)

# Track user interactions to determine display mode
if 'last_period_selection' not in st.session_state:
    st.session_state.last_period_selection = "None"
if 'last_selected_date' not in st.session_state:
    st.session_state.last_selected_date = selected_date
if 'last_interaction' not in st.session_state:
    st.session_state.last_interaction = "daily"

# Determine what to display based on user interaction
date_changed = selected_date != st.session_state.last_selected_date
period_changed = period_selection != st.session_state.last_period_selection

if date_changed:
    display_mode = "daily"
    st.session_state.last_selected_date = selected_date
    st.session_state.last_interaction = "daily"
elif period_changed and period_selection != "None":
    display_mode = "period"
    st.session_state.last_period_selection = period_selection
    st.session_state.last_interaction = "period"
elif period_selection == "None":
    display_mode = "daily"
    st.session_state.last_interaction = "daily"
else:
    display_mode = st.session_state.get('last_interaction', 'daily')

# Update period tracking
st.session_state.last_period_selection = period_selection

# Show info about what's selected
if display_mode == "period" and period_selection != "None":
    st.sidebar.info(f"📊 Showing {period_selection.lower()} historical data")
else:
    st.sidebar.info(f"📅 Showing daily data for {selected_date.strftime('%B %d, %Y')}")

# Manual parameters (used for demo mode or overrides)
if "Demo" in data_source:
    st.sidebar.subheader("Demo Parameters")
    max_temp = st.sidebar.slider("Peak Temperature (°F)", 90, 120, 110)
    peak_price = st.sidebar.slider("Peak Electricity Price ($/MWh)", 100, 200, 150)
    water_cost = st.sidebar.slider("Water Cost ($/1000 gal)", 2.0, 6.0, 3.24)
else:
    max_temp = 110
    peak_price = 150
    water_cost = 3.24

# Run optimization button - ONLY for daily mode
if display_mode == "daily":
    if st.sidebar.button("🚀 Run Daily Optimization", type="primary"):
        with st.spinner("Optimizing data center operations..."):

            # Initialize optimizer with Supabase support
            use_supabase = "Supabase" in data_source
            optimizer = LinearDataCenterOptimizer(use_supabase=use_supabase)

            if use_supabase:
                # Try to use Supabase data
                try:
                    st.info("🔄 Fetching data from Supabase...")

                    # Convert date to datetime
                    if selected_date:
                        target_date = datetime.combine(selected_date, datetime.min.time())
                    else:
                        target_date = datetime(2024, 8, 1)

                    # Run optimization with Supabase data
                    results = optimizer.optimize_with_supabase(date=target_date, solver_name='glpk')

                    if results:
                        st.success("✅ Optimization complete using real data!")
                        temperatures = results.get('temperatures', [])
                        prices = results.get('electricity_prices', [])
                    else:
                        st.error("Failed to optimize with Supabase data")
                        results = None

                except Exception as e:
                    st.warning(f"⚠️ Supabase error: {e}")
                    st.info("Falling back to demo data...")
                    use_supabase = False

            if not use_supabase or not results:
                # Generate demo data
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

                # Run optimization with demo data
                optimizer = LinearDataCenterOptimizer(use_supabase=False)
                optimizer.water_cost_per_gallon = water_cost / 1000
                model = optimizer.build_model(temperatures, prices)
                results = optimizer.solve(solver_name='glpk')

                if use_supabase and optimizer.data_interface:
                    # Save demo results to Supabase
                    try:
                        run_id = optimizer.save_results_to_supabase()
                        if run_id:
                            st.info(f"💾 Results saved to Supabase (Run ID: {run_id})")
                    except:
                        pass

            # Store in session state
            if results:
                st.session_state.results = results
                st.session_state.temperatures = temperatures
                st.session_state.prices = prices
                st.session_state.data_source = data_source
else:
    # Period mode - show informational message
    if period_selection != "None":
        st.sidebar.info(f"📊 Viewing {period_selection} historical data\n\nHistorical data is preloaded from the database.")

# Display results based on display mode
if 'results' in st.session_state or display_mode == "period":

    # For historical periods, fetch data from Supabase
    if display_mode == "period" and period_selection != "None" and "Supabase" in data_source:
        with st.spinner(f"Loading {period_selection} data..."):
            try:
                supabase_interface = DataInterface(use_supabase=True).supabase

                # Determine number of days based on selection
                days_map = {
                    "Last 30 Days": 30,
                    "Last 6 Months": 180,
                    "Last Year": 365
                }

                if period_selection in days_map:
                    days = days_map[period_selection]
                    period_summary = supabase_interface.get_period_summary(days)

                    if period_summary and period_summary.get('total_savings', 0) > 0:
                        # Display period summary metrics
                        st.subheader(f"📊 {period_selection} Analysis")

                        # Show projection indicator if applicable
                        if period_summary.get('is_projection', False):
                            actual_days = period_summary.get('actual_days_with_data', 0)
                            st.info(f"📈 Projected from {actual_days} days of actual data to {days} days")

                        col1, col2, col3, col4 = st.columns(4)

                        with col1:
                            if period_summary.get('is_projection', False):
                                label = f"💰 Total Savings ({period_selection})"
                            else:
                                label = f"💰 Total Savings ({period_summary['days_analyzed']} days)"

                            st.metric(
                                label=label,
                                value=f"${period_summary['total_savings']:,.2f}",
                                delta=f"{period_summary['avg_savings_percent']:.1f}% avg"
                            )

                        with col2:
                            st.metric(
                                label="📊 Average Daily Savings",
                                value=f"${period_summary['avg_daily_savings']:,.2f}",
                                delta="Based on actual data"
                            )

                        with col3:
                            if period_summary.get('is_projection', False):
                                water_label = f"💧 Total Water ({period_selection})"
                            else:
                                water_label = "💧 Total Water Used"

                            st.metric(
                                label=water_label,
                                value=f"{period_summary['total_water_usage']/1000:.1f}k gal",
                                delta=f"{period_summary['avg_water_usage']:.0f} gal/day avg"
                            )

                        with col4:
                            st.metric(
                                label="⚡ Peak Demand",
                                value=f"{period_summary['max_peak_demand']:.1f} MW",
                                delta=f"{period_summary['avg_peak_demand']:.1f} MW avg"
                            )

                        st.divider()

                        # Get monthly breakdown if available
                        if days >= 30:
                            months = max(1, days // 30)
                            monthly_df = supabase_interface.get_monthly_breakdown(months)
                            if not monthly_df.empty:
                                st.subheader("📈 Monthly Breakdown")

                                # Prepare data for chart
                                monthly_data = {
                                    'months': monthly_df['month'].dt.strftime('%b %Y').tolist(),
                                    'savings': monthly_df['cost_savings'].tolist()
                                }

                                # Create chart of monthly savings
                                fig_monthly = go.Figure()
                                fig_monthly.add_trace(go.Bar(
                                    x=monthly_data['months'],
                                    y=monthly_data['savings'],
                                    marker_color='#1f77b4',
                                    text=[f"${s:,.0f}" for s in monthly_data['savings']],
                                    textposition='auto'
                                ))
                                fig_monthly.update_layout(
                                    title='Monthly Cost Savings Trend',
                                    xaxis_title='Month',
                                    yaxis_title='Total Savings ($)',
                                    height=400
                                )
                                st.plotly_chart(fig_monthly, use_container_width=True)

                        # Get daily trends
                        daily_trends = supabase_interface.get_daily_trends(days)
                        if daily_trends and len(daily_trends['dates']) > 0:
                            st.subheader("📊 Daily Trends")

                            col1, col2 = st.columns(2)

                            with col1:
                                # Savings trend
                                fig_savings = go.Figure()
                                fig_savings.add_trace(go.Scatter(
                                    x=daily_trends['dates'],
                                    y=daily_trends['savings'],
                                    mode='lines',
                                    name='Daily Savings',
                                    line=dict(color='#1f77b4', width=2)
                                ))
                                fig_savings.update_layout(
                                    title='Daily Savings Trend',
                                    xaxis_title='Date',
                                    yaxis_title='Savings ($)',
                                    height=350
                                )
                                st.plotly_chart(fig_savings, use_container_width=True)

                            with col2:
                                # Water usage trend
                                fig_water = go.Figure()
                                fig_water.add_trace(go.Scatter(
                                    x=daily_trends['dates'],
                                    y=daily_trends['water_usage'],
                                    mode='lines',
                                    name='Water Usage',
                                    line=dict(color='#2ca02c', width=2)
                                ))
                                fig_water.update_layout(
                                    title='Daily Water Usage Trend',
                                    xaxis_title='Date',
                                    yaxis_title='Water (gallons)',
                                    height=350
                                )
                                st.plotly_chart(fig_water, use_container_width=True)
                    else:
                        st.info(f"No historical data available for {period_selection}. Run optimizations to build up history.")
            except Exception as e:
                st.warning(f"Could not load historical data: {e}")

    # Display daily results when in daily mode
    if display_mode == "daily" and 'results' in st.session_state:
        results = st.session_state.results

        st.subheader(f"Daily Optimization Results - {selected_date}")

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
                label="📈 Annual Projection",
                value=f"${results['savings']['annual_savings']:,.0f}",
                delta="Projected annually"
            )

        with col3:
            st.metric(
                label="💧 Water Conservation",
                value=f"{results['environmental']['water_saved_gallons']:,.0f} gal",
                delta=f"{results['environmental']['water_saved_gallons']*365/1e6:.1f}M gal/yr"
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
            st.success(f"**Smart Cooling**: Water cooling utilized {sum(df['water_cooling'])} hours")

        with col3:
            st.warning(f"**Peak Demand**: {results['summary']['peak_demand_mw']:.1f} MW maximum")

else:
    # No results yet
    st.info("👈 Configure parameters and click 'Run Daily Optimization' to analyze")

    # Show demo explanation
    with st.expander("ℹ️ About This Tool"):
        st.markdown("""
        ### Arizona Data Center Optimization Platform

        **Our optimization model:**
        - **Shifts computational loads** away from expensive peak hours (3-8 PM)
        - **Switches cooling modes** between water and air based on temperature and cost
        - **Minimizes total cost** while meeting all operational constraints
        - **Conserves water** by using it only when most efficient

        **Real data sources:**
        - EIA Southwest grid electricity data (309,181 records)
        - Arizona electricity pricing ($128.4/MWh average)
        - Phoenix temperature patterns (up to 118°F)

        **Impact for a 50MW Data Center:**
        - Annual savings: $9,477
        - Water conservation: 11.6M gallons
        - Carbon reduction: 3.8 tons CO2
        """)

# Optimization History Section
st.divider()
st.header("📚 Optimization History")

# Try to load history from Supabase
try:
    data_interface = DataInterface(use_supabase=True)
    if data_interface.supabase:
        history_df = data_interface.get_optimization_history(limit=10)

        if not history_df.empty:
            # Format the dataframe for display
            history_df['run_timestamp'] = pd.to_datetime(history_df['run_timestamp'])
            history_df['date'] = history_df['run_timestamp'].dt.strftime('%Y-%m-%d %H:%M')

            # Select columns to display
            display_cols = ['date', 'total_cost', 'cost_savings_percent',
                          'total_water_usage_gallons', 'peak_demand_mw']

            if all(col in history_df.columns for col in display_cols):
                display_df = history_df[display_cols].copy()
                display_df.columns = ['Date', 'Total Cost ($)', 'Savings (%)',
                                     'Water Used (gal)', 'Peak Demand (MW)']

                # Format numeric columns
                display_df['Total Cost ($)'] = display_df['Total Cost ($)'].apply(lambda x: f"${x:,.2f}")
                display_df['Savings (%)'] = display_df['Savings (%)'].apply(lambda x: f"{x:.1f}%")
                display_df['Water Used (gal)'] = display_df['Water Used (gal)'].apply(lambda x: f"{x:,.0f}")
                display_df['Peak Demand (MW)'] = display_df['Peak Demand (MW)'].apply(lambda x: f"{x:.1f}")

                st.dataframe(display_df, use_container_width=True)

                # Add a chart showing cost savings trend
                if len(history_df) > 1:
                    fig_history = go.Figure()
                    fig_history.add_trace(go.Scatter(
                        x=history_df['run_timestamp'],
                        y=history_df['cost_savings_percent'],
                        mode='lines+markers',
                        name='Cost Savings %',
                        line=dict(color='#1f77b4', width=2),
                        marker=dict(size=8)
                    ))
                    fig_history.update_layout(
                        title='Cost Savings Trend',
                        xaxis_title='Date',
                        yaxis_title='Savings (%)',
                        height=300
                    )
                    st.plotly_chart(fig_history, use_container_width=True)
            else:
                st.info("No optimization history available yet. Run optimizations to see historical data.")
        else:
            st.info("No optimization history available yet. Run optimizations to see historical data.")
    else:
        st.info("Supabase not connected. History tracking is disabled.")
except Exception as e:
    st.info("History tracking is currently unavailable.")

# Footer
st.divider()
st.caption("2025 IISE Optimization Hackathon | Team: Cooling the Cloud | Arizona State University")