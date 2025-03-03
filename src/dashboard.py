"""
Streamlit dashboard for the stocks picking competition.
"""
import os
import sys
# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from src.db.database import get_db, init_db
from src.db.models import Position, DailyPrice, ExchangeRate, PortfolioValue, PerformanceMetric
from src.data_processing import process_all_data

# Set page config
st.set_page_config(
    page_title="Stocks Picking Competition",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
init_db()

# Sidebar
st.sidebar.title("Stocks Picking Competition")
st.sidebar.subheader("H1 2025 Analyst Paper Trading Challenge")

# Check if data exists in the database
db = get_db()
positions_count = db.query(Position).count()

if positions_count == 0:
    st.sidebar.warning("No data found in the database. Click the button below to load data.")
    if st.sidebar.button("Load Initial Data"):
        with st.spinner("Loading data... This may take a few minutes."):
            process_all_data(db)
        st.sidebar.success("Data loaded successfully!")
        st.rerun()
else:
    # Add refresh button
    if st.sidebar.button("Refresh Data"):
        with st.spinner("Refreshing data... This may take a few minutes."):
            process_all_data(db)
        st.sidebar.success("Data refreshed successfully!")
        st.rerun()

# Get the last updated date
last_updated = db.query(PerformanceMetric.last_updated).order_by(PerformanceMetric.last_updated.desc()).first()
if last_updated:
    st.sidebar.info(f"Last updated: {last_updated[0]}")

# Main content
st.title("Stocks Picking Competition Dashboard")

# Get data from database
positions = db.query(Position).all()
performance_metrics = db.query(PerformanceMetric).all()

# Create DataFrames
positions_df = pd.DataFrame([
    {
        'name': p.name,
        'ticker': p.ticker,
        'exchange': p.exchange,
        'weight': p.weight,
        'formatted_ticker': p.formatted_ticker,
        'shares': p.shares,
        'price_at_start': p.price_at_start,
        'allocation_usd': p.allocation_usd
    }
    for p in positions
])

performance_df = pd.DataFrame([
    {
        'Portfolio': p.name,
        'Initial Value': p.initial_value,
        'Final Value': p.final_value,
        'Total Return (%)': p.total_return_pct,
        'Annualized Return (%)': p.annualized_return_pct
    }
    for p in performance_metrics
])

# Calculate person weights
person_weights = {}
for position in positions:
    name = position.name
    weight = position.weight
    
    if name not in person_weights:
        person_weights[name] = 0
    
    person_weights[name] += weight

# Create a DataFrame for visualization
weights_df = pd.DataFrame({
    'Person': list(person_weights.keys()),
    'Total Weight': list(person_weights.values())
})

# Add a status column
weights_df['Status'] = weights_df['Total Weight'].apply(lambda x: 'OK' if abs(x - 1.0) < 0.01 else 'ERROR - Not 100%')

# Display portfolio weight distribution
st.header("Portfolio Weight Distribution")
col1, col2 = st.columns([2, 1])

with col1:
    fig = px.bar(
        weights_df, 
        x='Person', 
        y='Total Weight',
        color='Status',
        title='Portfolio Weight Distribution by Person',
        text='Total Weight',
        color_discrete_map={'OK': 'green', 'ERROR - Not 100%': 'red'}
    )
    
    # Add a horizontal line at 1.0 (100%)
    fig.add_shape(
        type="line",
        x0=-0.5,
        y0=1.0,
        x1=len(weights_df)-0.5,
        y1=1.0,
        line=dict(color="black", width=2, dash="dash")
    )
    
    # Format the y-axis as percentage
    fig.update_layout(
        yaxis=dict(title="Total Weight", tickformat=".0%"),
        template='presentation'
    )
    
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Weight Summary")
    for name, total_weight in person_weights.items():
        status = 'OK' if abs(total_weight - 1.0) < 0.01 else 'ERROR - Not 100%'
        st.write(f"{name}: {total_weight:.2f} ({status})")

# Display portfolio allocations
st.header("Portfolio Allocations")

# Check if we have allocation data
if 'allocation_usd' in positions_df.columns:
    # Filter out positions without allocations
    allocations_df = positions_df[positions_df['allocation_usd'].notna()].copy()
    
    # Only proceed if we have data
    if not allocations_df.empty:
        # Calculate normalized weight
        normalized_weights = []
        for _, row in allocations_df.iterrows():
            normalized_weights.append(row['weight'] / person_weights[row['name']])
        
        allocations_df['normalized_weight'] = normalized_weights
        
        fig = px.treemap(
            allocations_df,
            path=['name', 'formatted_ticker'],
            values='allocation_usd',
            title='Portfolio Allocations by Person and Stock',
            color='allocation_usd',
            color_continuous_scale='Viridis',
            hover_data=['normalized_weight']
        )
        
        fig.update_layout(template='presentation')
        fig.update_traces(texttemplate="%{label}<br>$%{value:,.0f}<br>%{customdata[0]:.1%}", textposition="middle center")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No allocation data available yet. Please load initial data.")
else:
    st.info("No allocation data available yet. Please load initial data.")

# Get portfolio values from database
portfolio_values_data = db.query(PortfolioValue).order_by(PortfolioValue.date).all()

# Create DataFrame
portfolio_values = pd.DataFrame([
    {
        'date': pv.date,
        'name': pv.name,
        'value': pv.value,
        'pct_change': pv.pct_change
    }
    for pv in portfolio_values_data
])

# Pivot the data
if not portfolio_values.empty:
    portfolio_values_pivot = portfolio_values.pivot(index='date', columns='name', values='value')
    pct_change_pivot = portfolio_values.pivot(index='date', columns='name', values='pct_change')
    
    # Display portfolio performance
    st.header("Portfolio Performance")
    
    # Plot the portfolio values over time
    fig = go.Figure()
    
    # Plot individual portfolios
    for name in portfolio_values_pivot.columns:
        fig.add_trace(go.Scatter(
            x=portfolio_values_pivot.index,
            y=portfolio_values_pivot[name],
            name=f"{name}'s Portfolio",
            mode='lines'
        ))
    
    # Add a horizontal line for initial investment
    fig.add_shape(
        type="line",
        x0=portfolio_values_pivot.index[0],
        y0=100000,  # Initial investment
        x1=portfolio_values_pivot.index[-1],
        y1=100000,
        line=dict(color="red", width=2, dash="dash")
    )
    
    # Update layout
    fig.update_layout(
        title='Portfolio Performance Since Start Date',
        xaxis_title='Date',
        yaxis_title='Portfolio Value (USD)',
        template='presentation',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Plot the percentage change
    fig = go.Figure()
    
    # Plot individual portfolios
    for name in pct_change_pivot.columns:
        fig.add_trace(go.Scatter(
            x=pct_change_pivot.index,
            y=pct_change_pivot[name],
            name=f"{name}'s Portfolio",
            mode='lines'
        ))
    
    # Add a horizontal line at 0%
    fig.add_shape(
        type="line",
        x0=pct_change_pivot.index[0],
        y0=0,
        x1=pct_change_pivot.index[-1],
        y1=0,
        line=dict(color="red", width=2, dash="dash")
    )
    
    # Update layout
    fig.update_layout(
        title='Portfolio Performance (% Change) Since Start Date',
        xaxis_title='Date',
        yaxis_title='Percentage Change (%)',
        template='presentation',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Display performance metrics
st.header("Performance Summary")

if not performance_df.empty:
    # Sort the data to make the visualization more informative
    performance_data = performance_df.sort_values('Total Return (%)', ascending=False)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Create the figure with increased size
        fig = px.bar(
            performance_data,
            x='Portfolio',
            y='Total Return (%)',
            title='Portfolio Returns by Competitor',
            color='Total Return (%)',
            color_continuous_scale=['red', 'yellow', 'green'],
            text='Total Return (%)'
        )
        
        # Add a horizontal line at 0%
        fig.add_shape(
            type="line",
            x0=-0.5,
            y0=0,
            x1=len(performance_data)-0.5,
            y1=0,
            line=dict(color="black", width=2, dash="dash")
        )
        
        # Format the text labels and ensure they're visible
        fig.update_traces(
            texttemplate='%{text:.1f}%', 
            textposition='outside',
            textfont=dict(size=14)  # Increase text size
        )
        
        # Calculate the y-axis range with extra padding to ensure text is visible
        y_min = min(performance_data['Total Return (%)']) * 1.2 if min(performance_data['Total Return (%)']) < 0 else min(performance_data['Total Return (%)']) - 2
        y_max = max(performance_data['Total Return (%)']) * 1.2 if max(performance_data['Total Return (%)']) > 0 else max(performance_data['Total Return (%)']) + 2
        
        # Update layout with increased size, margins, and adjusted y-axis
        fig.update_layout(
            template='presentation',
            yaxis=dict(
                title='Total Return (%)',
                range=[y_min, y_max]  # Ensure text labels are visible
            ),
            margin=dict(l=40, r=40, t=80, b=40),  # Increase margins
            title=dict(
                text='Portfolio Returns by Competitor',
                font=dict(size=20),  # Larger title
                y=0.95  # Position title higher
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Performance Metrics")
        st.dataframe(
            performance_data[['Portfolio', 'Initial Value', 'Final Value', 'Total Return (%)', 'Annualized Return (%)']],
            hide_index=True,
            use_container_width=True
        )

# Footer
st.markdown("---")
st.markdown("Stocks Picking Competition Dashboard | Data source: Yahoo Finance")
