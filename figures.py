import plotly.express as px
import polars as pl
from polars import col as c
import numpy as np
import polars.selectors as cs


def scatter_plot(df: pl.DataFrame):
    fig = px.scatter(
        df,
        x='rx_count',
        y='total_diff_abs',
        size='total_diff_abs',
        color='percent_change',
        title='Prescription Analysis: Total Difference vs Prescription Count',
        log_x=True,
        log_y=True,
        size_max=60,  # Slightly smaller max size to reduce overlap
        color_continuous_scale='Spectral_r',  # High contrast color scale for better visibility
    )

    # Custom hovertemplate
    hovertemplate = (
        "<b>Product:</b> %{customdata[0]}<br>"
        "<b>Classification:</b> %{customdata[1]}<br>"
        "<b>Avg Unit Change:</b> %{customdata[2]:$.2f}<br>"
        "<b>Avg New NADAC:</b> %{customdata[3]:$.2f}<br>"
        "<b>Avg Old NADAC:</b> %{customdata[4]:$.2f}<br>"
        "<b>Total Diff:</b> %{customdata[5]:$,.0f}<br>"
        "<b>Avg Diff Per Rx:</b> %{customdata[6]:$.2f}<br>"
        "<b>Rx Count:</b> %{customdata[7]:,.0f}<br>"
        "<b>Units:</b> %{customdata[8]:,.0f}<br>"
        "<b>Avg Percent Change:</b> %{customdata[9]:.1%}<br>"
        "<extra></extra>"
    )    # Prepare customdata for hovertemplate
    fig.update_traces(
        customdata=df[[
            'product_group',
            'classification',
            'avg_unit_change',
            'avg_new_nadac',
            'avg_old_nadac',
            'total_diff',
            'diff_per_rx',
            'rx_count',
            'units',
            'percent_change'
        ]].to_numpy(),        hovertemplate=hovertemplate,
        marker={
            'sizemin': 5,
              # Slightly smaller max size to reduce overlap
            'line': {'width': 2, 'color': 'rgba(44, 62, 80, 0.9)'},  # Darker, thicker borders
            'opacity': 0.7  # Reduced opacity to see overlapping bubbles
        },
    )

    fig.update_layout(
        title={
            'text': '<b>Prescription Analysis</b><br><sub>Total Difference vs Prescription Count</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 24, 'family': 'Inter, Segoe UI, Arial, sans-serif', 'color': '#2c3e50'}
        },
        xaxis={
            'title': {
                'text': '<b>Prescription Count</b> (log scale)', 
                'font': {'size': 16, 'family': 'Inter, Segoe UI, Arial, sans-serif', 'color': '#34495e'}
            },
            'tickformat': '~s',
            'gridcolor': 'rgba(189, 195, 199, 0.3)',
            'gridwidth': 1,
            'showline': True,
            'linecolor': 'rgba(149, 165, 166, 0.5)',
            'linewidth': 1,
            'tickfont': {'size': 12, 'color': '#7f8c8d'}
        },
        yaxis={
            'title': {
                'text': '<b>Total Difference</b> ($, log scale)', 
                'font': {'size': 16, 'family': 'Inter, Segoe UI, Arial, sans-serif', 'color': '#34495e'}
            },
            'tickformat': '$~s',
            'gridcolor': 'rgba(189, 195, 199, 0.3)',
            'gridwidth': 1,
            'showline': True,
            'linecolor': 'rgba(149, 165, 166, 0.5)',
            'linewidth': 1,
            'tickfont': {'size': 12, 'color': '#7f8c8d'}
        },        coloraxis={
            'cmin': -1,
            'cmax': 1,
            'colorscale': 'Spectral_r',  # High contrast color scale
            'colorbar': {
                'title': {
                    'text': '<b>Percent Change</b>', 
                    'font': {'size': 14, 'family': 'Inter, Segoe UI, Arial, sans-serif', 'color': '#2c3e50'}
                },
                'tickformat': '.0%',
                'orientation': 'h',
                'x': 0.5,
                'y': -0.25,
                'len': 0.8,
                'thickness': 20,
                'tickfont': {'size': 11, 'color': '#7f8c8d'},
                'bordercolor': 'rgba(149, 165, 166, 0.3)',
                'borderwidth': 1
            }
        },
        plot_bgcolor='rgba(255, 255, 255, 0.95)',  # Nearly white background for better contrast
        paper_bgcolor='white',
        font={'family': 'Inter, Segoe UI, Arial, sans-serif', 'size': 12, 'color': '#2c3e50'},
        margin={'l': 90, 'r': 90, 't': 120, 'b': 120},        width=1400,
        height=700,
        showlegend=False,
        # Configure hover mode for better interaction with overlapping points
        hovermode='closest',
        # Add subtle shadow effect
        annotations=[
            dict(
                text="",
                showarrow=False,
                xref="paper", yref="paper",
                x=0, y=0, xanchor='left', yanchor='bottom',
                xshift=-5, yshift=-5,
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1,
                bgcolor="rgba(0,0,0,0.02)",
                width=1410, height=710
            )
        ]
    )

    return fig