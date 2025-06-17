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
        size_max=40,
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
    )

    # Prepare customdata for hovertemplate
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
        ]].to_numpy(),
        hovertemplate=hovertemplate,
        marker={
            'sizemin': 5,
            'line': {'width': 0.5, 'color': 'white'},
            'opacity': 0.8
        },
    )

    fig.update_layout(
        title={
            'text': 'Prescription Analysis: Total Difference vs Prescription Count',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis={
            'title': {'text': 'Prescription Count (log scale)', 'font': {'size': 14}},
            'tickformat': '~s',
            'gridcolor': 'lightgray',
            'gridwidth': 0.5
        },
        yaxis={
            'title': {'text': 'Total Difference ($, log scale)', 'font': {'size': 14}},
            'tickformat': '$~s',
            'gridcolor': 'lightgray',
            'gridwidth': 0.5
        },
        coloraxis_colorbar={
            'title': {'text': 'Percent Change', 'font': {'size': 12}},
            'tickformat': '.0%',
        },
        coloraxis={
            'cmin': -1,
            'cmax': 1,
            'colorbar': {
                'title': {'text': 'Percent Change', 'font': {'size': 12}},
                'tickformat': '.0%',
                # set horizontal colorbar
                'orientation': 'h',
                # move to bottom
                'x': 0.5,
                'y': -0.3,
            }
        },
   
        plot_bgcolor='white',
        paper_bgcolor='white',
        font={'family': 'Arial, sans-serif', 'size': 12},
        margin={'l': 80, 'r': 80, 't': 100, 'b': 80},
        width=1200,
        height=600
    )

    return fig