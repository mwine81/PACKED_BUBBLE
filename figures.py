import plotly.express as px
import polars as pl
from polars import col as c
import numpy as np
import polars.selectors as cs


def scatter_change_figure(data: pl.LazyFrame):
    df= data.with_columns((c.total_diff < 0).alias('savings')).with_columns(cs.matches('(?i)diff').abs()).collect().to_pandas()
    fig = px.scatter(
    df,
    x='rx_count',
    y='total_diff',
    size='total_diff_abs',
    color='savings',
    hover_data={'rx_count': True, 'total_diff': ':$,.0f', 'diff_per_rx': ':$,.2f'},
    labels={
        'rx_count': 'Prescription Count',
        'total_diff': 'Total Difference ($)',
        'diff_per_rx': 'Difference per Rx ($)'
    },
    title='Prescription Analysis: Total Difference vs Prescription Count',
    log_x=True,
    log_y=True,
    size_max=60,
    #color_continuous_scale='Viridis'
    )

    # Update layout for professional appearance and custom x-axis tick formatting
    fig.update_layout(
        title={
            'text': 'Prescription Analysis: Total Difference vs Prescription Count',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'family': 'Arial, sans-serif'}
        },
        xaxis={
            'title': {'text': 'Prescription Count (log scale)', 'font': {'size': 14}},
            'tickformat': '~s',  # SI prefix, e.g., 1K, 1M
            'gridcolor': 'lightgray',
            'gridwidth': 0.5
        },
        yaxis={
            'title': {'text': 'Total Difference ($, log scale)', 'font': {'size': 14}},
            'tickformat': '$~s',  # SI prefix with dollar sign, e.g., $1K, $1M
            'gridcolor': 'lightgray',
            'gridwidth': 0.5
        },
        coloraxis_colorbar={
            'title': {'text': 'Total Difference ($)', 'font': {'size': 12}},
            'tickformat': '$~s'  # SI prefix with dollar sign
        },
        plot_bgcolor='white',
        paper_bgcolor='white',
        font={'family': 'Arial, sans-serif', 'size': 12},
        margin={'l': 80, 'r': 80, 't': 100, 'b': 80},
        width=1400,
        height=800
    )

    # Update traces for better appearance
    fig.update_traces(
        marker={
            'sizemin': 4,
            'line': {'width': 0.5, 'color': 'white'},
            'opacity': 0.8
        },
        hovertemplate=
            '<b>Product:</b> %{customdata[0]}<br>' +
            '<b>Prescription Count:</b> %{x:,}<br>' +
            '<b>Total Difference:</b> $%{y:,.0f}<br>' +
            '<b>Difference per Rx:</b> $%{marker.size:.2f}<br>' +
            '<extra></extra>',
        customdata=df[['product_group']].values
)

    return fig