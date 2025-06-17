import dash
from dash import html,dcc, Output, Input
from interface import UI
from helpers import fetch_data
from figures import scatter_change_figure
import polars as pl
from polars import col as c
import polars.selectors as cs

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Hello, Dash!"),
    html.P("This is a basic Dash app."),
    UI.state_dropdown(),
    UI.date_dropdown(),
    dcc.Graph(id='change-graph', figure={}),
])

@app.callback(
    Output('change-graph', 'figure'),
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value')
)
def update_graph(state, date):
    data = fetch_data(date, state)
    
    avg_new_nadac = (c.new_nadac / c.units).round(4).alias('avg_new_nadac')
    avg_old_nadac = (c.old_nadac / c.units).round(4).alias('avg_old_nadac')
    diff_per_rx = (c.total_diff / c.rx_count).round(2).alias('diff_per_rx')
    data = (
        data
        .group_by('product_group')
        .agg(cs.numeric().exclude('avg_new_nadac','avg_old_nadac','diff_per_rx').sum())
        .with_columns(avg_new_nadac,
                     avg_old_nadac,
                     diff_per_rx = (c.total_diff / c.rx_count).round(2).alias('diff_per_rx')
                     )
    )

    return scatter_change_figure(data)


if __name__ == '__main__':
    app.run(debug=True)