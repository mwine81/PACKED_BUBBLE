import dash
from dash import html,dcc, Output, Input
from interface import UI
from helpers import fetch_data
from figures import scatter_plot
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
    data = fetch_data(date, state, 'product_group').collect(engine='streaming')
    
    return scatter_plot(data)


if __name__ == '__main__':
    app.run(debug=True)