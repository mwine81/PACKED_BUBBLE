import dash
from dash import html,dcc, Output, Input, ctx
from interface import UI
from helpers import fetch_data
from figures import scatter_plot, bar_chart
import polars as pl
from polars import col as c
import polars.selectors as cs

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Hello, Dash!"),
    html.P("This is a basic Dash app."),
    UI.state_dropdown(),
    UI.date_dropdown(),
    UI.change_dropdown(),
    dcc.Graph(id='change-graph', figure={}, clear_on_unhover=True),
    dcc.Graph(id='bar-graph', figure={}, clear_on_unhover=True),
    # Hidden div to store current click state
    html.Div(id='click-state', style={'display': 'none'})
])

@app.callback(
    Output('click-state', 'children'),
    Input('change-graph', 'clickData'),
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    Input('change-dropdown', 'value')
)
def manage_click_state(clickData, state, date, change):
    # Only store click data if the graph was clicked
    if ctx.triggered_id == 'change-graph' and clickData:
        return clickData['points'][0]['customdata'][0] if clickData.get('points') else None
    # Clear click state when dropdowns change
    return None


@app.callback(
    [Output('change-graph', 'figure'),
     Output('change-graph', 'clickData')],
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    Input('change-dropdown', 'value')
)
def update_graph(state, date, change):
    data = fetch_data(date, state, 'product_group')
    
    if change != 'all':
        plot_data = data.filter(c.classification == change)
    else:
        plot_data = data

    return scatter_plot(plot_data.collect(engine='streaming')), None  # Clear clickData


@app.callback(
    [Output('bar-graph', 'figure'),
     Output('bar-graph', 'key')],  # Add key output to force re-render
    Input('click-state', 'children'),
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    prevent_initial_call=True
)
def update_bar_graph(product_group_filter, state, date):
    # Generate a unique key to force complete re-render
    unique_key = f"bar-{product_group_filter}-{state}-{date}"
    
    # Only show bar chart if there's a valid click state
    if product_group_filter is None:
        # Return a completely empty figure to clear any previous data
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'Click on a point in the scatter plot to see detailed product data',
                'xaxis': {'visible': False},
                'yaxis': {'visible': False}
            }
        }
        return empty_fig, unique_key
    
    
    bar_data = (
        fetch_data(date, state, 'product', product_group_filter=product_group_filter)
        .sort(by='total_diff')
        .collect(engine='streaming')
    )
    
    print(f"Bar data shape: {bar_data.shape}")
    print(f"Products in bar data: {bar_data.select('product').to_series().to_list()}")
    
    # Ensure we have data before creating the chart
    if bar_data.height == 0:
        empty_fig = {
            'data': [],
            'layout': {
                'title': f'No data found for {product_group_filter}',
                'xaxis': {'visible': False},
                'yaxis': {'visible': False}
            }
        }
        return empty_fig, unique_key
    
    # Create a completely fresh chart
    fig = bar_chart(bar_data)
    
    # Add a unique identifier to force complete refresh
    fig.update_layout(
        uirevision=None  # This forces complete refresh
    )
    
    return fig, unique_key
    


if __name__ == '__main__':
    app.run(debug=True)