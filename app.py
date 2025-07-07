import dash
from dash import html,dcc, Output, Input, ctx
from interface import UI
from helpers import base_query, fetch_data, aggregate_data
from figures import scatter_plot, map_fig
from polars import col as c

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Hello, Dash!"),
    html.P("This is a basic Dash app."),
    UI.product_type_dropdown(),
    UI.product_group_dropdown(),
    UI.product_dropdown(),
    UI.state_dropdown(),
    UI.date_dropdown(),
    # UI.change_dropdown(),
    
    dcc.Graph(id='change-graph'),
    dcc.Graph(id='state-graph')
])

@app.callback(
    Output('change-graph', 'figure'),
    Output('product-group-dropdown', 'options'),
    Output('product-dropdown', 'options'),
    Output('state-graph', 'figure'),
    Input('product-type-dropdown', 'value'),
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    Input('product-dropdown', 'value'),
    Input('product-group-dropdown', 'value'),
)
def update_graph(product_view, state, date, product_dropdown, product_group_dropdown):
    base = base_query(date_id=date)


    # if ctx.triggered_id == 'product-dropdown
    base_data = fetch_data(base)

    product_groups = base_data.select(c.product_group).unique().sort(c.product_group).collect(engine='streaming').to_series().to_list()
    if product_group_dropdown:
        base_data = base_data.filter(c.product_group.is_in(product_group_dropdown))
    
    products = base_data.select(c.product).unique().sort(c.product).collect(engine='streaming').to_series().to_list()
    if product_dropdown:
        base_data = base_data.filter(c.product.is_in(product_dropdown))   
    

    state_data = aggregate_data(base_data, 'state').sort(c.diff_per_rx, descending=False).collect(engine='streaming')
    state_fig = map_fig(state_data)
    fig_data = aggregate_data(base_data.filter(c.state == state), 'product_group' if product_view == 'product_group' else 'product')

    fig = scatter_plot(fig_data.collect(engine='streaming'))

    return fig, product_groups, products, state_fig

if __name__ == '__main__':
    app.run(debug=True)