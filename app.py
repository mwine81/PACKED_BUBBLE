import dash
from dash import html,dcc, Output, Input, ctx
from interface import UI
from helpers import fetch_data
from figures import scatter_plot
from polars import col as c

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Hello, Dash!"),
    html.P("This is a basic Dash app."),
    UI.state_dropdown(),
    UI.date_dropdown(),
    UI.change_dropdown(),
    UI.product_dropdown(),
    dcc.Dropdown(
        id= 'product-dropdown',
    ),
    dcc.Graph(id='change-graph'),
])

@app.callback(
    Output('change-graph', 'figure'),
    Output('product-dropdown', 'options'),
    Input('state-dropdown', 'value'),
    Input('date-dropdown', 'value'),
    Input('change-dropdown', 'value'),
    Input('product-dropdown', 'value'),
)
def update_graph(state, date, change, product):

    data = fetch_data(date, state, ['product_group'])
    
    if change != 'all':
        plot_data = data.filter(c.classification == change)
    else:
        plot_data = data

    plot_data = plot_data.collect(engine='streaming')

    product_groups = plot_data.select(c.product_group).unique().to_series().to_list()
    if product:
        plot_data = fetch_data(date, state, ['product'], product_group_filter=product).collect(engine='streaming')
    fig = scatter_plot(plot_data) if not plot_data.is_empty() else {}
    return fig, product_groups






if __name__ == '__main__':
    app.run(debug=True)