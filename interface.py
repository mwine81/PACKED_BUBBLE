from dash import html, dcc
from states import state_dict
import polars as pl
from polars import col as c
import polars.selectors as cs

def load_date_id():
    """Load the date_id dataset."""
    data = pl.scan_parquet('data/date_id.parquet')
    return data

# load_date_id and create dictionary with date_fiter as key and date_id as value
def create_date_id_dict():
    """Create a dictionary from the date_id dataset."""
    date_id_data = load_date_id()
    key = date_id_data.select(c.date_filter).collect().to_series().to_list()
    value = date_id_data.select(c.date_id).collect().to_series().to_list()
    return dict(zip(key, value))

class UI:
    @staticmethod
    def state_dropdown():
        return dcc.Dropdown(
            id='state-dropdown',
            options=[{'label': state, 'value': abbr} for abbr, state in state_dict.items()],
            value='XX',
            clearable=False  # Default value
        )
    
    @staticmethod
    def date_dropdown():
        date_id_dict = [{'label': date, 'value': date_id} for date, date_id in create_date_id_dict().items()]
        return dcc.Dropdown(
            id='date-dropdown',
            options=date_id_dict,#type: ignore
            value=date_id_dict[-1]['value'] 
        )
    
    @staticmethod
    def change_dropdown():
        return dcc.Dropdown(
            id='change-dropdown',
            options=[
                {'label': 'All', 'value': 'all'},
                {'label': 'Increase', 'value': 'Increase'},
                {'label': 'Decrease', 'value': 'Decrease'}
            ],
            value='all'
        )
    
    @staticmethod
    def product_dropdown():
        date_id_dict = [{'label': date, 'value': date_id} for date, date_id in create_date_id_dict().items()]
        return dcc.Dropdown(
            id='date-dropdown',
            options=date_id_dict,#type: ignore
            value=date_id_dict[-1]['value'] 
        )


if __name__ == '__main__':
    # Example usage of the UI class
    pass