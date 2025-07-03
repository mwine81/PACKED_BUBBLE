from config import *
import polars as pl
from polars import col as c
import polars.selectors as cs
import re
from datetime import date

def load_medispan():
    """Load the Medispan dataset."""
    data = pl.scan_parquet(MEDISPAN)
    return data

def get_oral_solid_dosage_forms_ndcs() -> list[str]:
    """Get oral solid dosage forms from Medispan."""
    data = load_medispan().filter(c.is_rx).filter(c.dosage_form.str.contains('(?i)tab|cap')).select(c.ndc).collect().to_series().to_list()
    return data

def generate_date_id_table(data):
        (
        data
        .sort(c.effective_date)
        .select(c.effective_date.dt.month_start(), c.date_filter)
        .unique()
        .sort(c.effective_date)
        .with_row_index('date_id', 1)
        .sink_parquet('data/date_id.parquet')
        )
        return data
    
def add_date_id(data):
    date_id = pl.scan_parquet('data/date_id.parquet')
    return data.join(date_id, on='date_filter').drop('date_filter')

def generate_nadac_table():
    max_effective_date_filter = c.effective_date ==  c.effective_date.max().over(['ndc',c.effective_date.dt.month_start()])
    previous_price = c.unit_price.shift(1).over(c.ndc).alias('previous_unit_price')
    date_filter = c.effective_date.cast(pl.String).str.slice(0,7).alias('date_filter')
    
    (
        pl.scan_parquet(NADAC)
        .filter(c.classification == 'G')
        .filter(c.is_rx)
        .filter(c.ndc.is_in(get_oral_solid_dosage_forms_ndcs()))
        .sort(by=['ndc','effective_date','as_of'])
        .unique(subset=['ndc', 'effective_date'], keep='first')
        .filter(max_effective_date_filter)
        .sort(by=['ndc','effective_date'])
        .with_columns(previous_price)
        .with_columns(date_filter)
        .drop(cs.matches('(?i)as_of|explanation|classification|is_rx|updated'))
        .pipe(generate_date_id_table)
        .pipe(add_date_id)
        .sink_parquet('data/nadac.parquet')
    )

# def load_nadac():
#     max_effective_date_filter = c.effective_date ==  c.effective_date.max().over(['ndc',c.effective_date.dt.month_start()])
#     previous_price = c.unit_price.shift(1).over(c.ndc).alias('previous_unit_price')
#     date_filter = c.effective_date.cast(pl.String).str.slice(0,7).alias('date_filter')
  
    
#     data = (
#         pl.scan_parquet(NADAC)
#         .sort(by=['ndc','effective_date'])
#         .filter(max_effective_date_filter)
#         .with_columns(previous_price)
#         .with_columns(date_filter)
#     )
    
#     return data

def load_nadac():
    """Load the NADAC dataset."""
    data = pl.scan_parquet('data/nadac.parquet')
    return data

def generate_sdud_table():
    """Load the SDUD dataset."""
    #get current year
    current_year = date.today().year
    # return if file name contains 2023 or 2024
    files = [file for file in SDUD_DIR.iterdir() if re.match(r'(?i)state', file.name) and re.search(rf'{current_year}|{current_year-1}|{current_year-2}', file.name)]
    data = pl.scan_parquet(files)
    most_recent = data.select(c.year,c.quarter).unique().sort(c.year, c.quarter, descending=True).head(4)
    data = (
        data
        .join(most_recent, on= ['year', 'quarter'],)
        .group_by(c.state, c.ndc)
        .agg(cs.matches('(?i)units|rx').cast(pl.Float64).sum())
        .filter(c.ndc.is_in(load_nadac().select(c.ndc).collect().to_series().to_list()))
    )
    data.sink_parquet('data/sdud.parquet')
    return data

def update_tables():
    """Generate all necessary tables."""
    Path('data').mkdir(parents=True, exist_ok=True)
    generate_nadac_table()
    generate_sdud_table()
    return True

def load_sdud():
    """Load the SDUD dataset."""
    data = pl.scan_parquet('data/sdud.parquet')
    return data


def fetch_data(date_id: int, state_filter: str, group_by_col: list[str], **kwargs) -> pl.LazyFrame:
    # add context for group_by_col to be either 'product' or 'product_group'
    # if group_by_col not in ['product', 'product_group']:
    #     raise ValueError("group_by_col must be either 'product' or 'product_group'")

    unit_price_change = (c.unit_price - c.previous_unit_price).alias('unit_price_change')
    new_nadac = (c.unit_price * c.units).round(2).alias('new_nadac')
    old_nadac = (c.previous_unit_price * c.units).round(2).alias('old_nadac')
    total_diff = (new_nadac - old_nadac).alias('total_diff')
    diff_per_rx = (c.total_diff / c.rx_count).round(2).alias('diff_per_rx')



    # avg nadac change per unit
    def avg_unit_change() -> pl.Expr:
        return (c.avg_new_nadac - c.avg_old_nadac).round(2).alias('avg_unit_change')
    
    def classification()-> pl.Expr:
        # when total_diff is less than 0, return 'Decrease', otherwise return 'Increase'
        return pl.when(c.total_diff < 0).then(pl.lit('Decrease')).otherwise(pl.lit('Increase')).alias('classification')
    # add absolut columns for charting
    def abs_diff_col() -> pl.Expr:
        return cs.matches('(?i)diff').abs().name.suffix('_abs')
    
    # calculate percent change from old nadac to new nadac
    def percent_change() -> pl.Expr:
        return ((c.new_nadac - c.old_nadac) / c.old_nadac).round(4).alias('percent_change')    

    avg_new_nadac = (c.new_nadac / c.units).round(4).alias('avg_new_nadac')
    avg_old_nadac = (c.old_nadac / c.units).round(4).alias('avg_old_nadac')
    
    sdud = load_sdud().filter(c.state == state_filter)
    nadac = load_nadac().filter(c.date_id == date_id)

    data = sdud.join(nadac, on='ndc').with_columns(unit_price_change.round(4),new_nadac, old_nadac).with_columns((c.new_nadac - c.old_nadac).alias('total_diff'))
    data = data.join(load_medispan().select(c.ndc, c.generic_name.alias('product'), c.gpi_10_generic_name.alias('product_group')), on='ndc')

    
    if kwargs.get('product_group_filter'):
        data = data.filter(c.product_group == kwargs['product_group_filter'])
    
    data = (
        data
        .group_by(group_by_col)
        .agg(pl.col(['units','rx_count','total_diff', 'new_nadac', 'old_nadac']).sum())
        .with_columns(avg_new_nadac, avg_old_nadac)
        .with_columns(diff_per_rx)
        .with_columns(abs_diff_col(), classification(), avg_unit_change(), percent_change())
    )
    return data

if __name__ == "__main__":
    pass
    update_tables()
    
    #generate_nadac_table()
    #load_nadac().collect().glimpse()
    #load_medispan().filter(c.gpi_8_base_name == 'Methylphenidate').collect().glimpse()
    # date_filter = '2025-02'
    # state_filter = 'XX'  # Example state filter, replace with actual state code if needed
    #fetch_data(date_filter, 'XX').select(c.total_diff).sum().collect().glimpse()#.sort(c.total_diff).collect().glimpse()
    
