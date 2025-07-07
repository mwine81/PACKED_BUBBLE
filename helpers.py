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


def load_nadac():
    """Load the NADAC dataset."""
    data = pl.scan_parquet('data/nadac.parquet')
    return data

def load_sdud():
    """Load the SDUD dataset."""
    data = pl.scan_parquet('data/sdud.parquet')
    return data

def add_generic_name(data: pl.LazyFrame) -> pl.LazyFrame:
    return data.join(load_medispan().select(c.ndc, c.generic_name.alias('product'), c.gpi_10_generic_name.alias('product_group')), on='ndc')

def calculate_unit_price_change(data: pl.LazyFrame) -> pl.LazyFrame:
    unit_price_change = (c.unit_price - c.previous_unit_price).alias('unit_price_change')
    new_nadac = (c.unit_price * c.units).round(2).alias('new_nadac')
    old_nadac = (c.previous_unit_price * c.units).round(2).alias('old_nadac')

    return data.with_columns(unit_price_change, new_nadac, old_nadac)

# base query that join nadac and sdud data
def base_query(date_id: int) -> pl.LazyFrame:
    sdud = load_sdud()#.filter(c.state == state)
    nadac = load_nadac().filter(c.date_id == date_id)
    data = sdud.join(nadac, on='ndc')
    # add generic name to the data
    return add_generic_name(data)

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

def avg_old_new_nadac() -> list[pl.Expr]:
    avg_new_nadac = (c.new_nadac / c.units).round(4).alias('avg_new_nadac')
    avg_old_nadac = (c.old_nadac / c.units).round(4).alias('avg_old_nadac')
    return [avg_new_nadac, avg_old_nadac]

def difference_per_rx() -> pl.Expr:
    """Calculate the difference per prescription."""
    return (c.total_diff / c.rx_count).round(2).alias('diff_per_rx')

def fetch_data(data, **kwargs) -> pl.LazyFrame:
    

    # add columns for new nadac, old nadac, and unit price change
    data = data.pipe(calculate_unit_price_change)
    
    # add total difference column
    data = data.with_columns((c.new_nadac - c.old_nadac).alias('total_diff')).with_columns(classification())
    return data
    
def aggregate_data(data: pl.LazyFrame, group_by_col: str) -> pl.LazyFrame:
    data = (
        data
        .group_by(group_by_col)
        .agg(pl.col(['units','rx_count','total_diff', 'new_nadac', 'old_nadac']).sum())
        .with_columns(avg_old_new_nadac())
        .with_columns(difference_per_rx())
        .with_columns(abs_diff_col(), classification(), avg_unit_change(), percent_change())
    )
    return data

if __name__ == "__main__":
    pass
    
    #generate_nadac_table()
    #load_nadac().collect().glimpse()
    #load_medispan().filter(c.gpi_8_base_name == 'Methylphenidate').collect().glimpse()
    # date_filter = '2025-02'
    # state_filter = 'XX'  # Example state filter, replace with actual state code if needed
    #fetch_data(date_filter, 'XX').select(c.total_diff).sum().collect().glimpse()#.sort(c.total_diff).collect().glimpse()
    
