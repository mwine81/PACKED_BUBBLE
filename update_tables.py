from config import *
import polars as pl
from polars import col as c
import polars.selectors as cs
from helpers import load_nadac, get_oral_solid_dosage_forms_ndcs
import re
from datetime import date

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