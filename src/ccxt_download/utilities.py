import os
import glob
import pandas as pd
from typing import Optional, Union
from datetime import datetime, timedelta
from ccxt_download import DEFAULT_DOWNLOAD_DIR


def format_str(s: str):
    conversions = {"/": "%2F", ":": "%3A"}
    for c, sub in conversions.items():
        s = s.replace(c, sub)
    return s


def filename_builder(
    exchange: str,
    start_dt: Union[datetime, str],
    download_dir: str,
    symbol: str,
    data_type: str,
    data_type_id: Optional[str] = None,
):
    if isinstance(start_dt, datetime):
        start_str = start_dt.strftime("%Y-%m-%d")
    else:
        start_str = start_dt
    dtid = f"{data_type_id}_" if data_type_id else ""
    filename = os.path.join(
        download_dir,
        format_str(f"{exchange.lower()}_{dtid}{data_type}_{start_str}_{symbol}.csv.gz"),
    )
    return filename


def generate_date_range(
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
):
    date_range = []
    current_dt = start_dt
    while current_dt < end_dt:
        date_range.append(current_dt.strftime("%Y-%m-%d"))
        current_dt += timedelta(days=1)
    return date_range


def load_data(
    exchange: str,
    data_type: str,
    symbols: Optional[list[str]] = None,
    start_date: Optional[Union[datetime, str]] = None,
    end_date: Optional[Union[datetime, str]] = None,
    download_dir: Optional[str] = DEFAULT_DOWNLOAD_DIR,
    **kwargs,
):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # First get all files mathing the exchange and data type
    filename = filename_builder(
        exchange=exchange,
        start_dt="*",
        download_dir=download_dir,
        symbol="*",
        data_type=data_type,
        **kwargs,
    )
    files = glob.glob(filename)

    # Now filter based on symbols
    symbol_filtered_files = []
    formatted_symbols = [format_str(symbol) for symbol in symbols]
    for f in files:
        for symbol in formatted_symbols:
            if symbol in f:
                symbol_filtered_files.append(f)
                continue

    # Finally filter based on date range
    if start_date is not None or end_date is not None:
        date_range = generate_date_range(start_dt=start_date, end_dt=end_date)
        date_filtered_files = []
        for f in symbol_filtered_files:
            for dt in date_range:
                if dt in f:
                    date_filtered_files.append(f)
                    continue
    else:
        # No dates provided, take all
        date_filtered_files = symbol_filtered_files

    # Now load all data
    df = pd.DataFrame()
    for f in date_filtered_files:
        df = pd.concat([df, pd.read_csv(f, index_col=0, parse_dates=True)])

    # Clean
    df.sort_index(inplace=True)
    df.drop_duplicates(inplace=True)

    return df
