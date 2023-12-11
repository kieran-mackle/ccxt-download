import os
import pytz
import ccxt
import glob
import logging
import pandas as pd
from typing import Optional, Union
from datetime import datetime, timedelta
from ccxt_download import DEFAULT_DOWNLOAD_DIR, STR_CONVERSIONS


STRFMT = "%Y-%m-%d"
logger = logging.getLogger(__name__)


def format_str(s: str):
    """Format a string so that it can be used as a filename."""
    for c, sub in STR_CONVERSIONS.items():
        s = s.replace(c, sub)
    return s


def unformat_str(s: str):
    """The inverse function of format_str."""
    reversed_map = {v: k for k, v in STR_CONVERSIONS.items()}
    for c, sub in reversed_map.items():
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
    """Construct a filename based on the arguments provided."""
    if isinstance(start_dt, str):
        # Convert to datetime object
        start_dt = datetime.strptime(start_dt, STRFMT)
        start_dt = pytz.utc.localize(start_dt)

    # Get start date as string
    start_str = start_dt.strftime(STRFMT)

    # Set data ID
    dtid = f"{data_type_id}_" if data_type_id else ""

    # Check if the start_dt is today
    inc = "_incomplete" if start_str == datetime.utcnow().strftime(STRFMT) else ""

    # Construct filename and path
    filename = os.path.join(
        download_dir,
        format_str(
            f"{exchange.lower()}_{dtid}{data_type}_{start_str}_{symbol}{inc}.parquet"
        ),
    )

    return filename


def generate_date_range(
    start_dt: datetime,
    end_dt: datetime,
):
    """Generate a range of dates, returned as a list of strings."""
    date_range = []
    current_dt = start_dt
    while current_dt < end_dt:
        date_range.append(current_dt.strftime(STRFMT))
        current_dt += timedelta(days=1)
    return date_range


def load_data(
    exchange: str,
    data_type: str,
    symbols: Optional[list[str]] = None,
    start_date: Optional[Union[datetime, str]] = None,
    end_date: Optional[Union[datetime, str]] = None,
    download_dir: Optional[str] = DEFAULT_DOWNLOAD_DIR,
    include_incomplete: Optional[bool] = False,
    **kwargs,
):
    """Load data from the download directory.

    Parameters
    -----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    data_type : str
        The type of data to load.

    symbols : list[str]
        The symbols to load data for.

    start_date : str | datetime
        The start date of the data to load, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    end_date : str | datetime
        The end date of the data to load, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    include_incomplete : bool, optional
        If `True`, incomplete data will be loaded as well. A message
        will be logged to indicate which incomplete data sets were
        loaded. Note that this refers to incomplete days of data other
        than today, which will always be included. The default is False.
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, STRFMT)

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, STRFMT)

    def filter(unfiltered_files: list[str], match_strs: list[str]):
        filtered_files = []
        for filepath in unfiltered_files:
            for filter_str in match_strs:
                if filter_str in filepath:
                    filtered_files.append(filepath)
                    continue
        return filtered_files

    # Determine filepath building method to use
    if start_date is not None and end_date is not None:
        # Date range requested
        date_range = generate_date_range(start_dt=start_date, end_dt=end_date)
        if symbols is not None:
            # Specific symbols requested too
            files = []
            for date in date_range:
                for symbol in symbols:
                    filename = filename_builder(
                        exchange=exchange,
                        start_dt=date,
                        download_dir=download_dir,
                        symbol=symbol,
                        data_type=data_type,
                        **kwargs,
                    )
                    files.append(filename)

        else:
            # No symbol provided, filter only by date
            filename = filename_builder(
                exchange=exchange,
                start_dt="*",
                download_dir=download_dir,
                symbol="*",
                data_type=data_type,
                **kwargs,
            )
            all_files = glob.glob(filename)
            files = filter(unfiltered_files=all_files, match_strs=date_range)

    else:
        # No date range requested
        if symbols is not None:
            # Specific symbols requested
            files = []
            for symbol in symbols:
                filename = filename_builder(
                    exchange=exchange,
                    start_dt="*",
                    download_dir=download_dir,
                    symbol=symbol,
                    data_type=data_type,
                    **kwargs,
                )
                files += glob.glob(filename)

        else:
            # Get all symbols
            filename = filename_builder(
                exchange=exchange,
                start_dt="*",
                download_dir=download_dir,
                symbol="*",
                data_type=data_type,
                **kwargs,
            )
            files = glob.glob(filename)

    # Check for incomplete data
    if include_incomplete:
        _no_incomplete = 0

        # Check for incomplete versions of files
        for i, f in enumerate(files):
            if "incomplete" in f:
                # This dataset is already incomplete
                _no_incomplete += 1

            else:
                # Check for incomplete version
                _incomplete_filename = "_incomplete.parquet".join(f.split(".parquet"))
                if os.path.exists(_incomplete_filename):
                    # There is incomplete data for this date; use it
                    files[i] = _incomplete_filename
                    _no_incomplete += 1
                    logger.debug(
                        f"Loading incomplete dataset from {_incomplete_filename}"
                    )

    # Now load all data
    df = pd.DataFrame()
    for f in files:
        try:
            _df = pd.read_parquet(f)
            _df = _df[~_df.index.duplicated(keep="first")]
            df = pd.concat([df, _df])
        except:
            pass

    # Clean
    df.sort_index(inplace=True)

    return df


def flatten_ohlcv(df: pd.DataFrame, col: Optional[str] = "Close"):
    """Flatten OHLCV data of many symbols by performing a pivot
    operation.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe of OHLCV data, as returned by load_data.

    col : str, optional
        The column name to flatten by. The default is 'Close'.

    Returns
    -------
    pd.DataFrame
    """
    flat_df = df.pivot(columns="symbol", values=col).ffill()
    return flat_df


def get_symbols(exchange: str, market_type: Optional[str] = "swap"):
    """Helper function to get symbols for a specific market type
    on an exchange.

    Example
    -------
    >>> swap_markets = get_symbols(exchange="bybit", market_type="swap")
    """
    exchange = getattr(ccxt, exchange)()
    markets = exchange.load_markets()
    return [
        market["symbol"] for market in markets.values() if market["type"] == market_type
    ]


def get_tickers(
    exchange: str, threshold: Optional[float] = 0.0, market_type: Optional[str] = "swap"
):
    """Returns tickers, sorted by volume (in USDT)."""
    exchange: ccxt.Exchange = getattr(ccxt, exchange)()
    exchange.load_markets()

    # Get symbols
    symbols = [
        market["symbol"]
        for market in exchange.markets.values()
        if market["type"] == market_type
    ]

    # Fetch tickers
    tickers = exchange.fetch_tickers(symbols=symbols)

    # Sort by volume
    volumes = {v["quoteVolume"]: k for k, v in tickers.items()}

    return {
        volumes[v]: tickers[volumes[v]]
        for v in sorted(volumes, reverse=True)
        if v > threshold
    }
