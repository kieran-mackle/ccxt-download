import os
import pytz
import ccxt
import glob
import logging
import pandas as pd
from typing import Optional, Union
from datetime import datetime, timedelta
from ccxt_download.constants import DEFAULT_DOWNLOAD_DIR, STR_CONVERSIONS, CANDLES


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
    window_length: Optional[timedelta] = timedelta(days=1),
    data_type_id: Optional[str] = None,
):
    """Construct a filename based on the arguments provided."""
    if isinstance(start_dt, str) and start_dt != "*":
        # Convert to datetime object
        start_dt = datetime.strptime(start_dt, STRFMT)
        start_dt = pytz.utc.localize(start_dt)

    # Set data ID
    dtid = f"{data_type_id}_" if data_type_id else ""

    if start_dt != "*":
        # Get start date as string
        start_str = start_dt.strftime(STRFMT)

        # Check if today is in the time window
        now = pytz.utc.localize(datetime.utcnow())
        inc = "_incomplete" if start_dt < now < start_dt + window_length else ""

    else:
        # Wildcard date
        start_str = start_dt
        inc = ""

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
    data_type: str,
    **kwargs,
):
    """Generate a range of dates, returned as a list of strings."""
    td = timedelta_from_str(kwargs.get("data_type_id", "1m"))
    if data_type in [CANDLES]:
        # Adjust timestep
        timestep = _timestep_from_timedelta(td)
    else:
        timestep = timedelta(days=1)

    date_range = []
    current_dt = _period_start(td, start_dt)
    while current_dt < end_dt:
        date_range.append(current_dt.strftime(STRFMT))
        current_dt = _period_start(td, current_dt + timestep)
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
    # TODO - test with hourly candle data
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
    td = timedelta_from_str(kwargs.get("data_type_id", "1m"))
    if data_type in [CANDLES]:
        window = _timestep_from_timedelta(td)
    else:
        window = timedelta(days=1)
    if start_date is not None and end_date is not None:
        # Date range requested
        date_range = generate_date_range(
            start_dt=start_date, end_dt=end_date, data_type=data_type, **kwargs
        )
        if symbols is not None:
            # Specific symbols requested too
            files = []
            for date in date_range:
                for symbol in symbols:
                    filename = filename_builder(
                        exchange=exchange,
                        start_dt=date,
                        window_length=window,
                        download_dir=download_dir,
                        symbol=symbol,
                        data_type=data_type,
                        **kwargs,
                    )
                    files.append(filename)

        else:
            # No symbol provided, filter only by date. First get all files
            filename = filename_builder(
                exchange=exchange,
                start_dt="*",
                window_length=window,
                download_dir=download_dir,
                symbol="*",
                data_type=data_type,
                **kwargs,
            )
            all_files = glob.glob(filename)

            # Now filter them by the date range
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
                    window_length=window,
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
                window_length=window,
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

    # Remove duplicates of timestamp-symbol pairs
    multix = df.set_index(["symbol"], append=True)
    df2 = multix[~multix.index.duplicated()].reset_index(level=1)

    return df2


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
    tickers = {k: v for k, v in exchange.fetch_tickers().items() if k in symbols}

    # Sort by volume
    volumes = {v["quoteVolume"]: k for k, v in tickers.items()}

    return {
        volumes[v]: tickers[volumes[v]]
        for v in sorted(volumes, reverse=True)
        if v > threshold
    }


def timedelta_from_str(timeframe: str) -> timedelta:
    """Returns a timedelta object from a timeframe string.

    Parameters
    -----------
    timeframe: str
        The timeframe, specified in the format X[u], where X represents
        the quantity of time, and u represents the units. For example, 1s
        is for 1 second, 4h is for 4 hours. Only integer quantities are
        supported. The unit must be in [s, m, h, d] (seconds, minutes,
        hours, daily).

    Examples
    ---------
    >>> timedelta_from_str("30s")
    datetime.timedelta(seconds=30)
    >>> timedelta_from_str("1h")
    datetime.timedelta(seconds=3600)
    """
    timeframe = timeframe.lower()
    if "s" in timeframe:
        # Seconds
        return timedelta(seconds=int(timeframe.split("s")[0]))
    elif "m" in timeframe:
        # Minutes
        return timedelta(minutes=int(timeframe.split("m")[0]))
    elif "h" in timeframe:
        # Hours
        return timedelta(hours=int(timeframe.split("h")[0]))
    elif "d" in timeframe:
        # Days
        return timedelta(days=int(timeframe.split("d")[0]))
    else:
        raise ValueError(f"Cannot parse timeframe '{timeframe}'.")


def _period_start(td: timedelta, start_dt: datetime):
    """Ensure the date is at the start of its period.

    Parameters
    -----------
    td : timedelta
        The timeframe timedelta.

    start_dt : datetime
        The start datetime object.
    """
    if td >= timedelta(days=1):
        # Yearly period
        adj_start_dt = datetime(year=start_dt.year, month=1, day=1)
        # return pytz.utc.localize(start_dt)
    elif td >= timedelta(hours=1):
        # Monthly period
        adj_start_dt = datetime(year=start_dt.year, month=start_dt.month, day=1)
        # return pytz.utc.localize(start_dt)
    else:
        # Daily period; no adjustment needed
        return start_dt

    # Inherit timezone
    if start_dt.tzinfo is not None and start_dt.tzinfo.utcoffset(start_dt) is not None:
        return pytz.utc.localize(adj_start_dt)
    else:
        return adj_start_dt


def _timestep_from_timedelta(td: timedelta):
    if td >= timedelta(days=1):
        # Use yearly windows
        timestep = timedelta(days=31)

    elif td >= timedelta(hours=1):
        # Use monthly windows
        timestep = timedelta(days=31)

    else:
        # Use daily windows
        timestep = timedelta(days=1)

    return timestep
