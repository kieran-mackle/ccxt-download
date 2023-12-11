import os
import pytz
import asyncio
import logging
import pandas as pd
import ccxt.pro as ccxt
from typing import Optional, Union
from aiolimiter import AsyncLimiter
from datetime import datetime, timedelta
from ccxt_download.utilities import filename_builder
from ccxt_download import DEFAULT_DOWNLOAD_DIR, CANDLES, TRADES, FUNDING


logger = logging.getLogger(__name__)


def download(
    exchange: Union[str, ccxt.Exchange],
    data_types: list[str],
    symbols: list[str],
    start_date: Union[datetime, str],
    end_date: Union[datetime, str],
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    rate_limiter: Optional[AsyncLimiter] = None,
    verbose: Optional[bool] = True,
    options: Optional[dict[str, dict]] = None,
):
    """Download data.

    Parameters
    ----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    data_types : list[str]
        The type of data to download.

    symbols : list[str]
        The symbols to download data for.

    start_date : str | datetime
        The start date of the data to download, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    end_date : str | datetime
        The end date of the data to download, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    rate_limiter : asiolimiter.AsyncLimiter, optional
        An asyncio rate limiter object. The default is
        AsyncLimiter(max_rate=100, time_period=30).

    verbose : bool, optional
        Be verbose. The default is True.

    options : dict[str, dict], optional
        Extra options to pass to the download methods.
    """
    # TODO - should probably check that the end date isn't today, or
    # else a partial file will be written and never filled.

    # Create rate limiter
    if rate_limiter is None:
        rate_limiter = AsyncLimiter(max_rate=100, time_period=30)

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Enforce timezones
    start_date = pytz.utc.localize(start_date)
    end_date = pytz.utc.localize(end_date)

    asyncio.run(
        download_async(
            exchange=exchange,
            data_types=data_types,
            symbols=symbols,
            start_dt=start_date,
            end_dt=end_date,
            rate_limiter=rate_limiter,
            download_dir=download_dir,
            verbose=verbose,
            options=options,
        )
    )


async def download_async(
    exchange: Union[str, ccxt.Exchange],
    data_types: list[str],
    symbols: list[str],
    start_dt: datetime,
    end_dt: datetime,
    rate_limiter: AsyncLimiter,
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    verbose: Optional[bool] = True,
    options: Optional[dict[str, dict]] = None,
):
    """Async data download function.

    Parameters
    ----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    data_types : list[str]
        The type of data to download.

    symbols : list[str]
        The symbols to download data for.

    start_date : str | datetime
        The start date of the data to download, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    end_date : str | datetime
        The end date of the data to download, provided as a
        datetime object or as a string in the form 'YYYY-MM-DD'.

    rate_limiter : asiolimiter.AsyncLimiter
        An asyncio rate limiter object.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    verbose : bool, optional
        Be verbose. The default is True.

    options : dict[str, dict], optional
        Extra options to pass to the download methods.
    """
    # Create exchange instance
    if isinstance(exchange, str):
        exchange = getattr(ccxt, exchange)()
    elif not isinstance(exchange, ccxt.Exchange):
        raise Exception(
            f"Exchange must be of type 'str' or 'ccxt.pro.Exchange',  not {type(exchange)}."
        )
    await exchange.load_markets()

    # Check download directory
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)

    # Check options
    if options is None:
        options = {}

    tasks = []
    for datatype in data_types:
        for symbol in symbols:
            method = globals().get(datatype)
            kwargs = options.get(datatype, {})
            current_dt = start_dt
            while current_dt < end_dt:
                # Download data for this chunk
                coro = method(
                    exchange=exchange,
                    symbol=symbol,
                    start_dt=current_dt,
                    rate_limiter=rate_limiter,
                    download_dir=download_dir,
                    verbose=verbose,
                    **kwargs,
                )
                tasks.append(coro)

                # Walk forwards a day
                current_dt = current_dt + timedelta(days=1)

    await asyncio.gather(*tasks)

    # Close exchange connection
    await exchange.close()


def _check_to_proceed(filename: str):
    proceed = True
    if os.path.exists(filename) and "incomplete" not in filename:
        # Data already downloaded, skip
        proceed = False

    # Check for incomplete dataset on this day
    _incomplete_filename = "_incomplete.parquet".join(filename.split(".parquet"))
    if os.path.exists(_incomplete_filename):
        # Remove incomplete file (to be replaced with complete data now)
        # TODO - could do partial download using incomplete dataset for efficiency
        os.remove(_incomplete_filename)
        logger.debug(f"Removing previously incomplete data: {filename}.")

    return proceed


async def candles(
    exchange: ccxt.Exchange,
    symbol: str,
    start_dt: datetime,
    rate_limiter: AsyncLimiter,
    timeframe: Optional[str] = "1m",
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    verbose: Optional[bool] = True,
) -> pd.DataFrame:
    """Download candle (OHLCV) data.

    Parameters
    ----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    symbol : str
        The symbol to download data for.

    start_dt : datetime
        The start date of the data to download, provided as a
        datetime object.

    end_dt : datetime
        The end date of the data to download, provided as a
        datetime object.

    timeframe : str, optional
        The candlestick aggregation window. The default is 1m.

    rate_limiter : asiolimiter.AsyncLimiter
        An asyncio rate limiter object.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    verbose : bool, optional
        Be verbose. The default is True.
    """
    filename = filename_builder(
        exchange=exchange.name.lower(),
        start_dt=start_dt,
        download_dir=download_dir,
        symbol=symbol,
        data_type=CANDLES,
        data_type_id=timeframe,
    )

    # Check to proceed
    proceed = _check_to_proceed(filename)
    if not proceed:
        # Data already downloaded, skip
        logger.info(
            f"{timeframe} candles for {symbol} on {exchange.name} starting {start_dt} already exist."
        )
        return

    logger.debug(
        f"Fetching {timeframe} candles for on {exchange.name} {symbol} starting {start_dt}."
    )

    # Fetch OHLCV data
    timeframe_map = {
        "4h": 4 * 60 * 60e3,
        "1h": 60 * 60e3,
        "15m": 15 * 60e3,
        "5m": 5 * 60e3,
        "1m": 60e3,
        "1s": 1e3,
    }
    start_ts = int(start_dt.timestamp() * 1000)
    try:
        timeframe_ms = timeframe_map[timeframe]
    except KeyError:
        raise KeyError(
            f"Timeframe key '{timeframe}' not supported. Must be one of the following: {', '.join(timeframe_map.keys())}"
        )
    end_ts = int((start_dt + timedelta(days=1)).timestamp() * 1000)
    ohlcv_data = []
    current_ts = start_ts
    while current_ts < end_ts:
        limit = int((end_ts - current_ts) / timeframe_ms) + 1
        async with rate_limiter:
            data = await exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=current_ts,
                limit=limit,
            )
        if len(data) < 1:
            break
        ohlcv_data += data
        current_ts = data[-1][0] + 1

    # Convert the data into a DataFrame
    columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
    df = pd.DataFrame(ohlcv_data, columns=columns)

    # Check actual date range of data
    df = df.loc[(start_ts <= df["Timestamp"]) & (df["Timestamp"] < end_ts)]
    if len(df) == 0:
        logger.info(
            f"No {timeframe} candles for {symbol} on {exchange} found on {start_dt.strftime('%Y-%m-%d')}."
        )
        return

    # Convert the timestamp to a readable format
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")

    # Set the Timestamp column as the DataFrame's index
    df.set_index("Timestamp", inplace=True)

    # Add meta info
    df["exchange"] = exchange.name.lower()
    df["symbol"] = symbol

    # Save
    df.to_parquet(path=filename)

    if verbose:
        print(
            f"Finished downloading candles for {symbol} on {exchange} on {start_dt.strftime('%Y-%m-%d')}."
        )


async def trades(
    exchange: ccxt.Exchange,
    symbol: str,
    start_dt: datetime,
    rate_limiter: AsyncLimiter,
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    verbose: Optional[bool] = True,
) -> pd.DataFrame:
    """Download trade data.

    Parameters
    ----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    symbol : str
        The symbol to download data for.

    start_dt : datetime
        The start date of the data to download, provided as a
        datetime object.

    end_dt : datetime
        The end date of the data to download, provided as a
        datetime object.

    rate_limiter : asiolimiter.AsyncLimiter
        An asyncio rate limiter object.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    verbose : bool, optional
        Be verbose. The default is True.
    """
    filename = filename_builder(
        exchange=exchange.name.lower(),
        start_dt=start_dt,
        download_dir=download_dir,
        symbol=symbol,
        data_type=TRADES,
    )

    # Check to proceed
    proceed = _check_to_proceed(filename)
    if not proceed:
        # Data already downloaded, skip
        logger.info(
            f"Trade data for {symbol} on {exchange.name} starting {start_dt} already exist."
        )
        return

    logger.debug(
        f"Fetching trade data for {symbol} on {exchange.name} starting {start_dt}."
    )

    # Fetch trades
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int((start_dt + timedelta(days=1)).timestamp() * 1000)
    trade_data = []
    current_ts = start_ts
    while current_ts < end_ts:
        async with rate_limiter:
            data = await exchange.fetch_trades(
                symbol=symbol,
                since=current_ts,
                limit=1000,
            )
        if len(data) < 1:
            break
        trade_data += data
        current_ts = data[-1]["timestamp"] + 1

    # Convert the data into a DataFrame
    df = pd.DataFrame(
        trade_data,
        columns=["timestamp", "symbol", "side", "price", "amount", "cost", "fee"],
    )

    # Check actual date range of data
    df = df.loc[(start_ts <= df["timestamp"]) & (df["timestamp"] < end_ts)]
    if len(df) == 0:
        logger.info(
            f"No trades for {symbol} on {exchange} found on {start_dt.strftime('%Y-%m-%d')}."
        )
        return

    df["Timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("Timestamp", inplace=True)

    # Add meta info and clean up
    df["exchange"] = exchange.name.lower()
    df.drop("timestamp", inplace=True, axis=1)

    # Save
    df.to_parquet(filename)

    if verbose:
        print(
            f"Finished downloading trades for {symbol} on {exchange} on {start_dt.strftime('%Y-%m-%d')}."
        )


async def funding(
    exchange: ccxt.Exchange,
    symbol: str,
    start_dt: datetime,
    rate_limiter: AsyncLimiter,
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    verbose: Optional[bool] = True,
) -> pd.DataFrame:
    """Download funding rate data.

    Parameters
    ----------
    exchange : str | ccxt_pro.Exchange
        The exchange to download data from, provided either by
        name as a string, or directly as a CCXT Pro exchange
        instance.

    symbol : str
        The symbol to download data for.

    start_dt : datetime
        The start date of the data to download, provided as a
        datetime object.

    end_dt : datetime
        The end date of the data to download, provided as a
        datetime object.

    rate_limiter : asiolimiter.AsyncLimiter
        An asyncio rate limiter object.

    download_dir : str, optional
        The path to the download directory. The default is
        '.ccxt_data/' in your user's home directory.

    verbose : bool, optional
        Be verbose. The default is True.
    """
    filename = filename_builder(
        exchange=exchange.name.lower(),
        start_dt=start_dt,
        download_dir=download_dir,
        symbol=symbol,
        data_type=FUNDING,
    )

    # Check to proceed
    proceed = _check_to_proceed(filename)
    if not proceed:
        # Data already downloaded, skip
        logger.info(
            f"Funding rate data for {symbol} on {exchange.name} starting {start_dt} already exist."
        )
        return

    logger.debug(
        f"Fetching funding rate data for {symbol} on {exchange.name} starting {start_dt}."
    )

    # Fetch trades
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int((start_dt + timedelta(days=1)).timestamp() * 1000)
    fr_data = []
    current_ts = start_ts
    while current_ts < end_ts:
        async with rate_limiter:
            data = await exchange.fetch_funding_rate_history(
                symbol=symbol,
                since=current_ts,
                # limit=6,
            )
        if len(data) < 1:
            break
        fr_data += data
        current_ts = data[-1]["timestamp"] + 1

    # Convert the data into a DataFrame
    df = pd.DataFrame(
        fr_data,
        columns=["timestamp", "symbol", "fundingRate"],
    )

    # Check actual date range of data
    df = df.loc[(start_ts <= df["timestamp"]) & (df["timestamp"] < end_ts)]
    df.loc[(start_ts < df["timestamp"])]
    df.loc[(df["timestamp"] < end_ts)]
    if len(df) == 0:
        logger.info(
            f"No funding rate data for {symbol} on {exchange} found on {start_dt.strftime('%Y-%m-%d')}."
        )
        return

    df["Timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("Timestamp", inplace=True)

    # Add meta info and clean up
    df["exchange"] = exchange.name.lower()
    df.drop("timestamp", inplace=True, axis=1)

    # Save
    df.to_parquet(filename)

    if verbose:
        print(
            f"Finished downloading funding rate data for {symbol} on {exchange} on {start_dt.strftime('%Y-%m-%d')}."
        )
