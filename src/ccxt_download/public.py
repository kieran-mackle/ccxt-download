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
from ccxt_download import DEFAULT_DOWNLOAD_DIR, CANDLES


logger = logging.getLogger(__name__)


def download(
    exchange: Union[str, ccxt.Exchange],
    data_types: list[str],
    symbols: list[str],
    start_date: Union[datetime, str],
    end_date: Union[datetime, str],
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
    rate_limiter: Optional[AsyncLimiter] = None,
):
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
):
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

    tasks = []
    for datatype in data_types:
        for symbol in symbols:
            method = globals().get(datatype)
            current_dt = start_dt
            while current_dt < end_dt:
                # Download data for this chunk
                coro = method(
                    exchange=exchange,
                    symbol=symbol,
                    start_dt=current_dt,
                    rate_limiter=rate_limiter,
                    download_dir=download_dir,
                )
                tasks.append(coro)

                # Walk forwards a day
                current_dt = current_dt + timedelta(days=1)

    await asyncio.gather(*tasks)

    # Close exchange connection
    await exchange.close()


async def candles(
    exchange: ccxt.Exchange,
    symbol: str,
    start_dt: datetime,
    rate_limiter: AsyncLimiter,
    timeframe: Optional[str] = "1m",
    download_dir: str = DEFAULT_DOWNLOAD_DIR,
) -> pd.DataFrame:
    filename = filename_builder(
        exchange=exchange.name,
        start_dt=start_dt,
        download_dir=download_dir,
        symbol=symbol,
        data_type=CANDLES,
        data_type_id=timeframe,
    )

    if os.path.exists(filename):
        # Data already downloaded, skip
        logger.info(
            f"{timeframe} candles for {symbol} starting {start_dt} already exist."
        )
        return

    logger.debug(f"Fetching {timeframe} candles for {symbol} starting {start_dt}.")

    # Fetch OHLCV data
    timeframe_map = {"1m": 60e3}
    start_ts = int(start_dt.timestamp() * 1000)
    timeframe_ms = timeframe_map[timeframe]
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

    # Convert the timestamp to a readable format
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")

    # Set the Timestamp column as the DataFrame's index
    df.set_index("Timestamp", inplace=True)

    # Save
    df.to_csv(path_or_buf=filename, compression="gzip")
