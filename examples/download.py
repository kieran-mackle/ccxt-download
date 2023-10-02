import logging
from datetime import datetime
from ccxt_download import public, CANDLES


# Add handler to ccxt_download logger
logger = logging.getLogger("ccxt_download")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("test.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# Download candles
public.download(
    exchange="bybit",
    data_types=[CANDLES],
    symbols=[
        "SOL/USDT:USDT",
        # "MATIC/USDT:USDT",
        # "DOT/USDT:USDT",
        # "ETH/USDT:USDT",
        # "BTC/USDT:USDT",
        # "TRB/USDT:USDT",
    ],
    start_dt=datetime(2023, 9, 1),
    end_dt=datetime(2023, 9, 2),
)
