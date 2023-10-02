from datetime import datetime
from ccxt_download import CANDLES
from ccxt_download.utilities import load_data


df = load_data(
    exchange="bybit",
    data_type=CANDLES,
    data_type_id="1m",
    symbols=["ETH/USDT:USDT"],
    start_dt=datetime(2023, 9, 1),
    end_dt=datetime(2023, 9, 4),
)
