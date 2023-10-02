from ccxt_download import CANDLES
from ccxt_download.utilities import load_data


df = load_data(
    exchange="bybit",
    data_type=CANDLES,
    data_type_id="1m",
    symbols=["ETH/USDT:USDT"],
    start_date="2023-09-01",
    end_date="2023-09-04",
)
