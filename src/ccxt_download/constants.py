import os


CANDLES = "candles"
TRADES = "trades"
FUNDING = "funding"
DEFAULT_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), ".ccxt_data")
STR_CONVERSIONS = {"/": "%2F", ":": "%3A"}
