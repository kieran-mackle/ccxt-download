import os
from typing import Literal


CANDLES = "candles"
TRADES = "trades"
FUNDING = "funding"
DEFAULT_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), ".ccxt_data")
STR_CONVERSIONS = {"/": "%2F", ":": "%3A"}

DATATYPES = Literal["candles", "trades", "funding"]

# Exchanges from CCXT v4.1.78
CCXT_EXCHANGES = Literal[
    "alpaca",
    "ascendex",
    "bequant",
    "binance",
    "binancecoinm",
    "binanceus",
    "binanceusdm",
    "bingx",
    "bitcoincom",
    "bitfinex",
    "bitfinex2",
    "bitget",
    "bitmart",
    "bitmex",
    "bitopro",
    "bitpanda",
    "bitrue",
    "bitstamp",
    "bittrex",
    "bitvavo",
    "blockchaincom",
    "bybit",
    "cex",
    "coinbase",
    "coinbaseprime",
    "coinbasepro",
    "coinex",
    "cryptocom",
    "currencycom",
    "deribit",
    "exmo",
    "gate",
    "gateio",
    "gemini",
    "hitbtc",
    "hollaex",
    "htx",
    "huobi",
    "huobijp",
    "idex",
    "independentreserve",
    "kraken",
    "krakenfutures",
    "kucoin",
    "kucoinfutures",
    "luno",
    "mexc",
    "ndax",
    "okcoin",
    "okx",
    "phemex",
    "poloniex",
    "poloniexfutures",
    "probit",
    "upbit",
    "wazirx",
    "whitebit",
    "woo",
]
