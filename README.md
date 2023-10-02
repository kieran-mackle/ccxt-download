# CCXT Download Utility

A lightweight package to conventiently and efficiently download 
cryptocurrency data using [CCXT](https://github.com/ccxt/ccxt).

Why is this necessary? Because many times I have found myself wanting
to quickly download a bunch of price data for some quick analysis, only
to spend some time writing a hacky script to download the data with no
thought about storing it for later. Expecially when I need data for 
multiple periods of time and for multiple symbols, things get messy, 
fast. Not to mention the problems that pop up when I want to load that
data later...

With this package, the above issues are no longer a worry.


## Usage

```python
from ccxt_download import public, CANDLES

# Download candles
public.download(
    exchange="bybit",
    data_types=[CANDLES],
    symbols=[
        "ETH/USDT:USDT",
        "BTC/USDT:USDT",
    ],
    start_date="2023-09-01",
    end_date="2023-09-05",
)
```

Data will be downloaded to file between the dates specified. If the 
data already exists, it will not be re-downloaded.

## Notes
- Current implementation is most suited for minutely aggregated data. If
longer aggregation windows were used, the 1-day file partitioning will
need to be revised. An option is to partition dynamically, for example
daily for 1 minutely data, weekly for 30 minutely, and so on.

### To Do
- [ ] Add license to repo
- [ ] Progress verbosity
- [ ] Docstrings
- [ ] Expand contributing section
- [ ] Test commitizen version bumping and changelog setup
- [ ] Support trade data downloads


## Contributing

```
pip install -e .[dev]
pre-commit install
```
