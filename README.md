# CCXT Download Utility

A lightweight package to conventiently and efficiently download 
cryptocurrency data using [CCXT](https://github.com/ccxt/ccxt).

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
- [ ] Expand contributing section
- [ ] Test commitizen version bumping and changelog setup
- [ ] Support trade data downloads


## Contributing

```
pip install -e .[dev]
pre-commit install
```
