# CCXT Download Utility

A lightweight package to efficiently download cryptocurrency data using CCXT.

## Usage

```python
from datetime import datetime
from ccxt_download import public

# Download candles
public.download(
    exchange="bybit",
    data_types=["candles"],
    symbols=[
        "SOL/USDT:USDT",
        "MATIC/USDT:USDT",
        "DOT/USDT:USDT",
        "ETH/USDT:USDT",
        "BTC/USDT:USDT",
        "TRB/USDT:USDT",
    ],
    start_dt=datetime(2023, 9, 1),
    end_dt=datetime(2023, 9, 30),
)
```

Data will be downloaded to file between the dates specified. If the 
data already exists, it will not be re-downloaded.

## Notes
- Current implementation is most suited for minutely aggregated data. If
longer aggregation windows were used, the 1-day file partitioning will
need to be revised. An option is to partition dynamically, for example
daily for 1 minutely data, weekly for 30 minutely, and so on.

## Contributing

```
pip install -e .[dev]
pre-commit install
```
