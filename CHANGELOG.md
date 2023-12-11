## v0.3.0 (2023-12-11)

### Feat

- implement incomplete data management
- write data to parquet files instead of csv
- **utilities**: added get_tickers utility
- **candles**: added more timeframe keys
- **download**: allow passing options for each datatype download
- added download of funding rate history

### Fix

- **trades**: timestamp key error
- **download_async**: fix kwargs input"
- **public.py**: remove debug print
- **utilities.py**: dropping of duplicate values

## v0.2.0 (2023-10-09)

### Feat

- **utilities.py**: added flatten ohlcv utility

### Fix

- **public.py**: detect when data is out of range
- **public**: trades filename prefix

## v0.1.0 (2023-10-04)

### Feat

- **public.py**: added support for public trade data
- **candles**: add verbosity control
- **get_symbols**: added helper to get market type symbols
- **unformat_str**: implemented utility function
- **candles**: include data metadata in file (exchange and symbol)
- **load_data**: improved argument filtering
- **download**: optionally provide custom rate limiter
- optionally specify start and end dates as str
- created basic ccxt download package

### Fix

- **public.py**: remove debug print

### Refactor

- rename start/end_dt args to start/end_date for disambiguation
