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
