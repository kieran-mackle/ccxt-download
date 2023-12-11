
<h1 align="center">CCXT Download Utility</h1>

<p align="center">
  <a href="https://pypi.org/project/ccxt-download">
    <img src="https://img.shields.io/pypi/v/ccxt-download.svg?color=blue&style=plastic" alt="Latest version" width=95 height=20>
  </a>
  
  <a href="https://github.com/psf/black">
    <img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg">
  </a>
  
</p>


A lightweight wrapper to conventiently and efficiently download 
cryptocurrency data using [CCXT](https://github.com/ccxt/ccxt).

## Description

Why is this necessary? Many times I have found myself needing some data,
only to spend some time writing a quick and dirty script to download said 
data with no thought about storing it for later. Add multiple symbols across
different time periods to the mix, and things just get worse. Then when I 
want to load that data later, its so badly organised (or not at all) that it
is easier to write another quick and dirty script and repeat the cycle. With 
this package, the above issues are no longer issues. For me anyway.

What makes this useful? The following features:
- asynchronous downloading (download data in parallel)
- intelligent file management (won't re-download data if it already exists)
  - if data is downloaded for the current day, it will be marked as `incomplete`, 
  which will signal that it should be updated in future downloads
- efficient data storage (using [Apache Parquet](https://parquet.apache.org/))
- helpful utilities for loading and processing data


## Usage

Below are some brief examples illustrating the basic functionality of `ccxt-download`. 
For more, see the [examples](examples).


### Downloading data
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

Data will be downloaded between the dates specified. If the 
data already exists, it will not be re-downloaded. The exception to this 
is when a dataset has been marked as `incomplete`, for example when you 
download data for the current day (which has not ended yet). When this is
detected, that incomplete dataset will be updated. If it can be completed,
the `incomplete` marking will be removed.

### Reading downloaded data

```python
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
```


## Installation

```
pip install ccxt-download
```

## Notes and future work
- Support for private downloads to assist in accounting, account tracking
and analysis, etc.


## Contributing

Contributions to `ccxt-download` are welcomed. However, please try follow the
guidelines below.

### Seek early feedback
Please [open an issue](https://github.com/kieran-mackle/ccxt-download/issues)
before a [pull request](https://github.com/kieran-mackle/ccxt-download/pulls)
to discuss any changes you wish to make.

### Code style
The code in this project is formatted using [Black](https://github.com/psf/black). 
The Black package is included in the `dev` dependencies of `ccxt-download`.
As mentioned below, please run `black .` before opening a pull request.


### Setting up for Development
Fork this repository, then install from source using an editable installation. 
Additionally, install with the optional `dev` dependencies.

```
pip install -e .[dev]
```

If you want to make sure you have formatted the code before commiting, install
the pre-commit hook using the command below. This will check that the code
is formatted whenever you try to make a commit.

```
pre-commit install
```

If you get a message saying the code isn't formatted correctly, simply run the 
command below and try again.

```
black .
```
