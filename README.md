
<h1 align="center">CCXT Download Utility</h1>

<p align="center">
  <a href="https://pypi.org/project/ccxt-download">
    <img src="https://img.shields.io/pypi/v/ccxt-download.svg?color=blue&style=plastic" alt="Latest version" width=95 height=20>
  </a>
  
  <a href="https://github.com/psf/black">
    <img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg">
  </a>
  
</p>


A lightweight package to conventiently and efficiently download 
cryptocurrency data using [CCXT](https://github.com/ccxt/ccxt).

Why is this necessary? Because many times I have found myself wanting
to quickly download a bunch of price data for some quick analysis, only
to spend some time writing a hacky script to download the data with no
thought about storing it for later. Expecially when I need data for 
multiple periods of time and for multiple symbols, things get messy, 
fast. Not to mention the problems that pop up when I want to load that
data later...

With this package, the above issues are no longer a worry. For me anyway.


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

## Installation

```
pip install ccxt-download
```

## Notes and future work
- Current implementation is most suited for minutely aggregated data. If
longer aggregation windows were used, the 1-day file partitioning will
need to be revised. An option is to partition dynamically, for example
daily for 1 minutely data, weekly for 30 minutely, and so on.
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
