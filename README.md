# Forecaster Toolkit

This repository now includes a **sector forecaster** that blends market data with
news sentiment to estimate which U.S. equity sector shows the strongest outlook
between two dates.

## Sector Forecaster

The entry point lives in [`sector_forecaster.py`](./sector_forecaster.py). It
performs three high-level steps:

1. **News parsing** – loads articles from `i need news/news_dumps.json`, scores
   them with a light-weight sentiment dictionary, and links each article to the
   sectors whose bellwether tickers appear in the text.
2. **Market aggregation** – downloads historical prices for a curated list of
   sector leaders via Yahoo Finance, infers their share counts, and rebuilds a
   market-cap time series for every sector.
3. **Ranking** – normalises the market-cap growth and sentiment signals,
   combines them, and reports the most promising sector along with optional CSV
   exports.

### Installation

```bash
pip install pandas yfinance
```

### Usage

```bash
python sector_forecaster.py \
  --start 2023-01-31 \
  --end 2025-06-30 \
  --news-path "i need news/news_dumps.json" \
  --output-dir outputs
```

The script prints the sentiment table, sector growth ranking, and the final
composite ranking. When `--output-dir` is provided, it also writes three CSV
files: `market_history.csv`, `news_sentiment.csv`, and `sector_rankings.csv`.

### Colab notebook

If you prefer an interactive workflow, open [`sector_forecaster.ipynb`](./sector_forecaster.ipynb)
in Google Colab. The notebook mirrors the script's pipeline with individual
cells for installation, configuration, news analysis, price retrieval, and
ranking so you can tweak parameters or inspect intermediate outputs more
easily.

### Customisation

* Adjust the `DEFAULT_LEADERS` dictionary in `sector_forecaster.py` to change or
  expand the list of bellwether tickers per sector.
* Supply a different news dump with `--news-path`.
* Tweak the weighting logic in `rank_sectors` if you prefer a different balance
  between price action and news sentiment.

## Data Sources

* **News** – The bundled `news_dumps.json` file (potentially multiple JSON
  arrays joined together) is automatically normalised before parsing.
* **Prices** – The script relies on Yahoo Finance via the `yfinance` package.
  Network connectivity is required at runtime.
