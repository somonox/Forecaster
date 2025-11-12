"""Sector forecaster combining market data and news sentiment.

This script analyses sector leaders' market capitalisation trends and combines them with
news sentiment derived from the ``news_dumps.json`` file to rank sectors by outlook.

Usage example::

    python sector_forecaster.py --start 2023-01-31 --end 2025-06-30 \
        --news-path "i need news/news_dumps.json" --output-dir outputs

The script fetches historical price data from Yahoo Finance via :mod:`yfinance`.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:  # pragma: no cover - import guarded for user guidance
    import pandas as pd
except ImportError as exc:  # pragma: no cover - pandas is required at runtime
    raise SystemExit(
        "pandas is required to run this script. Install it with `pip install pandas`."
    ) from exc


try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover - yfinance is required at runtime
    raise SystemExit(
        "yfinance is required to run this script. Install it with `pip install yfinance`."
    ) from exc


@dataclass(frozen=True)
class Leader:
    ticker: str
    name: str
    aliases: Tuple[str, ...] = ()

    def keywords(self) -> Tuple[str, ...]:
        base_keywords = [self.ticker.lower(), self.name.lower()]
        base_keywords.extend(alias.lower() for alias in self.aliases)
        return tuple(base_keywords)


DEFAULT_LEADERS: Mapping[str, Tuple[Leader, ...]] = {
    "Information Technology": (
        Leader("AAPL", "Apple", aliases=("iphone", "macbook")),
        Leader("MSFT", "Microsoft", aliases=("azure", "windows")),
        Leader("NVDA", "Nvidia", aliases=("geforce", "cuda")),
    ),
    "Communication Services": (
        Leader("GOOGL", "Alphabet", aliases=("google", "youtube")),
        Leader("META", "Meta Platforms", aliases=("facebook", "instagram")),
        Leader("NFLX", "Netflix"),
    ),
    "Consumer Discretionary": (
        Leader("AMZN", "Amazon", aliases=("aws", "prime")),
        Leader("TSLA", "Tesla"),
        Leader("HD", "Home Depot"),
    ),
    "Financials": (
        Leader("JPM", "JPMorgan Chase", aliases=("j.p. morgan", "jp morgan")),
        Leader("BAC", "Bank of America"),
        Leader("V", "Visa"),
    ),
    "Health Care": (
        Leader("UNH", "UnitedHealth", aliases=("optum",)),
        Leader("JNJ", "Johnson & Johnson", aliases=("janssen",)),
        Leader("PFE", "Pfizer"),
    ),
    "Industrials": (
        Leader("CAT", "Caterpillar"),
        Leader("HON", "Honeywell"),
        Leader("BA", "Boeing"),
    ),
    "Energy": (
        Leader("XOM", "Exxon Mobil", aliases=("exxon", "mobil")),
        Leader("CVX", "Chevron"),
        Leader("SLB", "Schlumberger", aliases=("slb")),
    ),
    "Consumer Staples": (
        Leader("PG", "Procter & Gamble", aliases=("p&g", "tide")),
        Leader("KO", "Coca-Cola", aliases=("coke",)),
        Leader("PEP", "PepsiCo", aliases=("pepsi",)),
    ),
    "Utilities": (
        Leader("NEE", "NextEra Energy"),
        Leader("DUK", "Duke Energy"),
        Leader("SO", "Southern Company", aliases=("southern co",)),
    ),
    "Real Estate": (
        Leader("PLD", "Prologis"),
        Leader("AMT", "American Tower"),
        Leader("EQIX", "Equinix"),
    ),
    "Materials": (
        Leader("LIN", "Linde"),
        Leader("SHW", "Sherwin-Williams", aliases=("sherwin williams",)),
        Leader("NEM", "Newmont"),
    ),
}


POSITIVE_WORDS: Tuple[str, ...] = (
    "growth",
    "gain",
    "gains",
    "improve",
    "improved",
    "improving",
    "surge",
    "surged",
    "surging",
    "strong",
    "bullish",
    "optimistic",
    "upbeat",
    "record",
    "beat",
    "beats",
    "beating",
    "exceed",
    "exceeds",
    "expansion",
    "expand",
    "expands",
    "expanding",
    "profit",
    "profitable",
    "profits",
    "advances",
    "advance",
    "advanced",
    "resilient",
    "tailwind",
    "outperform",
    "outperformance",
    "lead",
    "leading",
    "positive",
    "constructive",
    "encourage",
    "encouraging",
    "solid",
)

NEGATIVE_WORDS: Tuple[str, ...] = (
    "loss",
    "losses",
    "decline",
    "declines",
    "declining",
    "drop",
    "drops",
    "dropped",
    "drag",
    "headwind",
    "bearish",
    "weak",
    "weaker",
    "weakness",
    "concern",
    "concerns",
    "risk",
    "risks",
    "volatile",
    "volatility",
    "selloff",
    "sell-off",
    "fear",
    "fears",
    "slowdown",
    "slowing",
    "recession",
    "warning",
    "warns",
    "warned",
    "pressure",
    "pressures",
    "problem",
    "problems",
    "challenge",
    "challenges",
    "uncertain",
    "uncertainty",
    "negative",
)


def _normalise_range(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    min_val = series.min()
    max_val = series.max()
    if math.isclose(min_val, max_val):
        return pd.Series([1.0] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)


def _clean_json_payload(raw_text: str) -> str:
    """Return a JSON string that can be parsed into a Python object.

    The ``news_dumps.json`` file in this repository sometimes contains concatenated JSON
    arrays (``][``). This helper replaces such boundaries with commas so that the
    resulting text is a valid JSON array.
    """

    stripped = raw_text.strip()
    if not stripped:
        return "[]"
    cleaned = re.sub(r"]\s*\[", ",", stripped)
    return cleaned


def load_news_articles(news_path: Path) -> List[dict]:
    text = news_path.read_text(encoding="utf-8")
    cleaned = _clean_json_payload(text)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse news data from {news_path}: {exc}") from exc
    if not isinstance(data, list):
        raise TypeError(f"Expected a list of articles in {news_path}, got {type(data)!r}")
    return data


def _tokenise(text: str) -> List[str]:
    return re.findall(r"[A-Za-z']+", text.lower())


def compute_sentiment_score(text: str) -> float:
    tokens = _tokenise(text)
    if not tokens:
        return 0.0
    positives = sum(token in POSITIVE_WORDS for token in tokens)
    negatives = sum(token in NEGATIVE_WORDS for token in tokens)
    return (positives - negatives) / len(tokens)


def parse_seendate(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


class SectorForecaster:
    def __init__(
        self,
        sector_leaders: Mapping[str, Sequence[Leader]],
        start: str,
        end: str,
        news_path: Path,
    ) -> None:
        self.sector_leaders = sector_leaders
        self.start = pd.Timestamp(start)
        self.end = pd.Timestamp(end)
        if self.end <= self.start:
            raise ValueError("End date must be after start date.")
        self.news_path = news_path
        self._history_end = self.end + pd.Timedelta(days=1)

    # ------------------------------------------------------------------
    # News processing
    # ------------------------------------------------------------------
    def analyse_news(self) -> pd.DataFrame:
        articles = load_news_articles(self.news_path)
        if not articles:
            return pd.DataFrame(columns=["sector", "sentiment", "article_count"])

        aggregates: MutableMapping[str, Dict[str, float]] = defaultdict(lambda: {
            "weighted_sentiment": 0.0,
            "weight_sum": 0.0,
            "article_count": 0,
        })

        for article in articles:
            title = article.get("title") or ""
            body = article.get("clean_text") or ""
            content = f"{title}\n{body}"
            sentiment = compute_sentiment_score(content)
            article_sectors = self._detect_sectors(content)
            if not article_sectors:
                continue

            published = parse_seendate(article.get("seendate"))
            if published is None:
                recency_weight = 1.0
            else:
                distance = (self.end - published).days
                # Give recent articles more weight using exponential decay (half-life 180 days)
                recency_weight = math.exp(-max(distance, 0) / 180.0)
                # Do not fully discount older articles
                recency_weight = max(recency_weight, 0.1)

            for sector in article_sectors:
                aggregates[sector]["weighted_sentiment"] += sentiment * recency_weight
                aggregates[sector]["weight_sum"] += recency_weight
                aggregates[sector]["article_count"] += 1

        records = []
        for sector, stats in aggregates.items():
            weight_sum = stats["weight_sum"] or 1.0
            records.append(
                {
                    "sector": sector,
                    "sentiment": stats["weighted_sentiment"] / weight_sum,
                    "article_count": int(stats["article_count"]),
                }
            )

        sentiment_df = pd.DataFrame(records)
        if not sentiment_df.empty:
            sentiment_df.sort_values("sentiment", ascending=False, inplace=True)
            sentiment_df.reset_index(drop=True, inplace=True)
        return sentiment_df

    def _detect_sectors(self, content: str) -> List[str]:
        content_lower = content.lower()
        matched_sectors = []
        for sector, leaders in self.sector_leaders.items():
            for leader in leaders:
                if any(keyword in content_lower for keyword in leader.keywords()):
                    matched_sectors.append(sector)
                    break
        return matched_sectors

    # ------------------------------------------------------------------
    # Market data processing
    # ------------------------------------------------------------------
    def fetch_market_trends(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        sector_frames: List[pd.DataFrame] = []
        summary_records: List[dict] = []

        for sector, leaders in self.sector_leaders.items():
            sector_series: List[pd.Series] = []
            start_caps: List[float] = []
            end_caps: List[float] = []

            for leader in leaders:
                ticker = leader.ticker
                ticker_obj = yf.Ticker(ticker)
                try:
                    history = ticker_obj.history(
                        start=str(self.start.date()),
                        end=str(self._history_end.date()),
                        auto_adjust=False,
                    )
                except Exception as exc:  # pragma: no cover - network failure
                    print(f"Warning: failed to download history for {ticker}: {exc}")
                    continue
                if history.empty:
                    print(f"Warning: no price history returned for {ticker}.")
                    continue

                close_prices = history["Close"].rename(ticker)
                shares = self._infer_share_count(ticker_obj, close_prices)
                market_caps = close_prices * shares
                sector_series.append(market_caps)
                start_caps.append(float(market_caps.iloc[0]))
                end_caps.append(float(market_caps.iloc[-1]))

            if not sector_series:
                continue

            combined = pd.concat(sector_series, axis=1).sort_index()
            combined = combined.ffill().bfill()
            combined["sector_market_cap"] = combined.sum(axis=1)
            combined["sector"] = sector
            sector_frames.append(combined[["sector_market_cap", "sector"]])

            sector_start = sum(start_caps)
            sector_end = sum(end_caps)
            growth_rate = (sector_end / sector_start) - 1 if sector_start else 0.0
            summary_records.append(
                {
                    "sector": sector,
                    "start_market_cap": sector_start,
                    "end_market_cap": sector_end,
                    "growth_rate": growth_rate,
                }
            )

        if not sector_frames:
            raise RuntimeError("Failed to download market data for all sectors.")

        market_history = pd.concat(sector_frames)
        market_history.reset_index(inplace=True)
        market_history.rename(columns={"index": "date"}, inplace=True)
        market_history["date"] = pd.to_datetime(market_history["date"])
        market_history.sort_values(["date", "sector"], inplace=True)
        market_history.reset_index(drop=True, inplace=True)

        summary_df = pd.DataFrame(summary_records)
        summary_df.sort_values("growth_rate", ascending=False, inplace=True)
        summary_df.reset_index(drop=True, inplace=True)
        return market_history, summary_df

    def _infer_share_count(self, ticker_obj: "yf.Ticker", close_prices: pd.Series) -> float:
        fast_info = getattr(ticker_obj, "fast_info", {}) or {}
        market_cap = fast_info.get("market_cap")
        last_price = fast_info.get("last_price") or fast_info.get("previous_close")
        if market_cap and last_price and last_price != 0:
            return float(market_cap) / float(last_price)

        info = {}
        try:
            info = ticker_obj.info or {}
        except Exception:  # pragma: no cover - network related failure fallback
            info = {}

        shares_outstanding = info.get("sharesOutstanding")
        if shares_outstanding:
            return float(shares_outstanding)

        market_cap_info = info.get("marketCap")
        if market_cap_info and not close_prices.empty:
            return float(market_cap_info) / float(close_prices.iloc[-1])

        # As a last resort fall back to scaling the series so that the first value is 1.
        first_price = float(close_prices.iloc[0]) if not close_prices.empty else 1.0
        return 1_000_000_000.0 / max(first_price, 1e-6)

    # ------------------------------------------------------------------
    # Ranking
    # ------------------------------------------------------------------
    def rank_sectors(self, sentiment_df: pd.DataFrame, growth_df: pd.DataFrame) -> pd.DataFrame:
        combined = growth_df.merge(sentiment_df, on="sector", how="left")
        combined["sentiment"].fillna(0.0, inplace=True)
        combined["article_count"].fillna(0, inplace=True)

        combined["growth_score"] = _normalise_range(combined["growth_rate"])
        combined["sentiment_score"] = _normalise_range(combined["sentiment"])
        combined["composite_score"] = 0.7 * combined["growth_score"] + 0.3 * combined["sentiment_score"]
        combined.sort_values("composite_score", ascending=False, inplace=True)
        combined.reset_index(drop=True, inplace=True)
        return combined


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forecast the most promising sector using price and news data.")
    parser.add_argument(
        "--news-path",
        type=Path,
        default=Path("i need news") / "news_dumps.json",
        help="Path to the news_dumps.json file.",
    )
    parser.add_argument("--start", type=str, default="2023-01-31", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", type=str, default="2025-06-30", help="End date (YYYY-MM-DD).")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory to write CSV exports (market history, sentiment, rankings).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    forecaster = SectorForecaster(DEFAULT_LEADERS, start=args.start, end=args.end, news_path=args.news_path)

    print("Loading and analysing news sentiment...")
    sentiment_df = forecaster.analyse_news()
    if sentiment_df.empty:
        print("No sector-linked news sentiment could be derived.")
    else:
        print("Top sentiment by sector:")
        print(sentiment_df.head(10).to_string(index=False))

    print("\nFetching market data for sector leaders...")
    market_history, growth_df = forecaster.fetch_market_trends()
    print("Market capitalisation growth by sector:")
    print(growth_df.to_string(index=False, formatters={"growth_rate": "{:.2%}".format}))

    print("\nCombining signals to rank sectors...")
    ranking_df = forecaster.rank_sectors(sentiment_df, growth_df)
    print(ranking_df.to_string(index=False, formatters={
        "growth_rate": "{:.2%}".format,
        "growth_score": "{:.2f}".format,
        "sentiment": "{:.4f}".format,
        "sentiment_score": "{:.2f}".format,
        "composite_score": "{:.2f}".format,
    }))

    best_sector = ranking_df.iloc[0]["sector"] if not ranking_df.empty else "Unknown"
    print(f"\nMost promising sector (highest composite score): {best_sector}")

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        market_history.to_csv(args.output_dir / "market_history.csv", index=False)
        sentiment_df.to_csv(args.output_dir / "news_sentiment.csv", index=False)
        ranking_df.to_csv(args.output_dir / "sector_rankings.csv", index=False)
        print(f"\nResults saved to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
