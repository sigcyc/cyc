"""
Dump 1 minute stock data and save it as parquet
"""

import argparse
import os
from pathlib import Path
from typing import Final

import polars as pl
import requests


POLYGON_URL_TEMPLATE: Final[str] = (
    "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
)


def _normalize_date(date_str: str) -> str:
    """Allow YYYYMMDD or YYYY-MM-DD."""
    cleaned = date_str.strip()
    if len(cleaned) == 8 and cleaned.isdigit():
        return f"{cleaned[:4]}-{cleaned[4:6]}-{cleaned[6:]}"
    return cleaned


def fetch_minute_prices(symbol: str, date_str: str, api_key: str) -> pl.DataFrame:
    if not api_key:
        raise ValueError("Polygon API key is required")

    iso_date = _normalize_date(date_str)
    url = POLYGON_URL_TEMPLATE.format(
        ticker=symbol.upper(), start=iso_date, end=iso_date
    )
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50_000,
        "apiKey": api_key,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("status") != "OK":
        message = (
            payload.get("error") or payload.get("message") or "Polygon request failed"
        )
        raise RuntimeError(f"Polygon API error: {message}")

    results = payload.get("results")
    if not results:
        raise RuntimeError(f"No minute-level data returned for {symbol} on {iso_date}")

    df = pl.DataFrame(results)
    if not {"t", "c"}.issubset(df.columns):
        raise RuntimeError("Polygon response was missing expected 't' or 'c' keys")

    dataset = df.select(
        pl.lit(symbol.upper()).alias("sym"),
        pl.col("t")
        .cast(pl.Datetime("ms"))
        .dt.cast_time_unit("ns")
        .dt.convert_time_zone(time_zone="America/New_York")
        .alias("time"),
        pl.col("c").cast(pl.Float32).alias("price"),
        pl.col("v").cast(pl.Float32).alias("dollar_delta"),
        pl.col("vw").cast(pl.Float32).alias("price_vwap")
    )
    return dataset


def write_parquet(df: pl.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch UBER minute-level stock prices for 2024-12-11, "
            "build a Polars dataset (sym, time, stock_price), and write it to parquet."
        )
    )
    parser.add_argument(
        "--symbol",
        default="UBER",
        help="Ticker symbol to download (default: UBER)",
    )
    parser.add_argument(
        "--date",
        default="20241211",
        help="Trading date as YYYYMMDD or YYYY-MM-DD (default: 20241211)",
    )
    parser.add_argument(
        "--output",
        default=str(Path("data") / "UBER_20241211_minute.parquet"),
        help="Path to the parquet file to write (default: data/UBER_20241211_minute.parquet)",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="Polygon API key. Falls back to POLYGON_API_KEY environment variable.",
    )

    args = parser.parse_args()
    api_key = args.api_key or os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise SystemExit(
            "Polygon API key missing. Provide --api-key or set POLYGON_API_KEY."
        )

    dataset = fetch_minute_prices(args.symbol, args.date, api_key)
    output_path = Path(args.output).expanduser()
    write_parquet(dataset, output_path)
    print(f"Wrote {len(dataset)} rows to {output_path}")


if __name__ == "__main__":
    main()
