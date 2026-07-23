from datetime import date

import polars as pl

# value is filed in $1,000s before the filer's dollars_since period and in whole dollars from it on.
# Default: the 20221231 transition period (SEC electronic-filing modernization). Exceptions are the
# top chronic early dollars-filers (docs/form13f_guide.html §5), covering ~91% of misfiled value;
# the ~1,300 small violators are accepted error. Regenerate: per (cik, period) compare the filer's
# quarter total against its own 20230630 total; ratio < 100 means that quarter was filed in dollars.
FIRST_DOLLARS_PERIOD = date(2022, 12, 31)
DOLLARS_SINCE = {
    861177: date(2017, 3, 31),  # UBS ASSET MANAGEMENT AMERICAS INC
    1166588: date(2013, 6, 30),  # BNP PARIBAS ARBITRAGE, SNC
    719245: date(2013, 6, 30),  # WESTPAC BANKING CORP
    1456670: date(2013, 6, 30),  # DNB Asset Management AS
    1132716: date(2013, 6, 30),  # UBS OCONNOR LLC
    1323645: date(2020, 6, 30),  # CAPITAL FUND MANAGEMENT S.A.
    1544204: date(2020, 12, 31),  # Liontrust Investment Partners LLP
    1539994: date(2013, 6, 30),  # AEGON ASSET MANAGEMENT UK Plc
    1278249: date(2013, 6, 30),  # TOWER BRIDGE ADVISORS
    1329948: date(2013, 6, 30),  # Janney Montgomery Scott LLC
    1876496: date(2021, 6, 30),  # Border to Coast Pensions Partnership Ltd
    1050743: date(2013, 6, 30),  # PEAPACK GLADSTONE FINANCIAL CORP
    1171592: date(2013, 9, 30),  # HAHN CAPITAL MANAGEMENT LLC
}


def enrich(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Scale `value` to whole dollars. Applied once in Df.load_data; Df.enrich() does not re-run it."""
    dollars_since = pl.col("cik").cast(pl.Int64).replace_strict(
        DOLLARS_SINCE, default=FIRST_DOLLARS_PERIOD, return_dtype=pl.Date
    )
    return lf.with_columns(
        pl.when(pl.col("date") < dollars_since).then(pl.col("value") * 1000).otherwise(pl.col("value"))
    )
