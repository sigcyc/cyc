# Per-df_type enrichment modules, dispatched via the `enrich` key in df_types.yaml.
# Each module exposes enrich(lf: pl.LazyFrame) -> pl.LazyFrame, applied once in Df.load_data.
