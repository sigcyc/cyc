# Save Data Template

Template script for saving a polars DataFrame as partitioned parquet files.

## Usage

```bash
python examples/save_toy_data.py [OPTIONS]
```

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--date` | str | `20260326` | Date partition in YYYYMMDD format |
| `--data-dir` | str | `None` | Override base directory (default: `data_dir` from `df_types.yaml`) |
| `--write` | bool | `False` | Actually write the file. Without this flag, variables are exposed as globals for interactive debugging |

## Output path

```
{data_dir}/{name}/{date}.parquet
```

`name` is derived from the script filename: `save_{name}.py`. For `save_toy_data.py`, this is `toy_data`.

## Creating a new save script

1. Copy `examples/save_toy_data.py` to `examples/save_{name}.py`
2. Replace the DataFrame construction with your data
3. Run with `--write` to save
