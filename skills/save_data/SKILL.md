---
name: save_data
description: Batch save data across a date range using parallel processes. Use when the user asks to save data for a date range.
argument-hint: <script_path> <date_range> [--processes N] [--data-dir path]
allowed-tools: Bash
---

Run `${CLAUDE_SKILL_DIR}/scripts/batch_save.py` with the user's arguments:

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py <script_path> <date_range> --processes <N>
```

- `<script_path>`: Relative path to the save script from the project root (e.g., `scripts/save_data_template.py`)
- `<date_range>`: YYYYMMDD or YYYYMMDD-YYYYMMDD. Expanded to trading days via `cyc.time_util.parse_dates`
- `--processes`: Number of parallel processes (default 10)
- `--data-dir`: Optional override for the base data directory

## Example

User: `/save_data scripts/save_data_template.py 20250101-20250110 --processes 10`

```bash
python ${CLAUDE_SKILL_DIR}/scripts/batch_save.py scripts/save_data_template.py 20250101-20250110 --processes 10
```
