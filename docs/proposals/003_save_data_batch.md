Goal: write a Claude skill to batch save data across a date range in parallel.

Context:
- `skills/save_data/scripts/save_data_template.py` is the template save script. Docs at `docs/save_data_template.md`.
- Save scripts accept `--date YYYYMMDD --write` and optionally `--data-dir`.
- `cyc.time_util.parse_dates` expands a date range (e.g., `20250101-20250110`) into trading days.

Requirements:
- Write a batch runner script at `skills/save_data/scripts/batch_save.py` that takes a save script path, date range, and process count, then runs the save script for each trading day in parallel.
- Write `skills/save_data/SKILL.md` as a proper Claude Code skill with frontmatter. The skill should invoke the batch runner script. Use `${CLAUDE_SKILL_DIR}` so it works from any project.
- Symlink the skill to `~/.claude/skills/` for global availability.

Example: `/save-data scripts/save_data_template.py 20250101-20250110 --processes 10`

Verification: run the skill with save_data_template targeting `/tmp`, confirm parquet files are created for each trading day.
