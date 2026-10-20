import typer
import subprocess
from multiprocessing import Pool
from pathlib import Path
from cyc.config import get_calendar, get_data_path, get_file_layout, get_script_path
from cyc.util_time import parse_dates


def _resolve(target: str) -> tuple[str, str]:
    """(script, df_type) from a save_<df_type>.py path, or from a df_type's `script:` key."""
    if Path(target).is_file():
        return target, Path(target).stem.removeprefix("save_")
    return str(get_script_path(target)), target


def _run_one(args: tuple[str, str, str]) -> tuple[str, subprocess.CompletedProcess]:
    script, date, data_dir = args
    cmd = ["python", script, "--data-dir", data_dir, "--write"] + (["--date", date] if date else [])
    return date, subprocess.run(cmd, capture_output=True, text=True)


def main(
    target: str,
    date: str = typer.Argument(""),
    processes: int = 18,
    data_dir: str | None = None,
):
    script, df_type = _resolve(target)
    # single layouts have no date dimension: one run, without --date
    dates = [""] if get_file_layout(df_type).startswith("single") else parse_dates(date, get_calendar(df_type))
    data_dir = data_dir or str(get_data_path(df_type))
    print(f"{script}: {len(dates)} dates ({df_type}) -> {data_dir}, {processes} processes", flush=True)

    tasks = [(script, d, data_dir) for d in dates]

    results = []
    with Pool(processes) as pool:
        for i, (d, r) in enumerate(pool.imap_unordered(_run_one, tasks), 1):
            status = "OK" if r.returncode == 0 else "FAIL"
            print(f"[{i}/{len(tasks)}] {status} {d}", flush=True)
            results.append(r)

    failed = [r for r in results if r.returncode != 0]
    for r in failed:
        print(f"FAILED: {r.args}\n{r.stderr}")
    print(f"{len(results) - len(failed)}/{len(results)} succeeded")


if __name__ == "__main__":
    typer.run(main)
