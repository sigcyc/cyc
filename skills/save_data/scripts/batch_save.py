import typer
import subprocess
from multiprocessing import Pool
from pathlib import Path
from cyc.config import get_calendar
from cyc.util_time import parse_dates


def _run_one(args: tuple[str, str, list[str]]) -> tuple[str, subprocess.CompletedProcess]:
    script, date, extra = args
    cmd = ["python", script, "--date", date, "--write"] + extra
    return date, subprocess.run(cmd, capture_output=True, text=True)


def main(
    script: str,
    date: str,
    processes: int = 18,
    data_dir: str | None = None,
):
    df_type = Path(script).stem.removeprefix("save_")
    dates = parse_dates(date, get_calendar(df_type))
    print(f"{len(dates)} dates ({df_type}), {processes} processes", flush=True)

    extra = ["--data-dir", data_dir] if data_dir else []
    tasks = [(script, d, extra) for d in dates]

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
