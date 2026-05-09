import typer
import subprocess
from multiprocessing import Pool
from cyc.time_util import parse_dates


def _run_one(args: tuple[str, str, list[str]]) -> subprocess.CompletedProcess:
    script, date, extra = args
    cmd = ["python", script, "--date", date, "--write"] + extra
    return subprocess.run(cmd, capture_output=True, text=True)


def main(
    script: str,
    date: str,
    processes: int = 40,
    data_dir: str | None = None,
):
    dates = parse_dates(date)
    print(f"{len(dates)} dates, {processes} processes")

    extra = ["--data-dir", data_dir] if data_dir else []
    tasks = [(script, d, extra) for d in dates]

    with Pool(processes) as pool:
        results = pool.map(_run_one, tasks)

    failed = [r for r in results if r.returncode != 0]
    for r in failed:
        print(f"FAILED: {r.args}\n{r.stderr}")
    print(f"{len(results) - len(failed)}/{len(results)} succeeded")


if __name__ == "__main__":
    typer.run(main)
