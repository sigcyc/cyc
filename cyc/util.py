
def parse_time_to_ns(raw: str) -> int:
    """
        raw: 9:30 or 9:30:12.5
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("time string cannot be empty")

    parts = raw.split(":")
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(f"Invalid time string '{raw}'")

    sec_part = parts[2] if len(parts) == 3 else "0"
    if "." in sec_part:
        sec_str, frac_str = sec_part.split(".", 1)
    else:
        sec_str, frac_str = sec_part, ""

    try:
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(sec_str) if sec_str else 0
    except ValueError as exc:
        raise ValueError(f"Invalid time string '{raw}'") from exc

    if not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60):
        raise ValueError(f"Invalid time string '{raw}'")

    if frac_str and not frac_str.isdigit():
        raise ValueError(f"Invalid time string '{raw}'")

    nanosecond = int(frac_str[:9].ljust(9, "0")) if frac_str else 0
    total_seconds = hour * 3600 + minute * 60 + second
    return total_seconds * 1_000_000_000 + nanosecond
