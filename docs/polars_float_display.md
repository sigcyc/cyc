# Polars Float Display

How Polars renders `f64` columns in table reprs. Source: `fmt_float` in
`polars-core/src/fmt.rs`. Verified against polars 1.39.3.

There are **two independent code paths**, and which one runs depends solely on
whether `pl.Config.set_float_precision(...)` is set:

| `float_precision` | path | rule |
|-------------------|------|------|
| set (e.g. `3`) | precision | fixed `{:.N}`; switch to scientific only when the fixed-point string is longer than 19 chars (precision-dependent — with precision=3 that is ≈ `\|v\|` ≥ 1e15). `set_fmt_float` is **ignored** on this path. |
| unset | `mixed` (default) or `full` | `full` = shortest round-trip; `mixed` = the ladder below. |

The `19` everyone remembers belongs to the **precision** path only. `mixed`
never looks at it.


## The `mixed` ladder

One constant: `SCIENTIFIC_BOUND = 999999.0`. Let `s = f"{v}"` be Rust's shortest
repr — whole numbers print with no decimal point (`222049064`, not
`222049064.0`).

```
if v is whole AND |v| < 999999:
    -> {:.1}            # 1234 -> "1234.0",  100000 -> "100000.0",  0 -> "0.0"

elif len(s) > 9:
    if |v| > 999999 or |v| < 1e-6:
        -> {:.4e}       # 352000200.35 -> "3.5200e8"   (4-decimal, lossy)
    else:               # in [1e-6, 999999] but long
        -> {:.6}, strip trailing zeros (keep one)
                        # 0.000012345 -> "0.000012"

else:                   # len(s) <= 9
    if v is whole:
        -> {:e}         # 222049064 -> "2.22049064e8"  (full significant digits)
    else:
        -> s            # 1234.5678 -> "1234.5678"
```

The key, counter-intuitive fact: the mantissa width (full vs 4 decimals) is
decided by **`len(s) > 9`, not by magnitude**.


## Worked examples

| value | `s = f"{v}"` | len | branch | display |
|-------|--------------|-----|--------|---------|
| `1234.0` | `1234` | – | whole, < 999999 | `1234.0` |
| `100000.0` | `100000` | – | whole, < 999999 | `100000.0` |
| `999998.0` | `999998` | – | whole, < 999999 | `999998.0` |
| `999999.0` | `999999` | 6 | whole, ≥ bound → `{:e}` | `9.99999e5` |
| `222049064.0` | `222049064` | 9 | whole, short → `{:e}` | `2.22049064e8` |
| `1234567890.0` | `1234567890` | 10 | len > 9 → `{:.4e}` | `1.2346e9` |
| `1234.5678` | `1234.5678` | 9 | short, fractional → `s` | `1234.5678` |
| `9999999.5` | `9999999.5` | 9 | short, fractional → `s` | `9999999.5` |
| `352000200.35` | `352000200.35` | 12 | len > 9 → `{:.4e}` | `3.5200e8` |
| `1079568301.13` | `1079568301.13` | 13 | len > 9 → `{:.4e}` | `1.0796e9` |
| `0.000012345` | `0.000012345` | 11 | len > 9, in range → `{:.6}` | `0.000012` |


## Consequences

- **Whole-number cliff at 999999.** `999998.0` → `999998.0`, but `999999.0` →
  `9.99999e5`. Whole numbers stay *full-precision* scientific (`{:e}`) until they
  reach 10 digits (≥ 1e9), where `len(s) > 9` drops them to `{:.4e}`:
  `999999999` → `9.99999999e8`, but `1234567890` → `1.2346e9`.

- **Same magnitude, different mantissa.** `222049064` (9-char whole) →
  `2.22049064e8` full, while `352000200.35` (12-char) → `3.5200e8`. Nearly equal
  values, opposite precision — purely because of string length.

- **A thousands separator suppresses scientific.** The `{:.4e}` branch is guarded
  by `&& get_thousands_separator().is_empty()`; set a separator and Polars falls
  back to the `{:.6}` decimal form, since scientific doesn't combine with digit
  grouping.

- **No magnitude threshold is configurable.** There is no knob for "go scientific
  above N". To force fixed-precision scientific (e.g. `1.235e8`) you must format
  the column to a string yourself.


## Why this matters here

`_print_all` (the `df._A` display) groups columns to fit the terminal using
`_estimate_col_widths`. A plain `cast(pl.String)` can't predict these widths,
because casting yields the *full* repr while the table shows the `mixed` one —
e.g. `374693089.85` casts to 12 chars but displays as `3.7469e8` (8), so a column
of such values gets sized ~2x too wide and the output fragments into extra
groups.

Rather than reimplement this ladder in Python (which would drift from the Rust
constants above), `_estimate_col_widths` renders a row sample through Polars and
reads the column widths off the table's top border. That is correct for every
dtype and for both float paths by construction — if column-width estimation ever
looks wrong, check that the global table format is still `ASCII_FULL_CONDENSED`
(the border parsing assumes it).
