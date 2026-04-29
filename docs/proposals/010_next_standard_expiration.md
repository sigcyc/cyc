# next_standard_expiration



## Requirement
Write a function in cyc/time_util.py to implement the logic
```
@lru_cache(maxsize=1024)
def _next_standard_expiration(date: _date, calendar: str = "nyse") -> _date:
```
- The typical standard expiration is on the third Friday of a month
- If the third Friday is a holiday of the calendar. For NYSE, the standard expiration is the date before. For SSE, the standard expiration is the next trading day.


- Write another function 

`def next_standard_expiration(date: pl.Series, calendar: str = "nyse") -> pl.Series:`

It maps each unqiue value of date to next_standard_expiration(date)

