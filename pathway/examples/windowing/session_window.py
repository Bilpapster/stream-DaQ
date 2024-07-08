import pathway as pw
from datetime import timedelta

from data_markdown_source import get_chocolate_consumption_data


table = get_chocolate_consumption_data()
result = table.windowby(
    table.time,
    window=pw.temporal.session(max_gap=timedelta(hours=2)),
    instance=table.name,
).reduce(
    pw.this.name,
    session_start=pw.this._pw_window_start,
    session_end=pw.this._pw_window_end,
    chocolate_bars=pw.reducers.sum(pw.this.chocolate_bars)
)

pw.debug.compute_and_print(result, include_id=False)
