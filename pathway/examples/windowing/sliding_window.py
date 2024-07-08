import pathway as pw
from datetime import timedelta
from data_markdown_source import get_chocolate_consumption_data


table = get_chocolate_consumption_data()
result = table.windowby(
    table.time,
    window=pw.temporal.sliding(duration=timedelta(hours=10), hop=timedelta(hours=3)),
    instance=table.name
).reduce(
    name=pw.this._pw_instance,
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    chocolate_bars=pw.reducers.sum(pw.this.chocolate_bars),
)

pw.debug.compute_and_print(result, include_id=False)
