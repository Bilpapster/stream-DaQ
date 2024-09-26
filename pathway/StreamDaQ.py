import pathway as pw
from datetime import timedelta
from typing import Self

from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial


class StreamDaQ:
    def __init__(self):
        self.measures = {}
        self.window = None
        self.instance = None
        self.time_column = None
        self.wait_for_late = None
        self.time_format = None
        self.show_window_start = True
        self.show_window_end = True
        self.sink_file_name = None

    def configure(self, window: Window, instance: str, time_column: str,
                  wait_for_late: int | float | timedelta | None = None,
                  time_format: str = '%Y-%m-%d %H:%M:%S', show_window_start: bool = True,
                  show_window_end: bool = True, sink_file_name: str = None) -> Self:
        self.window = window
        self.instance = instance
        self.time_column = time_column
        self.wait_for_late = wait_for_late
        self.time_format = time_format
        self.show_window_start = show_window_start
        self.show_window_end = show_window_end
        self.sink_file_name = sink_file_name
        return self

    def add(self, measure: pw.ColumnExpression | ReducerExpression, name: str) -> Self:
        self.measures[name] = measure
        return self

    def watch_out(self):
        if self.show_window_start:
            self.measures['window_start'] = pw.this._pw_window_start
        if self.show_window_end:
            self.measures['window_end'] = pw.this._pw_window_end

        data = artificial(number_of_rows=100, input_rate=10) \
            .with_columns(date_and_time=pw.this.timestamp.dt.strptime(self.time_format))

        data = data.windowby(
            data.date_and_time,
            window=self.window,
            instance=data[self.instance],
            behavior=pw.temporal.exactly_once_behavior(shift=timedelta(seconds=self.wait_for_late)),
        ).reduce(**self.measures)
        pw.debug.compute_and_print(data, include_id=False)
        # pw.io.csv.write(data, self.sink_file_name)
        pw.run()
