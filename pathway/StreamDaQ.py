import pathway as pw
from datetime import timedelta
from typing import Self

from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial


class StreamDaQ:
    """
    The fundamental class of the DQ monitoring system. An instance of this class is necessary and sufficient to
    perform the following actions/steps: \n
    1. Configure the monitoring details, such as the type and parameters of the monitoring window, the way late events
    are handled and the source/sink details. \n
    2. Define what **exactly** DQ means for your unique use-case, by adding DQ measurements of your data stream, from
    a palette of built-in, real-time measurements. \n
    3. Kick-off DQ monitoring of your data stream, letting Stream DaQ continuously watch out your data, while you
    focus on the important.
    """

    def __init__(self):
        """
        Class constructor. Initializes to default/None values or potentially useful class arguments.
        """
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
        """
        Configures the DQ monitoring parameters. Specifying a window object, the key instance and the time column name
        cannot be omitted. The rest of the arguments are optional and come with rational default values.
        :param window: a window object to use for widowing the source stream.
        :param instance: the name of the column that contains the key for each incoming element.
        :param time_column: the name of the column that contains the date/time information for every element.
        :param wait_for_late: the number of seconds to wait after the end timestamp of each window. Late elements that
        arrive more than `wait_for_late` seconds after the window is closed will be ignored.
        :param time_format: the format of the values in the column that contains date/time information
        :param show_window_start: boolean flag to specify whether the window starting timestamp should be included in
        the results
        :param show_window_end: boolean flag to specify whether the window ending timestamp should be included in
        the results
        :param sink_file_name: the name of the file to write the output to
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
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
        """
        Adds a DQ measurement to be monitored within the stream windows.
        :param measure: the measure to be monitored
        :param name: the name with which the measure will appear in the results
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        self.measures[name] = measure
        return self

    def watch_out(self):
        """
        Kicks-off the monitoring process. Calling this function at the end of your driver program is necessary, or else
        nothing of what you have declared before will be executed.
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
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
