from abc import ABC, abstractmethod
import pathway as pw


class AnomalyDetector(ABC):
    """Abstract base class for all anomaly detectors"""

    def __init__(self, **kwargs):
        """Initialize the anomaly detector with configuration parameters"""
        pass

    @abstractmethod
    def set_measures(self, data: pw.Table, time_column: str, instance: str) -> dict:
        """
        Set profiling measures based on the data structure
        :param data: The input data stream to perform anomaly detection on
        :param time_column: The name of the time column in the data
        :param instance: The name of the instance column in the data
        :return dictionary of measures to be computed
        """
        pass

    @abstractmethod
    def consume_windows(self, windowed_stream: pw.Table) -> pw.Table:
        """
        Driver function to consume windowed data streams and perform anomaly detection.
        :param windowed_stream: a windowed (pw.GroupedTable) data stream
        """
        pass