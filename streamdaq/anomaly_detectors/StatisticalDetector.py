from collections import defaultdict, deque

import numpy as np
import pathway as pw
from streamdaq.DaQMeasures import DaQMeasures
from streamdaq.anomaly_detectors.AnomalyDetector import AnomalyDetector

class StatisticalDetector(AnomalyDetector):
    def __init__(self, window_size=5, warmup_time=2, anomaly_threshold_method='percentile'):
        self.nof_summaries = 0
        self.window_size = window_size
        self.warmup_time = warmup_time
        self.rolling_history = defaultdict(lambda: deque(maxlen=window_size))
        self.rolling_means = {}
        self.windows_processed = 0

        # Statistical anomaly detection parameters
        self.anomaly_threshold_method = anomaly_threshold_method
        self.anomaly_scores_history = deque(maxlen=window_size)
        self.anomaly_threshold = 0

    def set_measures(self, data, time_column, instance) -> dict:
        profiling_measures = {}

        # Identify numeric columns
        numeric_columns = []

        for col_name in data.column_names():
            if col_name not in [time_column, instance, '_validation_metadata'] and not col_name.startswith('_pw'):
                try:
                    test_col = pw.cast(float, data[col_name])
                    numeric_columns.append(col_name)
                except Exception:
                    continue

        for col_name in numeric_columns:
            profiling_measures[f"{col_name}_min_prof"] = DaQMeasures.min(col_name)
            profiling_measures[f"{col_name}_max_prof"] = DaQMeasures.max(col_name)
            # profiling_measures[f"{col_name}_mean_prof"] = DaQMeasures.mean(col_name)
            # profiling_measures[f"{col_name}_missing_count_prof"] = DaQMeasures.missing_count(col_name)
            # profiling_measures[f"{col_name}_total_count_prof_prof"] = DaQMeasures.count(col_name)
            # profiling_measures[f"{col_name}_missing_percentage_prof"] = DaQMeasures.missing_fraction(col_name, precision=3)

        self.nof_summaries = int(len(profiling_measures) / len(numeric_columns) if numeric_columns else 0)
        # todo: Identify categorical columns and implement peculiarity measure
        # categorical_columns = []
        # for col_name in data.column_names():
        profiling_measures["_pw_window_start"] = pw.this._pw_window_start
        profiling_measures["_pw_window_end"] = pw.this._pw_window_end

        return profiling_measures

    def update_rolling_means(self, current_measures):
        """Update rolling means with current measure values"""
        for measure_name, current_value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue
            # Add current value to rolling history
            self.rolling_history[measure_name].append(current_value)

            # Calculate rolling mean
            if len(self.rolling_history[measure_name]) > 0:
                self.rolling_means[measure_name] = sum(self.rolling_history[measure_name]) / len(
                    self.rolling_history[measure_name])

    def compute_anomaly_score(self, current_measures):
        """Compute anomaly score based on difference from rolling mean"""
        # Return 0 during warmup period
        if self.windows_processed < self.warmup_time:
            return 0.0

        if not self.rolling_means:
            return 0.0

        differences = []

        for measure_name, current_value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue

            if measure_name in self.rolling_means:
                rolling_mean = self.rolling_means[measure_name]
                # Handle division by zero for percentage difference
                if rolling_mean != 0:
                    diff = abs(current_value - rolling_mean) / abs(rolling_mean)
                else:
                    diff = abs(current_value) if current_value != 0 else 0
                differences.append(diff)
        # Return mean of all differences as final anomaly score
        return sum(differences) / len(differences) if differences else 0.0

    def train_anomaly_threshold(self, current_anomaly_score):
        """Update statistical threshold for anomaly detection"""
        self.anomaly_scores_history.append(current_anomaly_score)

        scores = list(self.anomaly_scores_history)

        if self.anomaly_threshold_method == 'zscore':
            # Z-score method: threshold at 2 standard deviations
            mean_score = np.mean(scores)
            std_score = np.std(scores)
            self.anomaly_threshold = mean_score + 2 * std_score

        elif self.anomaly_threshold_method == 'percentile':
            # Percentile method: 95th percentile as threshold
            self.anomaly_threshold = np.percentile(scores, 95)

        elif self.anomaly_threshold_method == 'iqr':
            # Interquartile Range method
            q1 = np.percentile(scores, 25)
            q3 = np.percentile(scores, 75)
            iqr = q3 - q1
            self.anomaly_threshold = q3 + 1.5 * iqr

    def is_anomalous(self, current_anomaly_score):
        """Determine if current score indicates an anomaly"""
        if self.anomaly_threshold is None or self.windows_processed <= self.warmup_time:
            return False

        return current_anomaly_score > self.anomaly_threshold

    def get_anomaly_severity(self, current_score):
        """Get anomaly severity level"""
        if not self.is_anomalous(current_score):
            return "normal"

        if self.anomaly_threshold is None or self.anomaly_threshold == 0:
            return "unknown"

        # Calculate severity based on how far above threshold
        severity_ratio = current_score / self.anomaly_threshold

        if severity_ratio > 2.0:
            return "critical"
        elif severity_ratio > 1.5:
            return "high"
        else:
            return "moderate"

    def window_processor(self, **kwargs) -> tuple[float, str]:
        row = dict(kwargs)

        # Update rolling means
        self.update_rolling_means(row)

        # Increment windows processed counter
        self.windows_processed += 1

        # Compute anomaly score
        anomaly_score = self.compute_anomaly_score(row)

        # Update statistical threshold (only after warmup)
        if self.windows_processed > self.warmup_time:
            self.train_anomaly_threshold(anomaly_score)

        # Check if current window is anomalous
        severity = self.get_anomaly_severity(anomaly_score)

        return anomaly_score, severity

    def consume_windows(self, windowed_stream: pw.Table) -> pw.Table:
        profiled_stream = windowed_stream.select(
            **{col: pw.this[col] for col in windowed_stream.column_names()},  # Keep all existing columns
            _anomaly_metadata=pw.apply_with_type(
                self.window_processor,
                tuple[float, str],
                **{col: pw.this[col] for col in windowed_stream.column_names()}
            )
        )
        return profiled_stream