from collections import defaultdict, deque
from typing import Tuple, List

import numpy as np
import pathway as pw
from streamdaq.DaQMeasures import DaQMeasures
from streamdaq.anomaly_detectors.AnomalyDetector import AnomalyDetector

class StatisticalDetector(AnomalyDetector):
    def __init__(self, buffer_size=5, warmup_time=2, anomaly_threshold_method='percentile', training_period=5, top_k=1):
        self.nof_summaries = 0
        self.buffer_size = buffer_size
        self.warmup_time = warmup_time
        self.top_k = top_k
        self.training_period = training_period
        self.rolling_history = defaultdict(lambda: deque(maxlen=buffer_size))
        self.rolling_means = {}
        self.windows_processed = 0

        # Statistical anomaly detection parameters
        self.anomaly_threshold_method = anomaly_threshold_method
        self.anomaly_scores_history = deque(maxlen=buffer_size)
        self.anomaly_threshold = None  # use None to clearly indicate "not trained yet"

    def set_measures(self, data, time_column, instance) -> dict:
        profiling_measures = {}

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

        self.nof_summaries = int(len(profiling_measures) / len(numeric_columns) if numeric_columns else 0)
        profiling_measures["_pw_window_start"] = pw.this._pw_window_start
        profiling_measures["_pw_window_end"] = pw.this._pw_window_end

        return profiling_measures

    def update_rolling_means(self, current_measures):
        """Append current values to history and recalculate rolling means."""
        for measure_name, current_value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue
            self.rolling_history[measure_name].append(current_value)
            hist = self.rolling_history[measure_name]
            if len(hist) > 0:
                self.rolling_means[measure_name] = sum(hist) / len(hist)

    def compute_anomaly_score(self, current_measures) -> Tuple[float, List[Tuple[str, float]]]:
        """Compute anomaly score and identify the measure with the largest deviation.

        Returns:
            (average_score, top_measure_name_or_None, top_diff)
        """
        # Return 0 during warmup period (windows_processed counts previously seen windows)
        if self.windows_processed < self.warmup_time:
            return 0.0, []

        if not self.rolling_means:
            return 0.0, []

        diffs = []
        for measure_name, current_value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue
            if measure_name in self.rolling_means:
                rolling_mean = self.rolling_means[measure_name]
                # Use relative difference when possible, otherwise absolute difference
                if rolling_mean != 0:
                    diff = abs(current_value - rolling_mean) / abs(rolling_mean)
                else:
                    diff = abs(current_value - rolling_mean)
                diffs.append((measure_name, float(diff)))

        # overall score = average of diffs
        avg_score = sum(d for _, d in diffs) / len(diffs)

        # identify top-k deviating measures
        diffs.sort(key=lambda t: t[1], reverse=True)
        k = max(1, min(self.top_k, len(diffs)))
        top_k_list = diffs[:k]

        return float(avg_score), top_k_list

    def train_anomaly_threshold(self):
        """Update statistical threshold for anomaly detection."""
        scores = list(self.anomaly_scores_history)
        if not scores:
            return

        if self.anomaly_threshold_method == 'zscore':
            mean_score = np.mean(scores)
            std_score = np.std(scores)
            self.anomaly_threshold = mean_score + 2 * std_score
        elif self.anomaly_threshold_method == 'percentile':
            self.anomaly_threshold = float(np.percentile(scores, 95))
        elif self.anomaly_threshold_method == 'iqr':
            q1 = np.percentile(scores, 25)
            q3 = np.percentile(scores, 75)
            iqr = q3 - q1
            self.anomaly_threshold = float(q3 + 1.5 * iqr)

    def is_anomalous(self, current_anomaly_score):
        """Determine if current score indicates an anomaly."""
        # Require threshold to be trained and warmup to be completed
        if self.anomaly_threshold is None or self.windows_processed < self.warmup_time:
            return False
        return current_anomaly_score > self.anomaly_threshold

    def get_anomaly_severity(self, current_score):
        """Get anomaly severity level."""
        if not self.is_anomalous(current_score):
            return "normal"

        if self.anomaly_threshold is None or self.anomaly_threshold == 0:
            return "warmup period"

        severity_ratio = current_score / self.anomaly_threshold
        if severity_ratio > 2.0:
            return "critical"
        elif severity_ratio > 1.5:
            return "high"
        else:
            return "moderate"

    def window_processor(self, **kwargs) -> Tuple[float, str, List[Tuple[str, float]]]:
        row = dict(kwargs)

        # Compute anomaly score against existing rolling means (history excludes current row)
        anomaly_score, top_k_measures = self.compute_anomaly_score(row)
        self.anomaly_scores_history.append(anomaly_score)

        # Train threshold if warmup already completed (windows_processed counts previous windows)
        if self.windows_processed >= self.warmup_time:
            self.train_anomaly_threshold()

        if self.windows_processed % self.training_period == 0:
            self.train_anomaly_threshold()

        # Determine severity based on current score
        severity = self.get_anomaly_severity(anomaly_score)

        # Now update history/rolling means with the current row so next window uses it
        self.update_rolling_means(row)

        # Increment windows processed counter after using it for warmup/training logic
        self.windows_processed += 1
        # Only report top-k measures when it's anomalous
        if severity != "normal":
            return anomaly_score, severity, top_k_measures or []
        else:
            return anomaly_score, severity, []

    def consume_windows(self, windowed_stream: pw.Table) -> pw.Table:
        profiled_stream = windowed_stream.select(
            **{col: pw.this[col] for col in windowed_stream.column_names()},
            _anomaly_metadata =pw.apply(self.window_processor, **{col: pw.this[col] for col in windowed_stream.column_names()})
        )
        return profiled_stream