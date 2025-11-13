import inspect
from collections import defaultdict, deque
from typing import Tuple, List, Union, Callable, Any

import numpy as np
import pathway as pw
from streamdaq.DaQMeasures import DaQMeasures
from streamdaq.anomaly_detectors.AnomalyDetector import AnomalyDetector

class StatisticalDetector(AnomalyDetector):
    """
    Statistical anomaly detector using rolling window z-score analysis for streaming data.

    This detector implements an online anomaly detection algorithm that:
    1. Profiles numeric columns using configurable statistical DaQMeasures such as (mean, stddev, min, max, etc.)
    2. Maintains rolling windows of historical statistics with fixed capacity
    3. Detects anomalies by comparing current measures against rolling statistics using z-scores
    4. Provides severity classification and root cause analysis (RCA) via top-k deviating measures

    Algorithm Overview:
    - **Rolling Window Statistics**: Maintains deques of size `buffer_size` for each profiled measure
    - **Z-Score Calculation**: Computes standardized deviation from rolling mean: |x - μ| / σ
    - **Anomaly Detection**: Flags windows where max z-score exceeds `threshold` standard deviations
    - **Severity Classification**: Categorizes anomalies as moderate/high/critical based on z-score multiples
    - **Root Cause Analysis**: Identifies top-k measures contributing most to anomaly score

    Key Features:
    - **Adaptive Baseline**: Rolling statistics adapt to data distribution changes over time
    - **Memory Efficient**: Fixed-size deques prevent unbounded memory growth
    - **Warmup Period**: Allows initial windows for stable baseline establishment
    - **Flexible Profiling**: Supports custom measures via DaQMeasures or user-defined functions
    - **Multi-Column Analysis**: Simultaneously profiles multiple numeric columns

    Workflow:
    1. **Initialization**: Configure buffer size, warmup time, threshold, and profiling measures
    2. **Measure Setup**: Automatically detects numeric columns and creates statistical expressions
    3. **Window Processing**: For each data window:
       - Compute statistical measures (mean, stddev, etc.) for current window
       - Update rolling history with new measure values
       - Calculate z-scores against rolling baseline statistics
       - Determine anomaly severity and identify top contributing measures
    4. **Output**: Returns severity level and ranked list of anomalous measures

    Parameters:
        buffer_size (int): Rolling window capacity for historical statistics (default: 5)
        warmup_time (int): Number of initial windows to skip anomaly detection (default: 2)
        threshold (float): Z-score threshold for anomaly detection in standard deviations (default: 2.0)
        top_k (int): Number of top deviating measures to return for RCA (default: 1)
        measures (List[str]): Statistical measures to compute (default: ["mean", "stddev", "min", "max"])
        columns_profiled (List[str], optional): Specific columns to profile (default: all numeric columns)

    Severity Levels:
        - "warmup period": During initial warmup_time windows
        - "normal": Z-score ≤ threshold
        - "moderate": threshold < Z-score ≤ 2×threshold
        - "high": 2×threshold < Z-score ≤ 3×threshold
        - "critical": Z-score > 3×threshold

    Example Usage:
        detector = StatisticalDetector(
            buffer_size=10,
            warmup_time=3,
            threshold=2.5,
            top_k=3,
            measures=["mean", "stddev", "p95"]
        )

    Returns:
        For each window: Tuple[severity: str, top_k_measures: List[Tuple[measure_name, z_score]]]

    Note:
        - Requires at least 2 windows in rolling history for variance calculation
        - Zero variance cases fall back to absolute difference from mean
        - Automatically filters non-numeric columns during setup
    """
    MeasureSpec = Union[
        str,
        Callable[..., Any],
        pw.internals.expression.ColumnExpression,
        pw.internals.expression.ReducerExpression,
        Tuple[Any, ...],
    ]

    def __init__(self, buffer_size=5, warmup_time=2, threshold = 2, top_k=1, measures=["mean", "stddev", "min", "max"], columns_profiled=None):
        super().__init__()

        # Data Profiling setup
        self.nof_summaries = 0
        self.measures = measures
        self.columns_profiled = columns_profiled
        self.buffer_size = buffer_size
        self.warmup_time = warmup_time

        # RCA setup
        self.top_k = top_k

        # Statistical anomaly detection parameters
        self.rolling_means = defaultdict(lambda: deque(maxlen=buffer_size))
        self.threshold = threshold
        self.windows_processed = 0

    def _resolve_measure(self, measure_spec: MeasureSpec, column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Resolve a measure specification into a pathway ColumnExpression/ReducerExpression.

        Supported specs:
          - "mean" (string) -> calls DaQMeasures.mean(column_name)
          - ("mean", 4) -> calls DaQMeasures.mean(column_name, 4)
          - ("range_conformance_fraction", 0, 10, {"precision": 2}) -> kwargs support
          - callable -> called with column_name if the callable accepts parameters, otherwise called without args
          - existing pw expression -> returned as-is
        """
        # Already an expression -> return as-is
        expr_types = (pw.internals.expression.ColumnExpression, pw.internals.expression.ReducerExpression)
        if isinstance(measure_spec, expr_types):
            return measure_spec

        # String -> lookup on DaQMeasures
        if isinstance(measure_spec, str):
            fn = getattr(DaQMeasures, measure_spec, None)
            if fn is None or not callable(fn):
                raise ValueError(f"Unknown DaQMeasures measure: {measure_spec}")
            return fn(column_name)

        # Tuple -> first element is method name or callable, rest are args; last element may be kwargs dict
        if isinstance(measure_spec, tuple):
            if len(measure_spec) == 0:
                raise ValueError("Empty tuple provided as measure specification")
            first = measure_spec[0]
            args = list(measure_spec[1:])

            kwargs = {}
            if args and isinstance(args[-1], dict):
                kwargs = args.pop()  # remove last element as kwargs

            if isinstance(first, str):
                fn = getattr(DaQMeasures, first, None)
                if fn is None or not callable(fn):
                    raise ValueError(f"Unknown DaQMeasures measure: {first}")
            elif callable(first):
                fn = first
            else:
                raise ValueError("Tuple measure specification must start with a method name or callable")

            # Always pass column_name as the first positional argument to DaQMeasures methods when they expect it
            return fn(column_name, *args, **kwargs) if callable(fn) else fn

        # Callable -> try to call with column_name if signature accepts params, else without
        if callable(measure_spec):
            sig = inspect.signature(measure_spec)
            if len(sig.parameters) >= 1:
                return measure_spec(column_name)
            return measure_spec()

        raise ValueError(f"Unsupported measure specification type: {type(measure_spec)}")

    def set_measures(self, data, time_column, instance, prefix = "prof") -> dict:
        profiling_measures = {}

        numeric_columns = []

        for col_name in data.column_names():
            if col_name not in [time_column, instance, '_validation_metadata'] and not col_name.startswith('_pw'):
                try:
                    test_col = pw.cast(float, data[col_name])
                    numeric_columns.append(col_name)
                except Exception:
                    continue

        if all(isinstance(item, tuple) for item in self.measures):
            for measure, col_name in self.measures:
                if col_name in numeric_columns:
                    expr = self._resolve_measure(measure, col_name)
                    measure_name = f"{prefix}_{col_name}_{measure if isinstance(measure, str) else getattr(expr, 'name', 'custom')}"
                    profiling_measures[measure_name] = expr
        elif all(isinstance(item, str) for item in self.measures):
            if self.columns_profiled is None:
                self.columns_profiled = numeric_columns

            for measure in self.measures:
                for col_name in self.columns_profiled:
                        expr = self._resolve_measure(measure, col_name)
                        measure_name = f"{prefix}_{col_name}_{measure}"
                        profiling_measures[measure_name] = expr

        self.nof_summaries = int(len(profiling_measures) / len(numeric_columns) if numeric_columns else 0)
        profiling_measures["_pw_window_start"] = pw.this._pw_window_start
        profiling_measures["_pw_window_end"] = pw.this._pw_window_end

        return profiling_measures

    def update_online_normal_stats(self, current_measures):
        """Update rolling window statistics using deque with fixed capacity."""
        for measure_name, value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue

            # Add new value to rolling window (automatically removes oldest if at capacity)
            self.rolling_means[measure_name].append(value)

    def get_rolling_stats(self, measure_name):
        """Calculate mean and variance from current rolling window."""
        if measure_name not in self.rolling_means or len(self.rolling_means[measure_name]) == 0:
            return 0.0, 0.0

        values = list(self.rolling_means[measure_name])
        count = len(values)

        if count == 1:
            return values[0], 0.0

        mean = sum(values) / count
        variance = sum((x - mean) ** 2 for x in values) / (count - 1)

        return mean, variance

    def compute_anomaly_score(self, current_measures) -> Tuple[float, List[Tuple[str, float]]]:
        """Compute anomaly score using OnlineNormal algorithm."""
        # Update rolling statistics with current measures
        self.update_online_normal_stats(current_measures)

        # Return 0 during warmup period
        if self.windows_processed < self.warmup_time:
            return 0.0, []

        z_scores = []
        for measure_name, current_value in current_measures.items():
            if measure_name in ["_pw_window_start", "_pw_window_end"]:
                continue

            if measure_name in self.rolling_means:
                running_mean, running_variance = self.get_rolling_stats(measure_name)

                if running_variance > 0:
                    running_std = np.sqrt(running_variance)
                    z_score = abs(current_value - running_mean) / running_std
                    z_scores.append((measure_name, float(z_score)))
                elif len(self.rolling_means[measure_name]) > 1:
                    # Fallback for zero variance case
                    diff = abs(current_value - running_mean)
                    z_scores.append((measure_name, float(diff)))

        if not z_scores:
            return 0.0, []

        # Overall score = maximum z-score
        anomaly_score = max(z for _, z in z_scores)

        # Identify top-k deviating measures
        z_scores.sort(key=lambda t: t[1], reverse=True)
        k = max(1, min(self.top_k, len(z_scores)))
        top_k_list = [(name, round(score, 3)) for name, score in z_scores[:k]]

        return float(anomaly_score), top_k_list

    def is_anomalous(self, current_anomaly_score):
        """Determine if current score indicates an anomaly using standard deviation bound."""
        # For OnlineNormal, threshold is directly the number of standard deviations
        return current_anomaly_score > self.threshold

    def get_anomaly_severity(self, current_score):
        """Get anomaly severity based on standard deviation multiples."""
        if self.windows_processed < self.warmup_time:
            return "warmup period"

        if not self.is_anomalous(current_score):
            return "normal"

        # Severity based on multiples of the threshold
        if current_score > 3 * self.threshold:
            return "critical"
        elif current_score > 2 * self.threshold:
            return "high"
        else:
            return "moderate"

    def window_processor(self, **kwargs) -> Tuple[str, List[Tuple[str, float]]]:
        """Process a single window of data to detect anomalies."""
        row = dict(kwargs)

        anomaly_score, top_k_measures = self.compute_anomaly_score(row)

        # Determine severity
        severity = self.get_anomaly_severity(anomaly_score)

        # Increment counter after warmup
        self.windows_processed += 1

        # Return results
        if severity != "normal":
            return severity, top_k_measures or []
        else:
            return severity, []

    def consume_windows(self, windowed_stream: pw.Table) -> pw.Table:
        """Process stream window-by-window to detect anomalies."""
        profiled_stream = windowed_stream.select(
            **{col: pw.this[col] for col in windowed_stream.column_names()},
            _anomaly_metadata =pw.apply(self.window_processor, **{col: pw.this[col] for col in windowed_stream.column_names()})
        )
        return profiled_stream