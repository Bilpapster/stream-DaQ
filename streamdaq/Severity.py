from enum import Enum

class Severity(str, Enum):
    """
    An enumeration representing different levels of severity for anomaly detection.
    Each level indicates the intensity or criticality of an anomaly detected in the data stream.
    1. WARMUP: Initial phase where the system is learning normal behavior; anomalies detected here may be less reliable.
    2. NORMAL: Standard operating level where anomalies are detected under typical conditions.
    3. MODERATE: Elevated level of anomalies indicating potential issues that require attention.
    4. HIGH: Significant anomalies that suggest serious problems needing immediate investigation.
    5. CRITICAL: Extreme anomalies indicating critical failures or urgent situations that demand immediate action.
    """
    WARMUP = "warmup"
    NORMAL = "normal"
    MODERATE = "moderate"
    HIGH = "high"
    CRITICAL = "critical"

    def __str__(self) -> str:
        return self.value