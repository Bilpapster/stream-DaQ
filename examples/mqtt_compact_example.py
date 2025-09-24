#!/usr/bin/env python3
"""
Comprehensive example of StreamDaQ with MQTT compact data representation.

This example demonstrates:
1. How to use StreamDaQ with compact MQTT data
2. The automatic transformation from compact to native representation
3. Applying data quality measures on native fields
"""

import sys
import os
sys.path.insert(0, '/home/runner/work/stream-DaQ/stream-DaQ')

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, mqtt_connectors


def main():
    """Main example function."""
    print("=== StreamDaQ MQTT Compact Data Representation Example ===")
    print()
    
    # Define the expected field names
    field_names = ["temperature", "humidity", "pressure"]
    
    print(f"Field names: {field_names}")
    print("Data format expected: {'fields': [...], 'values': [...], 'timestamp': ...}")
    print()
    
    # Create MQTT source with compact representation support
    mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
        field_names=field_names,
        broker_host="localhost",  # Would be actual MQTT broker in production
        topic="sensors/environment"
    )
    
    # Configure StreamDaQ with compact fields
    daq = StreamDaQ().configure(
        window=Windows.tumbling(5),  # 5-second windows
        time_column="timestamp",
        source=mqtt_source,
        compact_fields=field_names,  # Enable automatic compact-to-native transformation
        wait_for_late=1,
    )
    
    # Define data quality measures using the native field names
    # Note: Even though the source data is compact, we can reference fields directly
    print("Data Quality Measures:")
    print("- Temperature count > 0")
    print("- Average temperature in range [15, 35]°C") 
    print("- Average humidity in range [20, 90]%")
    print("- Average pressure in range [980, 1040] hPa")
    print()
    
    daq.add(dqm.count('temperature'), assess=">0", name="temp_count") \
       .add(dqm.mean('temperature'), assess="[15, 35]", name="avg_temp") \
       .add(dqm.mean('humidity'), assess="[20, 90]", name="avg_humidity") \
       .add(dqm.mean('pressure'), assess="[980, 1040]", name="avg_pressure")
    
    print("Starting monitoring...")
    print("The compact MQTT data will be automatically transformed to native format.")
    print("Results show: (value, assessment_passed)")
    print()
    
    # Start monitoring
    daq.watch_out()


if __name__ == "__main__":
    main()