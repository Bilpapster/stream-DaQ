📊 Compact vs Native Data Formats
=================================

When working with streaming data, especially in IoT and resource-constrained environments, you'll encounter two fundamental data representation patterns: **compact** and **native** formats. Understanding these formats and their trade-offs is crucial for designing effective data quality monitoring systems.

What Are Data Formats?
-----------------------

**Data format** refers to how information is structured and organized within individual records or messages in your data stream. The choice between compact and native formats significantly impacts bandwidth usage, storage requirements, processing complexity, and quality monitoring approaches.

Native Data Format
------------------

**Native format** represents each measurement or field as a separate record with its own timestamp and metadata. This is the traditional approach used in most database systems and streaming platforms.

**Structure Characteristics:**

- One record per field/measurement
- Each record contains full metadata (timestamp, identifiers, etc.)
- Direct field access and querying
- Explicit relationships through shared identifiers

**Example: IoT Sensor Network (Native Format)**

.. code-block:: json

    // Three separate records for one sensor reading cycle
    {"timestamp": 1640995200, "sensor_id": "env_001", "field": "temperature", "value": 23.5}
    {"timestamp": 1640995200, "sensor_id": "env_001", "field": "humidity", "value": 65.2}
    {"timestamp": 1640995200, "sensor_id": "env_001", "field": "pressure", "value": 1013.25}

**Native Format Benefits:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Simple Processing**
        :class-header: bg-success text-white

        Direct field access, no transformation needed

    .. grid-item-card:: **Standard Compatibility**
        :class-header: bg-info text-white

        Works with all existing tools and databases

    .. grid-item-card:: **Flexible Schemas**
        :class-header: bg-primary text-white

        Easy to add/remove fields without format changes

    .. grid-item-card:: **Clear Relationships**
        :class-header: bg-warning text-dark

        Explicit field-to-record mapping

**Native Format Challenges:**

- **Higher bandwidth usage**: Repeated metadata in every record
- **Storage overhead**: Redundant information across related measurements
- **Network inefficiency**: More messages for the same information
- **Increased latency**: Multiple round-trips for related data

Compact Data Format
-------------------

**Compact format** groups multiple related fields into a single record, typically using arrays or structured objects. This approach minimizes redundancy and optimizes for transmission efficiency.

**Structure Characteristics:**

- Multiple fields per record
- Shared metadata (timestamp, identifiers)
- Array-based or object-based field representation
- Implicit relationships through positional or key-based mapping

**Example: IoT Sensor Network (Compact Format)**

.. code-block:: json

    // Single record containing all three measurements
    {
        "timestamp": 1640995200,
        "sensor_id": "env_001", 
        "fields": ["temperature", "humidity", "pressure"],
        "values": [23.5, 65.2, 1013.25]
    }

**Alternative Compact Representations:**

.. code-block:: json

    // Object-based compact format
    {
        "timestamp": 1640995200,
        "sensor_id": "env_001",
        "measurements": {
            "temperature": 23.5,
            "humidity": 65.2,
            "pressure": 1013.25
        }
    }

    // Nested array format (common in time-series databases)
    {
        "timestamp": 1640995200,
        "sensor_id": "env_001",
        "data": [
            ["temperature", 23.5],
            ["humidity", 65.2], 
            ["pressure", 1013.25]
        ]
    }

**Compact Format Benefits:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Bandwidth Efficiency**
        :class-header: bg-success text-white

        ~60% reduction in network traffic vs native format

    .. grid-item-card:: **Storage Optimization**
        :class-header: bg-info text-white

        Minimal metadata redundancy, better compression

    .. grid-item-card:: **Atomic Operations**
        :class-header: bg-primary text-white

        Related fields transmitted together, ensuring consistency

    .. grid-item-card:: **Batch Efficiency**
        :class-header: bg-warning text-dark

        Fewer network round-trips for related measurements

**Compact Format Challenges:**

- **Processing complexity**: Requires transformation for field-level analysis
- **Tool compatibility**: Many tools expect native format
- **Schema rigidity**: Changes require format restructuring
- **Debugging difficulty**: Less intuitive data inspection

When to Use Each Format
-----------------------

The choice between compact and native formats depends on your specific use case, infrastructure constraints, and processing requirements.

**Choose Native Format When:**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Scenario
     - Rationale
   * - **High-bandwidth environments**
     - Network efficiency is not a primary concern
   * - **Heterogeneous field types**
     - Different fields have varying schemas or update frequencies
   * - **Real-time field processing**
     - Need immediate access to individual field values
   * - **Standard tool integration**
     - Using existing tools that expect native format
   * - **Dynamic schemas**
     - Frequently adding/removing fields or changing structure
   * - **Debugging and development**
     - Need clear visibility into individual field values

**Choose Compact Format When:**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Scenario
     - Rationale
   * - **Resource-constrained environments**
     - Limited bandwidth, battery, or storage capacity
   * - **IoT and sensor networks**
     - Multiple related measurements from same source
   * - **High-frequency data**
     - Thousands of measurements per second
   * - **Wireless transmission**
     - Cellular, satellite, or low-power radio networks
   * - **Batch processing workflows**
     - Processing related fields together
   * - **Time-series databases**
     - Optimized storage for temporal data patterns

IoT and Compact Data
--------------------

Compact data representations are particularly common in IoT scenarios due to the unique constraints and requirements of connected devices and sensor networks.

**Why IoT Favors Compact Formats:**

.. admonition:: Real-World IoT Constraints
   :class: note

   IoT devices often operate under severe resource constraints: limited battery life, restricted bandwidth, intermittent connectivity, and minimal processing power. Compact data formats directly address these challenges by minimizing the overhead associated with data transmission and storage.

**Common IoT Compact Data Scenarios:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Environmental Monitoring**
        :class-header: bg-success text-white

        Weather stations transmitting temperature, humidity, pressure, wind speed in single messages

    .. grid-item-card:: **Industrial Sensors**
        :class-header: bg-info text-white

        Manufacturing equipment sending vibration, temperature, speed, pressure readings together

    .. grid-item-card:: **Smart Buildings**
        :class-header: bg-primary text-white

        HVAC systems reporting occupancy, air quality, energy usage, temperature in batches

    .. grid-item-card:: **Vehicle Telemetry**
        :class-header: bg-warning text-dark

        Connected cars transmitting GPS, speed, fuel, engine metrics as compact payloads

**IoT Compact Data Benefits:**

1. **Battery Life Extension**: Fewer transmission cycles preserve device battery
2. **Bandwidth Optimization**: Critical for cellular or satellite connections
3. **Intermittent Connectivity**: Batch multiple readings for transmission when connected
4. **Edge Processing**: Aggregate multiple sensor readings before cloud transmission
5. **Cost Reduction**: Lower data transmission costs for cellular IoT deployments

**Example: Smart Agriculture Sensor**

.. code-block:: python

    # Compact format optimized for solar-powered field sensors
    {
        "timestamp": 1640995200,
        "device_id": "field_sensor_001",
        "location": {"lat": 40.7128, "lon": -74.0060},
        "readings": {
            "soil_moisture": 45.2,      # Percentage
            "soil_temperature": 18.5,   # Celsius  
            "ambient_temperature": 22.1, # Celsius
            "light_intensity": 850,      # Lux
            "battery_voltage": 3.7       # Volts
        }
    }

    # Equivalent native format would require 5 separate messages
    # with repeated timestamp, device_id, and location data

.. tip::
   
   For a complete working example of IoT sensor monitoring with compact data, see ``examples/compact_data.py`` and the implementation guide in :doc:`../examples/advanced-examples`.

Stream DaQ's Unified Approach
-----------------------------

Stream DaQ eliminates the traditional trade-off between format efficiency and processing simplicity by providing **automatic transformation** from compact to native formats during quality monitoring.

**How Stream DaQ Handles Both Formats:**

.. code-block:: python

    from streamdaq import StreamDaQ, CompactData, DaQMeasures as dqm

    # For native format - direct configuration
    daq_native = StreamDaQ().configure(
        source=native_data_stream,
        time_column="timestamp"
    )

    # For compact format - automatic transformation
    daq_compact = StreamDaQ().configure(
        source=compact_data_stream,
        time_column="timestamp",
        compact_data=CompactData()
            .with_fields_column("fields")
            .with_values_column("values")
            .with_values_dtype(float)
    )

    # Identical quality measures work for both formats!
    for daq in [daq_native, daq_compact]:
        daq.add(dqm.count('temperature'), name="temp_readings") \
           .add(dqm.mean('humidity'), assess="(40, 80)", name="humidity_range") \
           .add(dqm.missing_count('pressure'), assess="==0", name="pressure_completeness")

**Stream DaQ's Transformation Benefits:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Format Agnostic**
        :class-header: bg-success text-white

        Same quality measures work for both compact and native data

    .. grid-item-card:: **Zero Preprocessing**
        :class-header: bg-info text-white

        No manual transformation code required

    .. grid-item-card:: **Automatic Handling**
        :class-header: bg-primary text-white

        Missing values, data types, and temporal alignment managed automatically

    .. grid-item-card:: **Performance Optimized**
        :class-header: bg-warning text-dark

        Efficient streaming transformation without intermediate storage

**What Stream DaQ Eliminates:**

Without Stream DaQ's automatic handling, compact data monitoring typically requires:

.. code-block:: python

    # Manual transformation pipeline (what you DON'T need with Stream DaQ)
    def transform_compact_to_native(compact_record):
        """Manual compact-to-native transformation (Stream DaQ does this automatically)"""
        native_records = []
        timestamp = compact_record['timestamp']
        fields = compact_record['fields']
        values = compact_record['values']
        
        for field, value in zip(fields, values):
            if value is not None:  # Handle missing values
                native_records.append({
                    'timestamp': timestamp,
                    'field': field,
                    'value': value
                })
        return native_records

    # Stream DaQ eliminates this entire preprocessing step!

Format Comparison Summary
-------------------------

.. list-table:: **Compact vs Native Format Trade-offs**
   :header-rows: 1
   :widths: 25 35 40

   * - Aspect
     - Native Format
     - Compact Format
   * - **Bandwidth Usage**
     - Higher (repeated metadata)
     - Lower (~60% reduction)
   * - **Processing Complexity**
     - Simple (direct access)
     - Complex (requires transformation)
   * - **Tool Compatibility**
     - Universal support
     - Limited native support
   * - **Schema Flexibility**
     - High (easy field changes)
     - Medium (format restructuring needed)
   * - **IoT Suitability**
     - Poor (resource intensive)
     - Excellent (optimized for constraints)
   * - **Debugging**
     - Easy (clear field visibility)
     - Moderate (requires unpacking)
   * - **Storage Efficiency**
     - Lower (metadata overhead)
     - Higher (minimal redundancy)
   * - **Stream DaQ Support**
     - Native (no configuration)
     - Automatic (CompactData configuration)

Best Practices
--------------

**For Compact Data:**

1. **Use consistent field ordering** to simplify processing and debugging
2. **Include field metadata** (names, types) when schema might change
3. **Handle missing values explicitly** using ``null`` or special markers
4. **Document the compact format** clearly for team members and tools
5. **Consider hybrid approaches** for mixed-frequency data (some fields update more often)

**For Native Data:**

1. **Minimize metadata redundancy** where possible without losing clarity
2. **Use consistent field naming** across related measurements
3. **Include sufficient context** (device IDs, locations) in each record
4. **Optimize for your query patterns** (how you'll access the data)

**For Stream DaQ Users:**

1. **Start with your natural format** - don't transform data just for Stream DaQ
2. **Use CompactData configuration** for compact formats rather than manual transformation
3. **Define quality measures** the same way regardless of input format
4. **Test with both formats** if you're unsure which your data sources will use

What's Next?
------------

Now that you understand the differences between compact and native data formats:

- 🧙‍♂️ **See it in action**: :doc:`../examples/advanced-examples` - Complete compact data monitoring example with full code
- 💻 **Try the code**: ``examples/compact_data.py`` - Hands-on compact data implementation with detailed comments
- 🪟 **Learn about windowing**: :doc:`stream-windows` - How Stream DaQ processes both formats in time-based windows
- 📏 **Explore measures**: :doc:`measures-and-assessments` - Quality measures that work with both formats
- ⚡ **Understand real-time processing**: :doc:`real-time-monitoring` - Production considerations for both formats

The key insight is that **Stream DaQ lets you choose the optimal format for your infrastructure while maintaining consistent quality monitoring approaches**. Whether your data arrives in compact IoT payloads or traditional native records, your quality measures remain the same.