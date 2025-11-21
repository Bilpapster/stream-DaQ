🛡️ Schema Validation
====================

Stream DaQ's schema validation provides comprehensive data structure and constraint enforcement through Pydantic models. This validation system acts as the first line of defense in data quality monitoring, ensuring data conforms to expected formats before quality measures are computed.

Core Principles
---------------

**Type Safety and Constraint Enforcement**

Schema validation enforces data integrity at multiple levels:

- **Type Validation**: Ensures fields match expected data types (int, str, float, etc.)
- **Constraint Checking**: Validates ranges, lengths, patterns, and custom business rules
- **Required Field Verification**: Confirms presence of mandatory data elements
- **Format Validation**: Checks email addresses, URLs, date formats, and custom patterns

**Pydantic Integration**

Stream DaQ leverages Pydantic's powerful validation capabilities:

.. code-block:: python

    from pydantic import BaseModel, Field

    class SensorReading(BaseModel):
        sensor_id: str = Field(..., min_length=5, pattern=r'^SENSOR_\d+$')
        temperature: float = Field(..., ge=-50, le=100)
        timestamp: int = Field(..., gt=0)
        location: Optional[str] = Field(None, max_length=100)

**Flexible Alert Strategies**

Schema validation provides sophisticated control over when and how violations are reported:

- **Persistent Alerts**: Always report violations for complete audit trails
- **First-K Windows**: Alert only during initial stabilization periods
- **Conditional Alerts**: Apply business logic to determine alert criticality

Validation Modes
-----------------

**Persistent Mode**

Always alerts on schema violations, providing complete data quality audit trails:

.. code-block:: python

    validator = create_schema_validator(
        schema=SensorReading,
        alert_mode=AlertMode.PERSISTENT,
        raise_on_violation=True,  # Fail fast on critical violations
        log_violations=True       # Maintain complete audit log
    )

**Use Cases:**
- Financial transaction processing
- Regulatory compliance systems
- Safety-critical applications
- Production data pipelines where data integrity is paramount

**First-K Windows Mode**

Alerts only during the first K windows with violations, then suppresses further alerts:

.. code-block:: python

    validator = create_schema_validator(
        schema=SensorReading,
        alert_mode=AlertMode.ONLY_ON_FIRST_K,
        k_windows=5,              # Alert for first 5 violation windows
        deflect_violating_records=True  # Separate invalid data for analysis
    )

**Use Cases:**
- System startup and initialization
- Development and testing environments
- IoT device deployment where initial calibration issues are expected
- Migration scenarios with temporary data quality issues

**Conditional Mode**

Applies custom business logic to determine when violations should trigger alerts:

.. code-block:: python

    def alert_condition(window_data: dict) -> bool:
        """Alert only during high-value transactions or peak hours."""
        transaction_volume = window_data.get('total_transactions', 0)
        hour = datetime.now().hour
        return transaction_volume > 1000 or 9 <= hour <= 17

    validator = create_schema_validator(
        schema=TransactionRecord,
        alert_mode=AlertMode.ONLY_IF,
        condition_func=alert_condition
    )

**Use Cases:**
- Business-aware data quality monitoring
- Dynamic alert prioritization based on operational context
- Resource-conscious environments where alert volume must be controlled
- Multi-tenant systems with varying quality requirements

Error Handling Strategies
--------------------------

**Record Deflection**

Invalid records can be automatically separated from the main data stream:

.. code-block:: python

    def write_deflected_records(data):
        # Send invalid records to separate processing pipeline
        pw.io.jsonlines.write(data, "invalid_records.jsonl")

    validator = create_schema_validator(
        schema=DataModel,
        deflect_violating_records=True,
        deflection_sink=write_deflected_records,
        include_error_messages=True  # Include validation error details
    )

**Benefits:**
- Prevents invalid data from corrupting downstream analysis
- Enables separate processing workflow for data cleaning
- Maintains detailed error information for debugging
- Supports data quality investigation and improvement

**Fail-Fast Processing**

For critical systems, validation can halt processing on violations:

.. code-block:: python

    validator = create_schema_validator(
        schema=CriticalData,
        raise_on_violation=True,    # Throw exception on validation failure
        log_violations=True         # Log details before failing
    )

**Benefits:**
- Ensures no invalid data enters critical processing pipelines
- Provides immediate feedback on data quality issues
- Supports strict compliance requirements
- Enables automated recovery procedures

Data Quality Integration
------------------------

**Pre-Processing Validation**

Schema validation occurs before quality measures are computed, ensuring:

- **Clean Input Data**: Only valid records contribute to quality statistics
- **Accurate Measures**: Quality calculations aren't skewed by malformed data
- **Reliable Baselines**: Historical quality baselines reflect true data patterns
- **Consistent Analysis**: Temporal comparisons use consistently structured data

Advanced Validation Patterns
-----------------------------

**Multi-Level Validation**

Combine multiple validation layers for comprehensive data quality:

.. code-block:: python

    class BasicValidation(BaseModel):
        """Essential field validation."""
        id: str = Field(..., min_length=1)
        timestamp: int = Field(..., gt=0)

    class BusinessValidation(BaseModel):
        """Business rule validation."""
        amount: float = Field(..., ge=0)
        currency: str = Field(..., regex=r'^[A-Z]{3}$')

    class ComplianceValidation(BaseModel):
        """Regulatory compliance validation."""
        customer_id: str = Field(..., min_length=10)
        risk_score: float = Field(..., ge=0, le=1)

**Custom Validators**

Implement domain-specific validation logic:

.. code-block:: python

    from pydantic import validator

    class TransactionData(BaseModel):
        amount: float
        account_balance: float

        @validator('amount')
        def amount_must_be_reasonable(cls, v, values):
            balance = values.get('account_balance', 0)
            if v > balance * 1.1:  # Allow 10% overdraft
                raise ValueError('Amount exceeds available balance')
            return v

**Cross-Field Validation**

Validate relationships between multiple fields:

.. code-block:: python

    class OrderData(BaseModel):
        order_date: datetime
        ship_date: Optional[datetime]
        total_amount: float
        discount_amount: float

        @validator('ship_date')
        def ship_date_after_order(cls, v, values):
            if v and values.get('order_date'):
                if v < values['order_date']:
                    raise ValueError('Ship date cannot be before order date')
            return v

        @validator('discount_amount')
        def discount_not_exceed_total(cls, v, values):
            total = values.get('total_amount', 0)
            if v > total:
                raise ValueError('Discount cannot exceed total amount')
            return v

Performance Considerations
--------------------------

**Validation Overhead**

Schema validation adds computational overhead proportional to:

- **Record Complexity**: Number and complexity of validated fields
- **Constraint Complexity**: Custom validators and cross-field validations
- **Error Handling**: Logging, deflection, and error message generation

For practical implementation examples, see :doc:`../examples/advanced-examples` and the ``examples/schema_validation.py`` file.