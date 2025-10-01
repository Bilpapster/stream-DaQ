"""
Compact-to-native data representation converter for Stream DaQ.

This module provides utilities to convert compact data representation (fields and values arrays)
to native representation (individual columns) automatically.
"""
import pathway as pw
from typing import List, Dict, Any, Union, Optional


@pw.udf(deterministic=True)
def extract_ith_value_as_float(values: list, i: int) -> float:
    """Extract the i-th value from a list of values and cast to float."""
    if i < len(values):
        val = values[i]
        if val is not None:
            return float(val)
    return 0.0


@pw.udf(deterministic=True)
def extract_ith_value_as_int(values: list, i: int) -> int:
    """Extract the i-th value from a list of values and cast to int."""
    if i < len(values):
        val = values[i]
        if val is not None:
            return int(val)
    return 0


@pw.udf(deterministic=True)
def extract_ith_value_as_str(values: list, i: int) -> str:
    """Extract the i-th value from a list of values and cast to string."""
    if i < len(values):
        val = values[i]
        if val is not None:
            return str(val)
    return ""


class CompactSchema(pw.Schema):
    """
    Schema for compact representation data.
    Expected format: {'fields': [...], 'values': [...]}
    """
    fields: list[str]
    values: list


def convert_compact_to_native(
    compact_table: pw.Table, 
    expected_fields: Optional[List[str]] = None,
    field_types: Optional[Dict[str, str]] = None
) -> pw.Table:
    """
    Convert a table with compact representation to native representation.
    
    Args:
        compact_table: Pathway table with 'fields' and 'values' columns
        expected_fields: Optional list of expected field names. If provided,
                        only these fields will be extracted in the given order.
                        If None, fields will be extracted from the first row.
        field_types: Optional dict mapping field names to types ("int", "float", "str").
                    If not provided, all fields are treated as float.
        
    Returns:
        Pathway table with individual columns for each field
        
    Example:
        Input table with columns ['fields', 'values']:
            fields=['temp', 'humidity'], values=[20.5, 60.2]
        
        Output table with columns ['temp', 'humidity']:
            temp=20.5, humidity=60.2
    """
    
    if expected_fields is None:
        # For now, we require expected_fields to be provided
        # TODO: In future versions, we could extract field names dynamically
        raise ValueError("expected_fields parameter is required for this version")
    
    if field_types is None:
        field_types = {field: "float" for field in expected_fields}
    
    # Create transformations for each expected field
    transformations = {}
    for i, field_name in enumerate(expected_fields):
        field_type = field_types.get(field_name, "float")
        
        if field_type == "int":
            transformations[field_name] = extract_ith_value_as_int(pw.this.values, i)
        elif field_type == "str":
            transformations[field_name] = extract_ith_value_as_str(pw.this.values, i)
        else:  # default to float
            transformations[field_name] = extract_ith_value_as_float(pw.this.values, i)
    
    # Apply transformations and remove original compact columns
    result = compact_table.with_columns(**transformations).without("fields", "values")
    
    return result


class CompactToNativeConverter:
    """
    A converter class that handles compact-to-native transformations for Stream DaQ.
    """
    
    def __init__(self, field_names: List[str], field_types: Optional[Dict[str, str]] = None):
        """
        Initialize the converter with expected field names.
        
        Args:
            field_names: List of expected field names in order
            field_types: Optional dict mapping field names to types ("int", "float", "str")
        """
        self.field_names = field_names
        self.field_types = field_types or {field: "float" for field in field_names}
    
    def convert(self, compact_table: pw.Table) -> pw.Table:
        """
        Convert a compact table to native representation.
        
        Args:
            compact_table: Table with compact representation
            
        Returns:
            Table with native representation
        """
        return convert_compact_to_native(compact_table, self.field_names, self.field_types)
    
    def create_mqtt_connector(
        self, 
        host: str, 
        port: int, 
        topic: str, 
        **kwargs
    ) -> pw.Table:
        """
        Create an MQTT connector that automatically converts compact data to native.
        
        Args:
            host: MQTT broker host
            port: MQTT broker port  
            topic: MQTT topic to subscribe to
            **kwargs: Additional arguments for MQTT connector
            
        Returns:
            Table with native representation
        """
        # Note: This is a placeholder for MQTT implementation
        # In a real implementation, you would use pw.io.mqtt.read or similar
        raise NotImplementedError(
            "MQTT connector implementation is not available in this version. "
            "Use create_python_connector for testing with Python subjects."
        )
    
    def create_python_connector(self, subject_class) -> pw.Table:
        """
        Create a Python connector that automatically converts compact data to native.
        
        Args:
            subject_class: A ConnectorSubject class that generates compact data
            
        Returns:
            Table with native representation
        """
        # Read compact data
        compact_table = pw.io.python.read(subject_class(), schema=CompactSchema)
        
        # Convert to native
        return self.convert(compact_table)


def create_compact_connector(
    field_names: List[str], 
    connector_type: str = "python",
    **connector_args
) -> CompactToNativeConverter:
    """
    Factory function to create a compact-to-native converter.
    
    Args:
        field_names: List of expected field names
        connector_type: Type of connector ("python", "mqtt", etc.)
        **connector_args: Arguments to pass to the specific connector
        
    Returns:
        CompactToNativeConverter instance
    """
    converter = CompactToNativeConverter(field_names)
    return converter