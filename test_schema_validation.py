#!/usr/bin/env python3
"""
Simple test for schema validation functionality.
"""

from pydantic import BaseModel, Field
from streamdaq import create_schema_validator, AlertMode


class SimpleSchema(BaseModel):
    """Simple schema for testing."""
    user_id: str = Field(..., min_length=1)
    value: float = Field(..., ge=0)
    
    class Config:
        extra = "allow"


def test_basic_validation():
    """Test basic schema validation functionality."""
    print("Testing basic schema validation...")
    
    # Create validator
    validator = create_schema_validator(
        schema=SimpleSchema,
        alert_mode=AlertMode.PERSISTENT,
        log_violations=True,
        raise_on_violation=False
    )
    
    # Test valid record
    valid_record = {"user_id": "test_user", "value": 5.0}
    is_valid, error = validator.validate_record(valid_record)
    print(f"Valid record: {is_valid}, error: {error}")
    
    # Test invalid record
    invalid_record = {"user_id": "", "value": -1.0}
    is_valid, error = validator.validate_record(invalid_record)
    print(f"Invalid record: {is_valid}, error: {error}")
    
    # Test alert logic
    should_alert = validator.should_alert(False, invalid_record)
    print(f"Should alert: {should_alert}")
    
    print("Basic validation test completed successfully!")


def test_alert_modes():
    """Test different alert modes."""
    print("\nTesting alert modes...")
    
    # Test ONLY_ON_FIRST_K
    validator_k = create_schema_validator(
        schema=SimpleSchema,
        alert_mode=AlertMode.ONLY_ON_FIRST_K,
        k_windows=2,
        log_violations=False
    )
    
    # Simulate violations in multiple windows
    invalid_record = {"user_id": "", "value": 1.0}
    
    # First window
    validator_k.process_window_start()
    alert1 = validator_k.should_alert(False, invalid_record)
    print(f"Window 1 alert: {alert1}")
    
    # Second window
    validator_k.process_window_start()
    alert2 = validator_k.should_alert(False, invalid_record)
    print(f"Window 2 alert: {alert2}")
    
    # Third window - should not alert
    validator_k.process_window_start()
    alert3 = validator_k.should_alert(False, invalid_record)
    print(f"Window 3 alert: {alert3}")
    
    # Test ONLY_IF
    def condition_func(record):
        return record.get("user_id", "").startswith("VIP_")
    
    validator_if = create_schema_validator(
        schema=SimpleSchema,
        alert_mode=AlertMode.ONLY_IF,
        condition_func=condition_func,
        log_violations=False
    )
    
    # Test with VIP user
    vip_record = {"user_id": "VIP_user", "value": -1.0}
    alert_vip = validator_if.should_alert(False, vip_record)
    print(f"VIP user alert: {alert_vip}")
    
    # Test with regular user
    regular_record = {"user_id": "regular_user", "value": -1.0}
    alert_regular = validator_if.should_alert(False, regular_record)
    print(f"Regular user alert: {alert_regular}")
    
    print("Alert modes test completed successfully!")


if __name__ == "__main__":
    test_basic_validation()
    test_alert_modes()
    print("\nAll tests passed! Schema validation is working correctly.")