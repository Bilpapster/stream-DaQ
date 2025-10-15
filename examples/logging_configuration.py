#!/usr/bin/env python3
"""
Example demonstrating the logging configuration capabilities of Stream DaQ.

This example shows how to configure logging levels and formats to control
the verbosity of Stream DaQ's output.
"""
import logging
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from streamdaq import configure_logging, set_level


def example_default_logging():
    """Example using default logging configuration (INFO level)."""
    print("\n" + "="*70)
    print("Example 1: Default Logging (INFO level)")
    print("="*70)
    
    # Default configuration is INFO level, so INFO and above will be shown
    # DEBUG messages will not be shown
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    # No source is specified, so artificial data will be used
    # This will trigger an INFO log message
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured with default logging")
    print("  Notice the INFO message about artificial data above")


def example_debug_logging():
    """Example using DEBUG logging level for detailed output."""
    print("\n" + "="*70)
    print("Example 2: Debug Logging (DEBUG level)")
    print("="*70)
    
    # Set logging to DEBUG to see all messages including debug details
    set_level(logging.DEBUG)
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured with DEBUG logging")
    print("  You'll see more detailed messages including DEBUG level")


def example_warning_logging():
    """Example using WARNING logging level for minimal output."""
    print("\n" + "="*70)
    print("Example 3: Warning Logging (WARNING level)")
    print("="*70)
    
    # Set logging to WARNING to see only warnings and errors
    set_level(logging.WARNING)
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured with WARNING logging")
    print("  Notice that INFO messages are suppressed now")


def example_custom_format():
    """Example using custom logging format."""
    print("\n" + "="*70)
    print("Example 4: Custom Logging Format")
    print("="*70)
    
    # Configure logging with a custom format
    configure_logging(
        level=logging.INFO,
        format_string="%(asctime)s - %(levelname)s - %(message)s",
        date_format="%H:%M:%S"
    )
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured with custom logging format")
    print("  Notice the different format with timestamp")


def example_minimal_logging():
    """Example with minimal logging (ERROR level only)."""
    print("\n" + "="*70)
    print("Example 5: Minimal Logging (ERROR level)")
    print("="*70)
    
    # Set logging to ERROR to see only errors
    set_level(logging.ERROR)
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured with ERROR logging")
    print("  Only errors will be shown (none in this example)")


def example_string_level():
    """Example using string level names."""
    print("\n" + "="*70)
    print("Example 6: Using String Level Names")
    print("="*70)
    
    # You can use string names for convenience
    set_level("INFO")
    
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )
    
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    
    print("✓ StreamDaQ configured using string level name 'INFO'")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("Stream DaQ Logging Configuration Examples")
    print("="*70)
    print("\nThis example demonstrates various logging configurations.")
    print("Note: These examples don't run the actual stream processing")
    print("      to keep the demo focused on logging configuration.\n")
    
    try:
        example_default_logging()
        example_debug_logging()
        example_warning_logging()
        example_custom_format()
        example_minimal_logging()
        example_string_level()
        
        print("\n" + "="*70)
        print("All examples completed successfully! ✓")
        print("="*70)
        print("\nKey takeaways:")
        print("  • Use set_level() to quickly change verbosity")
        print("  • Use configure_logging() for full customization")
        print("  • Default level is INFO (shows INFO, WARNING, ERROR)")
        print("  • Logging levels: DEBUG < INFO < WARNING < ERROR < CRITICAL")
        print("  • Use DEBUG for troubleshooting, WARNING for production")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
        import traceback
        traceback.print_exc()
