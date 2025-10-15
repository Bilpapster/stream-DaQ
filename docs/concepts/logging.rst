Logging Configuration
=====================

Stream DaQ provides a centralized logging system that allows you to control the verbosity of output messages throughout the library. This helps you customize the logging behavior based on your needs, whether you're debugging, running in production, or just want to see what's happening under the hood.

Overview
--------

The logging system in Stream DaQ:

* Uses Python's standard ``logging`` module
* Provides configurable verbosity levels
* Unifies logging across all modules (including schema validation)
* Replaces scattered ``print()`` statements with structured logging
* Allows custom formatting and handlers

Logging Levels
--------------

Stream DaQ uses Python's standard logging levels:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Level
     - Description
   * - ``DEBUG``
     - Detailed information, typically for diagnosing problems. Most verbose.
   * - ``INFO``
     - Confirmation that things are working as expected. **Default level**.
   * - ``WARNING``
     - An indication that something unexpected happened, but the software is still working.
   * - ``ERROR``
     - A more serious problem; the software has not been able to perform some function.
   * - ``CRITICAL``
     - A serious error; the program itself may be unable to continue running.

The default logging level is ``INFO``, which shows informational messages, warnings, and errors, but hides debug messages.

Quick Start
-----------

Basic Usage
^^^^^^^^^^^

By default, Stream DaQ logs at the ``INFO`` level:

.. code-block:: python

   from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
   
   # Default INFO level logging is active
   daq = StreamDaQ().configure(
       window=Windows.tumbling(3),
       instance="user_id",
       time_column="timestamp",
       wait_for_late=1,
       time_format='%Y-%m-%d %H:%M:%S'
   )
   # Output: [INFO] streamdaq.StreamDaQ: Data set to artificial and data representation to native.

Changing the Logging Level
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``set_level()`` to quickly change the verbosity:

.. code-block:: python

   from streamdaq import set_level
   import logging
   
   # Increase verbosity for debugging
   set_level(logging.DEBUG)
   
   # Or reduce verbosity for production
   set_level(logging.WARNING)
   
   # You can also use string names
   set_level("ERROR")

Common Scenarios
----------------

Development and Debugging
^^^^^^^^^^^^^^^^^^^^^^^^^

When developing or debugging, you may want to see all messages:

.. code-block:: python

   from streamdaq import StreamDaQ, set_level
   import logging
   
   # Set to DEBUG to see everything
   set_level(logging.DEBUG)
   
   # Now you'll see detailed debug messages
   daq = StreamDaQ().configure(...)
   # Output will include DEBUG messages from all Stream DaQ modules

Production Deployment
^^^^^^^^^^^^^^^^^^^^^

In production, you typically want minimal output:

.. code-block:: python

   from streamdaq import StreamDaQ, set_level
   import logging
   
   # Set to WARNING to see only warnings and errors
   set_level(logging.WARNING)
   
   # Only warnings and errors will be shown
   daq = StreamDaQ().configure(...)
   # INFO messages like "Data set to artificial..." will not appear

Quiet Mode
^^^^^^^^^^

To suppress all but critical errors:

.. code-block:: python

   from streamdaq import set_level
   import logging
   
   set_level(logging.ERROR)
   # Only errors and critical messages will be shown

Advanced Configuration
----------------------

Custom Formatting
^^^^^^^^^^^^^^^^^

Use ``configure_logging()`` for full control over log formatting:

.. code-block:: python

   from streamdaq import configure_logging
   import logging
   
   # Custom format with timestamps
   configure_logging(
       level=logging.INFO,
       format_string="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
       date_format="%Y-%m-%d %H:%M:%S"
   )
   
   # Output: 2025-10-15 14:30:45 - INFO - streamdaq.StreamDaQ - Data set to artificial...

Minimal Format
^^^^^^^^^^^^^^

For cleaner output:

.. code-block:: python

   from streamdaq import configure_logging
   import logging
   
   configure_logging(
       level=logging.INFO,
       format_string="[%(levelname)s] %(message)s"
   )
   
   # Output: [INFO] Data set to artificial and data representation to native.

Custom Handlers
^^^^^^^^^^^^^^^

For advanced use cases, you can provide custom logging handlers:

.. code-block:: python

   import logging
   from logging.handlers import RotatingFileHandler
   from streamdaq import configure_logging
   
   # Create a file handler
   file_handler = RotatingFileHandler(
       'streamdaq.log',
       maxBytes=10485760,  # 10MB
       backupCount=5
   )
   file_handler.setFormatter(
       logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
   )
   
   # Configure Stream DaQ to use the file handler
   configure_logging(
       level=logging.INFO,
       handlers=[file_handler]
   )

Checking Current Level
^^^^^^^^^^^^^^^^^^^^^^

You can check the current logging level:

.. code-block:: python

   from streamdaq import get_current_level
   import logging
   
   level = get_current_level()
   print(f"Current level: {logging.getLevelName(level)}")
   # Output: Current level: INFO

API Reference
-------------

.. py:function:: configure_logging(level=logging.INFO, format_string=None, date_format=None, handlers=None)

   Configure logging for the Stream DaQ library.
   
   :param level: The logging level (int or str). Default is ``logging.INFO``.
   :type level: int or str
   :param format_string: Custom format string for log messages. If None, uses a default format.
   :type format_string: str, optional
   :param date_format: Custom date format for log messages. If None, uses ISO 8601 format.
   :type date_format: str, optional
   :param handlers: List of custom logging handlers. If None, uses a StreamHandler writing to stderr.
   :type handlers: list, optional
   
   **Example:**
   
   .. code-block:: python
   
      from streamdaq import configure_logging
      import logging
      
      configure_logging(
          level=logging.DEBUG,
          format_string="%(levelname)s: %(message)s"
      )

.. py:function:: set_level(level)

   Set the logging level for Stream DaQ.
   
   This is a convenience function that updates the logging level without changing other settings.
   
   :param level: The logging level (int or str).
   :type level: int or str
   
   **Example:**
   
   .. code-block:: python
   
      from streamdaq import set_level
      import logging
      
      # Using integer constant
      set_level(logging.DEBUG)
      
      # Using string
      set_level("WARNING")

.. py:function:: get_current_level()

   Get the current logging level.
   
   :returns: The current logging level as an integer.
   :rtype: int
   
   **Example:**
   
   .. code-block:: python
   
      from streamdaq import get_current_level
      import logging
      
      level = get_current_level()
      print(logging.getLevelName(level))

Best Practices
--------------

1. **Set logging level early**: Configure logging at the start of your application, before creating any Stream DaQ objects.

2. **Use appropriate levels**:
   
   * Development: ``DEBUG`` or ``INFO``
   * Testing: ``INFO``
   * Production: ``WARNING`` or ``ERROR``

3. **Keep it simple**: Use ``set_level()`` for most cases; only use ``configure_logging()`` when you need custom formatting or handlers.

4. **Environment-based configuration**: Consider setting the logging level based on environment variables:

   .. code-block:: python
   
      import os
      import logging
      from streamdaq import set_level
      
      # Set level based on environment
      env = os.getenv('ENVIRONMENT', 'development')
      if env == 'production':
          set_level(logging.WARNING)
      else:
          set_level(logging.DEBUG)

5. **Don't suppress errors**: Avoid setting the level to ``CRITICAL`` as this may hide important error messages.

Examples
--------

For complete examples of logging configuration, see:

* ``examples/logging_configuration.py`` - Demonstrates various logging configurations
* ``examples/schema_validation.py`` - Shows logging with schema validation

Troubleshooting
---------------

No log messages appearing
^^^^^^^^^^^^^^^^^^^^^^^^^

If you don't see any log messages:

1. Check that the logging level is appropriate:

   .. code-block:: python
   
      from streamdaq import get_current_level
      import logging
      print(logging.getLevelName(get_current_level()))

2. Ensure you're looking at stderr (where logs are written by default), not stdout.

Too many log messages
^^^^^^^^^^^^^^^^^^^^^

If you're seeing too much output:

1. Increase the logging level to show fewer messages:

   .. code-block:: python
   
      from streamdaq import set_level
      import logging
      set_level(logging.WARNING)

Messages appearing multiple times
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This should not happen with Stream DaQ's logging system. If you see duplicate messages:

1. Check that you haven't configured logging multiple times
2. Ensure you're not mixing Stream DaQ's logging with manual ``logging.basicConfig()`` calls

Migration from print statements
--------------------------------

Previous versions of Stream DaQ used ``print()`` statements for output. If you were relying on these:

* ``print()`` statements have been replaced with appropriate logging calls
* The default logging level (``INFO``) shows similar information to what was previously printed
* You can reduce verbosity with ``set_level(logging.WARNING)`` or increase it with ``set_level(logging.DEBUG)``

No changes to your existing code are required, but you can now control the output with the logging configuration functions.
