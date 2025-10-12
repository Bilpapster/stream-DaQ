from datetime import timedelta
from copy import deepcopy
from typing import Any, Callable, Optional, Self, Dict, Tuple

import pathway as pw
from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from streamdaq.artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial
from streamdaq.utils import create_comparison_function, extract_violation_count
from streamdaq.SchemaValidator import SchemaValidator
from streamdaq.CompactData import CompactData
from streamdaq.Task import Task, CriticalTaskFailureError


class StreamDaQ:
    """
    The fundamental class of the DQ monitoring system. An instance of this class is necessary and sufficient to
    perform the following actions/steps: \n
    1. Configure the monitoring details, such as the type and parameters of the monitoring window, the way late events
    are handled and the source/sink details. \n
    2. Define what **exactly** DQ means for your unique use-case, by adding DQ measurements of your data stream, from
    a palette of built-in, real-time measurements. \n
    3. Kick-off DQ monitoring of your data stream, letting Stream DaQ continuously watch out your data, while you
    focus on the important.
    """
    
    # Internal state keys for storing tasks assigned to this instance
    _TASKS_KEY: str = "TASKS"

    def __init__(self):
        from collections import OrderedDict
        
        # State management at the daq level
        self._DAQ_INTERNAL_STATE = None
        self.__initialize_state()

        # Task management
        self._default_task: Optional[Task] = None
        self._task_counter: int = 0
        
        # Backward compatibility - these will be moved to default task when configure() is called
        self.measures = OrderedDict()
        self.window = None
        self.window_behavior = None
        self.time_column = None
        self.instance = None
        self.wait_for_late = None
        self.time_format = None
        self.show_window_start = True
        self.show_window_end = True
        self.source = None
        self.sink_file_name = None
        self.sink_operation = None
        self.schema_validator = None
        self.compact_data = None
        
    @property
    def _tasks(self) -> Dict[str, Task]:
        return self._DAQ_INTERNAL_STATE[self._TASKS_KEY]
    
    def __initialize_state(self) -> None:
        # State management at the daq level
        self._DAQ_INTERNAL_STATE = dict()
        tasks: Dict[str, Task] = {}
        self._DAQ_INTERNAL_STATE[self._TASKS_KEY] = tasks
    
    def _add_task_to_state(self, task: Task, name: str) -> bool:
        if self._is_task_in_state(name):
            return False
        self._tasks[name] = task
        return True
    
    def _is_task_in_state(self, name: str) -> bool:
        return name in self._tasks

    def configure(
        self,
        window: Window,
        time_column: str,
        behavior: pw.temporal.CommonBehavior | pw.temporal.ExactlyOnceBehavior | None = None,
        instance: str | None = None,
        wait_for_late: int | float | timedelta | None = None,
        time_format: str = "%Y-%m-%d %H:%M:%S",
        show_window_start: bool = True,
        show_window_end: bool = True,
        source: pw.internals.Table | None = None,
        sink_file_name: str = None,
        sink_operation: Callable[[pw.internals.Table], None] | None = None,
        schema_validator: SchemaValidator | None = None,
        compact_data: CompactData | None = None,
    ) -> Self:
        """
        Configures the DQ monitoring parameters. For backward compatibility, this creates a default task.
        
        :param window: a window object to use for windowing the source stream
        :param time_column: the name of the column that contains the date/time information for every element
        :param behavior: the temporal behavior of the monitoring window; see pathways temporal behaviors for more
        :param instance: the name of the column that contains the key for each incoming element
        :param wait_for_late: the number of seconds to wait after the end timestamp of each window
        :param time_format: the format of the values in the column that contains date/time information
        :param show_window_start: boolean flag to specify whether the window starting timestamp should be included
        :param show_window_end: boolean flag to specify whether the window ending timestamp should be included
        :param source: the source to get data from
        :param sink_file_name: the name of the file to write the output to
        :param sink_operation: the operation to perform in order to send data out of Stream DaQ
        :param schema_validator: an optional schema validator to apply on the input data stream
        :param compact_data: an optional compact data configuration for working with compact data representations
        :return: a self reference for method chaining
        """
        # Create default task if it doesn't exist (backward compatibility)
        if self._default_task is None:
            self._default_task = self.new_task("default_task", critical=False)
        
        # Configure the default task
        self._default_task.configure(
            window=window,
            time_column=time_column,
            behavior=behavior,
            instance=instance,
            wait_for_late=wait_for_late,
            time_format=time_format,
            show_window_start=show_window_start,
            show_window_end=show_window_end,
            source=source,
            sink_file_name=sink_file_name,
            sink_operation=sink_operation,
            schema_validator=schema_validator,
            compact_data=compact_data,
        )
        
        # Also set the instance variables for backward compatibility
        self.window = window
        self.window_behavior = behavior
        self.instance = instance
        self.time_column = time_column
        self.wait_for_late = wait_for_late
        self.time_format = time_format
        self.show_window_start = show_window_start
        self.show_window_end = show_window_end
        self.source = source
        self.sink_file_name = sink_file_name
        self.sink_operation = sink_operation
        self.schema_validator = schema_validator
        self.compact_data = compact_data
        
        # Copy measures from default task for backward compatibility
        self.measures = self._default_task.task_output
        
        return self

    # TODO: rename to new_task and change all code occurences both here and in the examples
    def new_task(self, name: Optional[str] = None, critical: bool = False) -> Task:
        """
        Add a new monitoring task to this StreamDaQ instance.
        
        :param name: Optional **unique** name for the task. If None, auto-generates task_1, task_2, etc.
        :param critical: If True, failure of this task will stop all monitoring
        :return: The created Task instance for method chaining
        :raises ValueError: If the provided name is not unique
        """
        if name is None:
            name = self._generate_task_name()
        elif not self._validate_task_name(name):
            raise ValueError(f"Task name '{name}' already exists. Task names must be unique.")
        
        task = Task(name=name, critical=critical)
        self._add_task_to_state(task, name)
        # self._tasks[name] = task
        return task

    def _generate_task_name(self) -> str:
        """Generate a unique task name in the format task_1, task_2, etc."""
        while True:
            self._task_counter += 1
            name = f"task_{self._task_counter}"
            if not self._is_task_in_state:
            # if name not in self._tasks:
                return name

    def _validate_task_name(self, name: str) -> bool:
        """Validate that a task name is unique."""
        return not self._is_task_in_state(name)
        # return name not in self._tasks

    def get_task(self, name: str) -> Optional[Task]:
        """Get a task by name. Returns a deep copy of the task."""
        return deepcopy(self._tasks.get(name))

    def list_tasks(self) -> list[str]:
        """Get a list of all task names."""
        return list(self._tasks.keys())

    def remove_task(self, name: str) -> bool:
        """
        Remove a task by name.
        
        :param name: Name of the task to remove
        :return: True if task was removed (or non existent), else False
        """
        if name in self._tasks:
            del self._tasks[name]
            return True
        return False

    def add(
        self,
        measure: pw.ColumnExpression | ReducerExpression,
        assess: str | Callable[[Any], bool] | None = None,
        name: Optional[str] = None,
    ) -> Self:
        """
        Adds a DQ measurement to be monitored within the stream windows.
        
        **DEPRECATED**: Use check() instead for better clarity.
        This method is maintained for backward compatibility but it is not planed 
        to receive any functionality updates.
        
        :param measure: the measure to be monitored
        :param assess: the assessment mechanism to be applied on the measure
        :param name: the name with which the measure and assessment result will appear in the output
        :return: a self reference for method chaining
        """
        import warnings
        warnings.warn(
            "The add() method is deprecated. Use check() for better clarity. \
            Stream DaQ will keep maintaining the add() method, but no functionality updates \
            are planned. Please migrate to check(), which is as simple to use but much more \
            powerful and expressive.",
            DeprecationWarning,
            stacklevel=2
        )
        
        return self.check(measure, assess, name)

    def check(
        self,
        measure: pw.ColumnExpression | ReducerExpression,
        assess: str | Callable[[Any], bool] | None = None,
        name: Optional[str] = None,
    ) -> Self:
        """
        Add a data quality check to be monitored within the stream windows.
        For backward compatibility, this adds the check to the default task.
        
        :param measure: the measure to be monitored
        :param assess: the assessment mechanism to be applied on the measure
        :param name: the name with which the measure and assessment result will appear in the output
        :return: a self reference for method chaining
        """
        # Ensure default task exists
        if self._default_task is None:
            raise RuntimeError("No default task exists. Call configure() first or use add_task() to create tasks explicitly.")
        
        # Add check to default task
        self._default_task.check(measure, assess, name)
        
        # Update instance variables for backward compatibility
        self.measures = self._default_task.task_output
        
        return self

    def watch_out(self, start: bool = True) -> Optional[pw.Table]:
        """
        Kicks-off the data quality monitoring process for all configured tasks.

        **Important**: All previous configurations are just declaring the different steps of the pipeline.
        Calling `watch_out()` is essential for your declared pipeline to *actually run*.
        This happens when the `start` parameter is set to `True` (default).
        In this case, the function returns `None`.

        **Advanced users** may want to set the `start` parameter to `False`, in order to programmatically
        access the quality meta-stream. For multi-task scenarios, this returns the default task's
        quality meta-stream for backward compatibility.
        **Users must make sure that `pw.run()` is called** before the end of their script, otherwise the
        defined pipeline will not be executed.

        :return: Does not return (monitoring of unbounded stream) if `start` is set to
        `True` (default), else a `pw.Table` reference to the default task's quality
        meta-stream for backward compatibility.
        """
        # Handle empty task scenarios (backward compatibility)
        if not self._tasks:
            if self._default_task is None:
                raise RuntimeError("No tasks configured. Call `configure()` or `new_task()` first.")
        
        # Execute all tasks with proper error handling
        quality_meta_streams = []
        failed_tasks = []
        
        for task_name, task in self._tasks.items():
            try:
                print(f"Starting monitoring for task: {task_name}")
                quality_meta_stream = task.execute_monitoring_pipeline()
                quality_meta_streams.append(quality_meta_stream)
                print(f"Task '{task_name}' started successfully")
                
            except CriticalTaskFailureError as e:
                print(f"CRITICAL TASK FAILURE: {e}")
                print(f"Task '{task_name}' is marked as critical. Stopping all monitoring.")
                self._log_task_failure(task_name, e.original_error, critical=True)
                raise
                
            except Exception as e:
                # Handle non-critical task failures
                print(f"NON-CRITICAL TASK FAILURE: Task '{task_name}' failed with error: {e}")
                self._log_task_failure(task_name, e, critical=False)
                failed_tasks.append(task_name)
                print(f"Continuing with remaining tasks...")
        
        # Report summary of task execution
        if failed_tasks:
            print(f"Task execution summary: {len(failed_tasks)} non-critical tasks failed: {failed_tasks}")
            print(f"Successfully started {len(quality_meta_streams)} tasks")
        else:
            print(f"All {len(quality_meta_streams)} tasks started successfully")
        
        if not start:
            # For backward compatibility, return default task's quality meta-stream
            if self._default_task and quality_meta_streams:
                return quality_meta_streams[0] if quality_meta_streams else None
            return None

        # Start unified execution for all tasks
        pw.run()

    def _log_task_failure(self, task_name: str, error: Exception, critical: bool = False) -> None:
        """
        Log detailed information about task failures.
        
        :param task_name: Name of the failed task
        :param error: The exception that caused the failure
        :param critical: Whether this was a critical task failure
        """
        import traceback
        import datetime
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        severity = "CRITICAL" if critical else "WARNING"
        
        print(f"\n{'='*60}")
        print(f"TASK FAILURE REPORT - {severity}")
        print(f"{'='*60}")
        print(f"Timestamp: {timestamp}")
        print(f"Task Name: {task_name}")
        print(f"Critical: {'Yes' if critical else 'No'}")
        print(f"Error Type: {type(error).__name__}")
        print(f"Error Message: {str(error)}")
        print(f"Traceback:")
        print(traceback.format_exc())
        print(f"{'='*60}\n")

    def get_task_status(self) -> dict:
        """
        Get the current status of all tasks.
        
        :return: Dictionary with task names as keys and status info as values
        """
        status = {
            "total_tasks": len(self._tasks),
            "tasks": {}
        }
        
        for task_name, task in self._tasks.items():
            print("skata")
            print(task_name, task)
            status["tasks"][task_name] = {
                "name": task_name,
                "critical": task.critical,
                "configured": task.window is not None and task.time_column is not None,
                "has_source": task.source is not None,
                "has_checks": len(task._checks) > 0
            }
        
        return status

    def get_output_configuration(self) -> dict:
        """
        Get the output configuration for all tasks.
        
        :return: Dictionary showing sink configuration for each task
        """
        output_config = {}
        
        for task_name, task in self._tasks.items():
            output_config[task_name] = {
                "show_window_start": task.show_window_start,
                "show_window_end": task.show_window_end,
                "sink_operation": "Custom" if task.sink_operation else "Console (default)",
                "sink_file_name": task.sink_file_name or "None",
                "schema_validation": "Yes" if task.schema_validator else "No",
                "violations_deflection": "Yes" if (
                    task.schema_validator and 
                    task.schema_validator.settings().deflect_violating_records
                ) else "No"
            }
        
        return output_config
