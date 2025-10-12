🚨 Common Issues
=================

Quick solutions to common Stream DaQ problems.

Installation Issues
-------------------

**Problem**: ``pip install streamdaq`` fails

**Solutions**:
- Ensure Python >= 3.11: ``python --version``
- Update pip: ``pip install --upgrade pip``
- Use virtual environment: ``python -m venv venv && source venv/bin/activate``

Configuration Problems
------------------------

**Problem**: "No data in windows"

**Solutions**:
- Check time column format matches your data
- Verify data source is producing records
- Ensure window size matches data frequency

**Problem**: High memory usage

**Solutions**:
- Reduce ``wait_for_late`` parameter
- Use larger window sizes for high-volume streams
- Check for data leaks in custom functions

Quality Check Issues
----------------------

**Problem**: Assessments always fail/pass

**Solutions**:
- Verify threshold values against actual data
- Test with known good/bad data samples
- Use debug output to see measure values

**Problem**: Custom assessment functions are slow

**Solutions**:
- Simplify assessment logic
- Use string assessments when possible
- Cache expensive calculations

Output Problems
-----------------

**Problem**: Results not appearing

**Solutions**:
- Check sink operation configuration
- Verify destination permissions and connectivity
- Test with console output first

**Problem**: Delayed results

**Solutions**:
- Reduce window size
- Decrease ``wait_for_late`` tolerance
- Use sliding windows instead of tumbling

Multi-Source Issues
--------------------

**Problem**: Tasks not running simultaneously

**Solutions**:
- Use single StreamDaQ instance with multiple tasks
- Check task criticality settings
- Verify all tasks are properly configured

**Problem**: Critical task failures stop everything

**Solutions**:
- Review which tasks are marked as critical
- Use try-catch blocks for error handling
- Consider making non-essential tasks non-critical

Getting Help
------------

1. Check the :doc:`../user-guide/index` for detailed configuration
2. Review :doc:`../examples/index` for working examples
3. Search existing issues on GitHub
4. Create a new issue with minimal reproduction case