🔧 Installation Guide
=====================

This guide covers everything you need to install and set up Stream DaQ in different environments.

Basic Installation
------------------

Stream DaQ is available on PyPI and can be installed with pip:

.. code-block:: bash

    pip install streamdaq

.. admonition:: Requirements
   :class: note

   - **Python**: 3.11 or higher
   - **Operating System**: Windows, macOS, or Linux

Verify Your Installation
------------------------

Test that Stream DaQ is properly installed:

.. code-block:: python

    import streamdaq
    print(f"Stream DaQ version: {streamdaq.__version__}")
    print("✅ Installation successful!")

Development Installation
------------------------

If you want to contribute to Stream DaQ or use the latest development version:

.. code-block:: bash

    # Clone the repository
    git clone https://github.com/bilpapster/stream-DaQ.git
    cd stream-DaQ

    # Install in editable mode
    pip install -e .

    # Install development dependencies
    pip install -r requirements-dev.txt

Virtual Environment Setup
--------------------------

We recommend using a virtual environment to avoid dependency conflicts:

.. tab-set::

    .. tab-item:: venv (Built-in)

        .. code-block:: bash

            # Create virtual environment
            python -m venv streamdaq_env

            # Activate it
            # On Windows:
            streamdaq_env\Scripts\activate
            # On macOS/Linux:
            source streamdaq_env/bin/activate

            # Install Stream DaQ
            pip install streamdaq

    .. tab-item:: conda

        .. code-block:: bash

            # Create conda environment
            conda create -n streamdaq python=3.11
            conda activate streamdaq

            # Install Stream DaQ
            pip install streamdaq

    .. tab-item:: Poetry

        .. code-block:: bash

            # Initialize Poetry project
            poetry init
            poetry add streamdaq

            # Activate shell
            poetry shell

Docker Installation
-------------------

Run Stream DaQ in a Docker container:

.. code-block:: dockerfile

    FROM python:3.11-slim

    WORKDIR /app

    # Install Stream DaQ
    RUN pip install streamdaq

    # Copy your monitoring scripts
    COPY . .

    CMD ["python", "your_monitoring_script.py"]

Build and run:

.. code-block:: bash

    docker build -t my-streamdaq-app .
    docker run my-streamdaq-app

Troubleshooting
---------------

Common Installation Issues
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Issue**: ``ModuleNotFoundError: No module named 'streamdaq'``

.. code-block:: text

    Solution: Make sure you've activated the correct virtual environment
    and that the installation completed successfully.

**Issue**: ``Python version compatibility error``

.. code-block:: text

    Solution: Stream DaQ requires Python 3.11+. Check your version:
    python --version

**Issue**: ``Permission denied during installation``

.. code-block:: text

    Solution: Use --user flag or virtual environment:
    pip install --user streamdaq

**Issue**: ``Network/firewall blocking PyPI access``

.. code-block:: text

    Solution: Configure pip to use your corporate proxy:
    pip install --proxy http://your-proxy:port streamdaq

Performance Considerations
^^^^^^^^^^^^^^^^^^^^^^^^^^

For high-volume streams (>10,000 events/second):

.. code-block:: bash

    # Install with performance optimizations
    pip install streamdaq[performance]

Memory usage guidelines:

- **Small streams** (<1,000 events/sec): 256MB RAM
- **Medium streams** (1,000-10,000 events/sec): 512MB RAM  
- **Large streams** (>10,000 events/sec): 1GB+ RAM

Testing Your Setup
------------------

Run this comprehensive test to ensure everything works:

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    import pandas as pd
    from datetime import datetime

    def test_streamdaq_installation():
        """Test basic Stream DaQ functionality"""
        try:
            # Create test data
            test_data = pd.DataFrame({
                'value': [1, 2, 3, 4, 5],
                'timestamp': pd.date_range('2024-01-01', periods=5, freq='1S'),
                'group_id': ['A', 'A', 'B', 'B', 'A']
            })

            # Set up monitor
            daq = StreamDaQ().configure(
                window=Windows.tumbling(3),
                instance="group_id",
                time_column="timestamp"
            )

            # Add a simple check
            daq.add(dqm.count('value'), assess=">0", name="has_data")

            # Test monitoring
            results = daq.watch_out(test_data)
            
            print("✅ Stream DaQ installation test passed!")
            print(f"✅ Processed {len(test_data)} test records")
            print("✅ Ready for real-time monitoring!")
            
            return True
            
        except Exception as e:
            print(f"❌ Installation test failed: {e}")
            return False

    if __name__ == "__main__":
        test_streamdaq_installation()

IDE Integration
---------------

**VS Code**: Install the Python extension and configure your interpreter to point to your virtual environment.

**PyCharm**: Add your virtual environment as a project interpreter in Settings > Project > Python Interpreter.

**Jupyter**: Install in your environment and register the kernel:

.. code-block:: bash

    pip install jupyter
    python -m ipykernel install --user --name=streamdaq

Next Steps
----------

Now that Stream DaQ is installed:

- 🚀 **Try the quickstart**: :doc:`quickstart` - See it working in 5 minutes
- 🎯 **Build your first monitor**: :doc:`first-monitoring` - Step-by-step tutorial
- 📚 **Learn the concepts**: :doc:`../concepts/index` - Understand the fundamentals

Need Help?
----------

- 🐛 **Report installation issues**: `GitHub Issues <https://github.com/bilpapster/stream-DaQ/issues>`_
- 💬 **Ask questions**: `GitHub Discussions <https://github.com/bilpapster/stream-DaQ/discussions>`_
- 📧 **Contact support**: papster@csd.auth.gr