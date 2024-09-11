# StreamDaQ
## Streaming Data Quality solutions in Python, using Faust and Bytewax

### Important: you are viewing a legacy branch, containing a comparison between Faust and Bytewax stream processing frameworks. The branch is not actively maintained at current time with no vision for this to change in the future.

#### In order to view the latest state of the project, please visit main branch from the menu in top left.

<p align="center">
    <img align="middle" src="Stream%20DaQ%20logo.png" alt="Stream Data Quality logo: a rubber duck and text"/>
</p>

In this repository you can find Python source code for Data Quality (DQ) solutions in streaming settings. We provide support for statistical DQ checks in tumbling (i.e. non overlapping) time windows, although the interested developer can easily adapt the type of the windows in all cases with little effort. The supported checks include finding the following statistical values inside time-based windows:
- maximum value
- minimum value
- mean of the values
- number of elements (count)
- number of distinct elements

In order to accomplish the above functionality, we leverage two different Python modules for stream processing:
[Faust](https://faust-streaming.github.io/faust/) and [Bytewax](https://bytewax.io/). We implement the above DQ checks 
in both modules, adapting our solutions to the specific features each one of them offers. Finally, we visualize results
in a lively-updated dashboard, enabling DQ monitoring over unbounded streams.

<p align="center">
    <img align="middle" src="dq_dashboard/dq_dashboard.gif" alt="Stream Data Quality dashboard animation"/>
</p>

## Execution

The easiest way to run the code in this repository is to create a new conda environment and install the required
packages. To do so, execute the following commands in a terminal:
```bash
conda env create --file environment.yml
conda activate daq
pip install -r requirements.txt
```

The above three commands are required only the *first* time you run the code. For every next run, simply activate
the conda environment `daq`:
```bash
conda activate daq
```
and then insert the commands you find below, depending on the streaming module you are interested in using.

### Faust

Requirements: 
- Kafka 2.13-3.7 or newer
- Faust 0.11.0 or newer

In order to run the Faust statistical manager and simulate a data stream do the following:

1. Start Kafka Zookeeper and Kafka Server:
    ```bash
    # inside your Kafka installation folder
    
    # open Kafka Zookeeper in a terminal
    bin/zookeeper-server-start.sh config/zookeeper.properties
    
    # open Kafka Server in a NEW terminal
    bin/kafka-server-start.sh config/server.properties
    ```
1. Go to the `faust` directory of the project
   ```bash
   cd faust
   # all the commands from now on should be executed in this directory
   ```

1. Start a Faust `statistical_manager` worker. This worker will be responsible for processing the stream of events and
implement the DQ logic on it.
    ```bash
    python statistics_manager.py worker -l info
    ```

1. Start a Faust `data_generator` worker. This worker will be responsible for generating data in a Kafka topic, in order
to simulate a stream. The worker will wait for some random seconds before sending the next event, to better simulate the
stream. IMPORTANT: the worker needs to be initialized in a **new terminal**.
    ```bash
    # open a new terminal to run the following command
    python data_generator.py worker -l info --web-port 6067
    ```

1. (Optionally) Visualize the live results!
   ```bash
   python live_plotter.py
   ```
***
### Bytewax
1. Starting from the root folder of the project, go to the `bytewax` directory.
   ```bash
   cd bytewax
   # all the commands from now on should be executed in this directory
   ```

1. Run the `statistics_manager` file with the help of the Bytewax module. Specify that the name of the 
dataflow that is to be run is `flow`.
    ```bash
    python -m bytewax.run statistics_manager:flow
    ```
   
1. (Optionally) Visualize the live results!
   ```bash
   python live_plotter.py
   ```

## Acknowledgements

Special thanks to [Maria Kavouridou](https://www.linkedin.com/in/maria-kavouridou/) for putting effort and love, in order to give birth to the StreamDaQ logo.

