# Stream DaQ

Remember the joy of bath time with those trusty rubber ducks, keeping us company while floating through the bubbles? Well, think of **Stream DaQ** as the duck for your data — keeping your streaming data clean and afloat in a sea of information. Just like those bath ducks helped make our playtime fun and carefree, Stream DaQ keeps an eye on your data and lets you know the moment things get messy, so you can take action ***in real time***!

<p align="center">
    <img align="middle" src="Stream%20DaQ%20logo.png" alt="Stream Data Quality logo: a rubber duck and text"/>
</p>

### The project

**Stream DaQ** is originally developed in Python, leveraging internally the [Pathway](https://github.com/pathwaycom/pathway) stream processing library, which is an open source project, as well. Previous versions of the project were featuring more Python stream processing libraries, namely [Faust](https://faust-streaming.github.io/faust/) and [Bytewax](https://bytewax.io/). You can find source code using these frameworks in the `faust-vs-bytewax` branch of the repository. Comparisons between the two libraries are also available there. Our immediate plans is to extend the functionality of Stream DaQ primarily in Pathway. The latest advancements of the tool will always be available in the `main` branch (you are here).

The project is developed by the Data Engineering Team (DELAB) of [Datalab AUTh](https://datalab.csd.auth.gr/), under the supervision of [Prof. Anastasios Gounaris](https://datalab-old.csd.auth.gr/~gounaris/).

### Key functionality

**Stream DaQ** keeps an eye on your data stream, letting you know when travelling data are not as expected. In **real time**. So that you can take actions. There are several key aspects of the tool making it a powerful option for data quality monitoring on data streams:
1. *Highly configurable*: Stream DaQ comes with plenty of built-in data quality measurements, so that you can choose which of them fit your use case. We know that every data-centric application is different, so being able to **define** what "data quality" means for you is precious.
2. *Real time alerts*: Stream DaQ defines highly meaningful data quality checks for data streams, letting the check results be a stream on their own, as well. This architectural choice enables real time alerts, in case the standards or thresholds you have defined are not met! 


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
and then follow the following steps:

1. Starting from the root folder of the project, go to the `pathway` directory.
   ```bash
   cd pathway
   # all the commands from now on should be executed in this directory
   ```
1. Run the `statistics_manager` file
    ```bash
    python statistics_manager.py
    ```

## Work in progress

The project is in full development at current time, so more functionalities, documentation, examples and demonstrations are on their way to be included shortly. We thank you for your patience.

## Acknowledgements

Special thanks to [Maria Kavouridou](https://www.linkedin.com/in/maria-kavouridou/) for putting effort and love, in order to give birth to the StreamDaQ logo.

