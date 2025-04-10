# Stream DaQ

## TL; DR
```python
# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from some_existing_workflow import check_most_frequent_items

daq = StreamDaQ().configure(
    window=Windows.tumbling(3),
    instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
    .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
    .add(dqm.most_frequent('interaction_events'), assess=check_most_frequent_items, name="freq_interact") \
    .add(dqm.number_of_distinct_approx('interaction_events'), assess="==9", name="approx_dist_interact")

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
```
More examples can be found in the [examples directory](https://github.com/Bilpapster/stream-DaQ/tree/main/examples) of the project. Even more examples are on their way to be integrated shortly! Thank you for your patience! 


Remember the joy of bath time with those trusty rubber ducks, keeping us company while floating through the bubbles?
Well, think of **Stream DaQ** as the duck for your data — keeping your streaming data clean and afloat in a sea of
information. Just like those bath ducks helped make our playtime fun and carefree, Stream DaQ keeps an eye on your data
and lets you know the moment things get messy, so you can take action ***in real time***!

<p align="center">
    <img align="middle" src="Stream%20DaQ%20logo.png" alt="Stream Data Quality logo: a rubber duck and text"/>
</p>

## The project

**Stream DaQ** is originally developed in Python, leveraging internally
the [Pathway](https://github.com/pathwaycom/pathway) stream processing library, which is an open source project, as
well. Previous versions of the project were featuring more Python stream processing libraries,
namely [Faust](https://faust-streaming.github.io/faust/) and [Bytewax](https://bytewax.io/). You can find source code
using these frameworks in the `faust-vs-bytewax` branch of the repository. Comparisons between the two libraries are
also available there. Our immediate plans is to extend the functionality of Stream DaQ primarily in Pathway. The latest
advancements of the tool will always be available in the `main` branch (you are here).

The project is developed by the Data Engineering Team (DELAB) of [Datalab AUTh](https://datalab.csd.auth.gr/), under the
supervision of [Prof. Anastasios Gounaris](https://datalab-old.csd.auth.gr/~gounaris/).

## Key functionality

**Stream DaQ** keeps an eye on your data stream, letting you know when travelling data are not as expected. In **real
time**. So that you can take actions. There are several key aspects of the tool making it a powerful option for data
quality monitoring on data streams:

1. *Highly configurable*: Stream DaQ comes with plenty of built-in data quality measurements, so that you can choose
   which of them fit your use case. We know that every data-centric application is different, so being able to **define
   ** what "data quality" means for you is precious.
2. *Real time alerts*: Stream DaQ defines highly meaningful data quality checks for data streams, letting the check
   results be a stream on their own, as well. This architectural choice enables real time alerts, in case the standards
   or thresholds you have defined are not met!

## Stream DaQ's architecture

![StreamDaQ architecture animation](https://github.com/user-attachments/assets/df59f529-c74d-425e-a19f-66a95482c716)

## Current Suite of Measurements
The following list provides a brief overview of the current Suite of Measurements supported by **Stream DaQ**. Note that for all the functionalities below, **Stream DaQ** comes with a wide range of _implementation variations_, in order to cover a broad and heterogeneous spectrum of DQ needs, depending on the domain, the task and the system at hand. For all these functionalities, **Stream DaQ** lets room for extensive customization via custom thresholding and variation selection with only a couple lines of code. Stay tuned, since new functionalities are on their way to be incorporated into **Stream DaQ**! Since then, Hapy Qua(ck)litying!

- **Descriptive statistics**: min, max, mean, median, std, number/fraction above mean
- **Availability**: at least a value in a window
- **Frozen stream detection**: all/most values are the same in a window
- **Range/Set validation**: values fall within a range or set of accepted values
- **Regex validation**: values comply with a privded regex
- **Unordered stream detection**: elements violate (time) ordering
- **Volume & Cardinalities**: count, distinct, unique, most_frequent, heavy hitters (approx)
- **Distribution percentiles**
- **Correlation analysis** between fields
- **Value Lengths**: {min, max, mean, median} length (_String-specific_)
- **Integer - Fractional parts**: {min, max, mean, median} integer or fractional part (_Float-specific_)

## Example code

Data quality monitoring is a highly case-specific task. We feel you! That's why we have tried to make it easier than
ever to define what data quality means to your *very own case*. You just need a couple of lines to define what you want
to be monitored and that's it! Sit back, and focus on the important stuff; the **real time** results. Stream DaQ
reliably handles all the rest for you. See it in a toy example!

```python
from StreamDaQ import StreamDaQ
from DaQMeasures import DaQMeasures as dqm
from Windows import tumbling
from datetime import timedelta

# Step 1: Configure monitoring parameters
daq = StreamDaQ().configure(
    window=tumbling(duration=timedelta(hours=1)),
    wait_for_late=timedelta(seconds=10),
    time_format="%H:%M:%S"
)

# Step 2: Define what data quality means for your unique use-case
daq.add(dqm.count('items'), "count") \
    .add(dqm.min('items'), "min") \
    .add(dqm.median('items'), "std") \
    .add(dqm.most_frequent('items'), "most_frequent") \
    .add(dqm.number_of_distinct('items'), "distinct")

# Step 3: Kick-off DQ monitoring
daq.watch_out()
```

In this simple example, we define four different measurements:

- the number of elements (`count`)
- their minimum (`min`)
- their median (`median`)
- their standard deviation (`std_dev`)
- the most frequent values (`most_frequent`)
- the number of distinct values (`number_of_distinct`)

The above data quality measures are monitored and reported in real time for every window of the stream.
After defining them, Stream DaQ takes over to continuously monitor (`watch_out`) the stream, as new data arrive.
The monitoring results are reported in real time, as a meta stream. That is, every row of the result is a new stream
object itself, as following:

```markdown
window_start | count | min | median | most_frequent | distinct
18:06:40     | 12    | 1   | 5.5    | (9, 1)        | 7
18:07:00     | 3     | 4   | 7      | (4, 7)        | 3
18:07:20     | 17    | 1   | 5      | 3             | 10
18:07:40     | 9     | 1   | 3      | (3, 1)        | 7
  .             .      .     .           .            .
  .             .      .     .           .            .
  .             .      .     .           .            .
```

The above measurements are just a small subset of the large amount of built-in measurements ready-for-use. A detailed 
list of all the available measurements will be included shortly.

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
1. Run the `main` file
    ```bash
    python main.py
    ```

## Work in progress

The project is in full development at current time, so more functionalities, documentation, examples and demonstrations
are on their way to be included shortly. We thank you for your patience.

## Acknowledgements

Special thanks to [Maria Kavouridou](https://www.linkedin.com/in/maria-kavouridou/) for putting effort and love, in
order to give birth to the Stream DaQ logo.

