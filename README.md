<p align="center">
  <img height="400px" src="https://github.com/user-attachments/assets/a42a440d-d61c-4cf8-aff0-092209aea052" alt="Stream DaQ logo">
</p>

<p align="center">
  <a href="https://pypi.org/project/streamdaq/"><img src="https://img.shields.io/pypi/v/streamdaq?label=release&color=blue&" alt="PyPI version"></a>
  <a href="https://pypi.org/project/streamdaq/"><img src="https://img.shields.io/pypi/pyversions/streamdaq.svg" alt="Python versions"></a>
  <a href="https://pepy.tech/project/streamdaq"><img src="https://pepy.tech/badge/streamdaq" alt="Downloads"></a>
  <a href="https://bilpapster.github.io/stream-DaQ/"><img src="https://img.shields.io/website?label=docs&url=https%3A%2F%2Fbilpapster.github.io/stream-DaQ%2F" alt="Documentation"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
</p>

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
daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact")
    .add(dqm.most_frequent('interaction_events'), assess=check_most_frequent_items, name="freq_interact")
    .add(dqm.distinct_count_approx('interaction_events'), assess="==9", name="approx_dist_interact")

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
```
More examples can be found in the [examples directory](https://github.com/Bilpapster/stream-DaQ/tree/main/examples) of the project. Even more examples are on their way to be integrated shortly! Thank you for your patience!

## Motivation
Remember the joy of bath time with those trusty rubber ducks, keeping us company while floating through the bubbles?
Well, think of **Stream DaQ** as the duck for your data — keeping your streaming data clean and afloat in a sea of
information. Just like those bath ducks helped make our playtime fun and carefree, Stream DaQ keeps an eye on your data
and lets you know the moment things get messy, so you can take action ***in real time***!


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
3. *Configurable logging*: Stream DaQ provides a unified logging system with adjustable verbosity levels, allowing you 
   to control output from detailed debugging to minimal production logs. See [logging configuration examples](examples/logging_configuration.py).

## Stream DaQ's architecture

![StreamDaQ architecture animation](https://github.com/user-attachments/assets/d57377c9-d0fc-4a8e-8346-d01204da09a2)

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
# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


def is_seven_frequent(most_frequent: int | float | tuple) -> bool:
    from collections.abc import Iterable

    # if most frequent items are more than one, check for set membership
    if isinstance(most_frequent, Iterable):
        return 7 in most_frequent

    # if there is a single most frequent item, check just for equality
    return most_frequent == 7


daq = StreamDaQ().configure(
    window=Windows.tumbling(3),
    instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
    .add(dqm.most_frequent('interaction_events'), assess=is_seven_frequent, name="freq_interact")
    .add(dqm.distinct_count_approx('interaction_events'), assess="==9", name="approx_dist_interact")

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
```

In this simple example, we define three different data quality checks:

- the number of elements (`count`): we expect them to be in the range `(5, 15]`;
- the most frequent values (`most_frequent`): we expect number 7 to be frequent;
- the number of distinct values (`number_of_distinct_approx`): we expect the distict values to be exactly 9 (`==9`).

These checks are monitored in real time for every stream window. Windows can be tumbling, sliding, or session-based and are 
a fundamental notion of the *Stream DaQ* ecosystem. That's why *Stream DaQ* gives you full control on configuring windows
that are suitable for your use-case!

Based on your desired window settings, Stream DaQ takes over to continuously monitor (`watch_out`) the stream, as new data arrives.
The monitoring results are reported in real time, as a meta stream. That is, every row of the result is a new stream
object itself, as following:

```markdown
user_id | window_start | window_end   | count       | max_interact | med_interact | freq_interact   
UserA   | 1744195038.0 | 1744195041.0 | (14, True)  | (10, True)   | (6.5, True)  | (6, False)      
UserA   | 1744195041.0 | 1744195044.0 | (16, False) | (10, True)   | (5.0, True)  | (7, True)      
UserB   | 1744195044.0 | 1744195047.0 | (16, False) | (10, True)   | (7.0, True)  | (9, False)      
UserB   | 1744195047.0 | 1744195050.0 | (8, True)   | (8, True)    | (5.0, True)  | ((2, 7), True)
```

The above checks are just a small subset of the large amount of built-in, plug-and-play data quality validations *Stream DaQ* comes with. A detailed 
list of all the available checks will be included shortly.

## Execution

Just install the Python library (Python >= 3.11) and ... happy quacklitying!

   ```bash
    pip install streamdaq
   ```

## Work in progress

The project is in full development at current time, so more functionalities, documentation, examples and demonstrations
are on their way to be included shortly. We thank you for your patience.

## Acknowledgements

Special thanks to [Maria Kavouridou](https://www.linkedin.com/in/maria-kavouridou/) for putting effort and love, in
order to give birth to the Stream DaQ logo.

