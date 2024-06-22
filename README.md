# StreamDaQ
## Streaming Data Quality solutions in Python, using Faust and Bytewax

<p align="center">
    <img align="middle" src="Stream%20DaQ%20logo.png" alt="Stream Data Quality logo: a rubber duck and text"/>
</p>

In this repository you can find Python source code for Data Quality (DQ) solutions in streaming settings. 
At the current time, we provide support for statistical DQ checks in tumbling (i.e. non overlapping) time windows,
with the vision to enrich the functionality with more stream-meaningful checks shortly. The supported checks include
finding the following statistical values inside time-based windows:
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
    <img align="middle" src="faust/Resources/DQ%20monitoring%20dashboard/Figure_1.png" alt="Stream Data Quality dashboard"/>
</p>
