import pathway as pw
from datasketch import HyperLogLogPlusPlus
# todo check if this implementation of HLL++ sketching is more reliable (Apache) https://apache.github.io/datasketches-python/5.0.2/distinct_counting/hyper_log_log.html


class StdDevReducer(pw.BaseCustomAccumulator):
    def __init__(self, count, sum, sum_squares):
        self.count = count
        self.sum = sum
        self.sum_squares = sum_squares

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(1, value, value ** 2)

    def update(self, other):
        self.count += other.count
        self.sum += other.sum
        self.sum_squares += other.sum_squares

    def compute_result(self) -> float:
        mean = self.sum / self.count
        mean_squares = self.sum_squares / self.count
        return mean_squares - mean ** 2

    def retract(self, other):
        self.count -= other.count
        self.sum -= other.sum
        self.sum_squares -= other.sum_squares


std_dev_reducer = pw.reducers.udf_reducer(StdDevReducer)


class ApproxDistinctReducer(pw.BaseCustomAccumulator):

    def __init__(self, element: str):
        self.hpp_sketch = HyperLogLogPlusPlus()
        self.hpp_sketch.update(element.encode('utf-8'))

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(str(value))

    def update(self, other):
        self.hpp_sketch.merge(other.hpp_sketch)

    def compute_result(self) -> float:
        return self.hpp_sketch.count()


approx_distinct_count_reducer = pw.reducers.udf_reducer(ApproxDistinctReducer)
