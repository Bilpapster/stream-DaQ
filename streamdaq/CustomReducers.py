import pathway as pw
from datasketch import HyperLogLogPlusPlus

# todo check if this implementation of HLL++ sketching is more reliable (Apache)
#  https://apache.github.io/datasketches-python/5.0.2/distinct_counting/hyper_log_log.html
from datasketches import frequent_items_error_type, frequent_strings_sketch


class StdDevReducer(pw.BaseCustomAccumulator):
    def __init__(self, count, sum, sum_squares):
        self.count = count
        self.sum = sum
        self.sum_squares = sum_squares

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(1, value, value**2)

    def update(self, other):
        self.count += other.count
        self.sum += other.sum
        self.sum_squares += other.sum_squares

    def compute_result(self) -> float:
        mean = self.sum / self.count
        mean_squares = self.sum_squares / self.count
        return mean_squares - mean**2

    def retract(self, other):
        self.count -= other.count
        self.sum -= other.sum
        self.sum_squares -= other.sum_squares


std_dev_reducer = pw.reducers.udf_reducer(StdDevReducer)


class ApproxDistinctReducer(pw.BaseCustomAccumulator):

    def __init__(self, element: str):
        self.hpp_sketch = HyperLogLogPlusPlus()
        self.hpp_sketch.update(element.encode("utf-8"))

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(str(value))

    def update(self, other):
        self.hpp_sketch.merge(other.hpp_sketch)

    def compute_result(self) -> float:
        return self.hpp_sketch.count()


approx_distinct_count_reducer = pw.reducers.udf_reducer(ApproxDistinctReducer)


class ApproxMostFrequentItemsReducer(pw.BaseCustomAccumulator):
    # internally leverages apache datasketches for python
    # https://github.com/apache/datasketches-python/tree/main

    def __init__(self, element: str):
        # the sketch must be saved in its serialized (raw bytes) form, because it is not "picklable" otherwise.
        # unfortunately, pathway uses pickle.dump(self) internally, so this is the only easy solution I came up with
        # todo re-explore alternatives, such as implementing __getstate__ and __setstate__ or __reduce__ class methods
        k = 3
        self.serialized_sketch = frequent_strings_sketch(k).serialize()
        sketch = frequent_strings_sketch.deserialize(self.serialized_sketch)
        sketch.update(element, 1)
        self.serialized_sketch = sketch.serialize()

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(str(value))

    def update(self, other):
        other_sketch = frequent_strings_sketch.deserialize(other.serialized_sketch)
        sketch = frequent_strings_sketch.deserialize(self.serialized_sketch)
        sketch.merge(other_sketch)
        self.serialized_sketch = sketch.serialize()

    def compute_result(self) -> list:
        sketch = frequent_strings_sketch.deserialize(self.serialized_sketch)
        return sketch.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)


approx_most_frequent_items_reducer = pw.reducers.udf_reducer(ApproxMostFrequentItemsReducer)


class AllValuesTheSameReducer(pw.BaseCustomAccumulator):

    def __init__(self, element: str):
        # self.hpp_sketch = HyperLogLogPlusPlus()
        # self.hpp_sketch.update(element.encode('utf-8'))

        self.all_same_so_far = True
        self.element = element

    @classmethod
    def from_row(cls, row):
        [value] = row
        return cls(str(value))

    def update(self, other):
        if self.all_same_so_far and other.all_same_so_far and self.element == other.element:
            return

        self.all_same_so_far = False
        other.all_same_so_far = False
        # setting other.all_same_so_far may be redundant, if always `self` is updated. Todo: double-check

    def compute_result(self) -> bool:
        return self.all_same_so_far


all_values_the_same_reducer = pw.reducers.udf_reducer(AllValuesTheSameReducer)
