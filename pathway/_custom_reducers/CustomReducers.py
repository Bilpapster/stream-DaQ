import pathway as pw


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


std_dev_reducer = pw.reducers.udf_reducer(StdDevReducer)
