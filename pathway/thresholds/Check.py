from abc import ABC
from Threshold import Threshold


class CheckResult(ABC):
    def __init__(self, result: bool, message: str = None):
        self.__result = result
        self.__message = message

    def set_message(self, message: str):
        self.__message = message


class Check(ABC):
    def __init__(self, measurement, name):
        self.__measurement = measurement
        self.__name: str = name
        self.__thresholds: list[Threshold] = list()
        self.__evaluationResults: list[CheckResult] = list()

    def add_threshold(self, threshold: Threshold):
        self.__thresholds.append(threshold)

    def execute(self):
        for threshold in self.__thresholds:
            # todo find out how the measurement result will be available here
            # probably something like measurement.get_result()
            result = ...
            self.__evaluationResults.append(threshold.evaluate(result))
