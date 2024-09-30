from abc import ABC, abstractmethod
from enum import Enum


class ThresholdType(Enum):
    LOW = 'LOW'  # measurement must be strictly greater than the threshold
    LOW_INC = 'LOW_INC'  # measurement must be greater or equal to the threshold
    HIGH = 'HIGH'  # measurement must be strictly smaller than the threshold
    HIGH_INC = 'HIGH_INC'  # measurement must be smaller or equal to the threshold
    EXACT = 'EXACT'  # measurement must be equal to the threshold


class Threshold(ABC):
    def __init__(self, threshold_value, threshold_type):
        self.__threshold_value = threshold_value
        self.__threshold_type = threshold_type

    def get_threshold_value(self):
        return self.__threshold_value

    def set_threshold_value(self, value):
        self.__threshold_value = value

    def get_threshold_type(self):
        return self.__threshold_type

    def set_threshold_type(self, value):
        self.__threshold_type = value

    def evaluate(self, measurement_result):
        evaluation_function = self.__get_evaluation_function()
        return evaluation_function(measurement_result, self.get_threshold_value())

    def __get_evaluation_function(self):
        match self.get_threshold_type():
            case ThresholdType.LOW:
                return lambda result, threshold: result > threshold
            case ThresholdType.LOW_INC:
                return lambda result, threshold: result >= threshold
            case ThresholdType.HIGH:
                return lambda result, threshold: result < threshold
            case ThresholdType.HIGH_INC:
                return lambda result, threshold: result <= threshold
            case ThresholdType.EXACT:
                return lambda result, threshold: result == threshold
