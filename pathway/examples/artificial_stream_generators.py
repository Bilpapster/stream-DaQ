import pathway as pw


def generate_artificial_range_stream(number_of_rows: int = 50, offset: int = 10, input_rate: float = 1.0):
    """
    A utility method to generate an artificial stream of increasing numbers.
    :param number_of_rows: The number of rows to generate. If set to None, the stream will be generated infinitely.
    Defaults to 50 values.
    :param offset: The number to start generating from. Defaults to 10.
    :param input_rate: The rate with which the values of the stream will be generated. Defaults to 1 value per second.
    :return: a stream of increasing numbers, starting from offset and generating number_of_rows values.
    """
    artificial_range_stream = pw.demo.range_stream(nb_rows=number_of_rows, offset=offset, input_rate=input_rate)
    return artificial_range_stream
