from typing import Self, Type, Optional


class CompactData:
    """Use this class to configure the compact data representation
    of your data within Stream DaQ. Compact data representation is
    typical in IoT streaming scenarios. If you are not sure whether
    your data is in compact or native representation, see the examples
    below. The default parameters, driven by IoT use-cases, are also
    detailed below.

    Is My Data Compact?
    ^^^^^^^^^^^^^^^^^^^
    **Compact** data representation looks like this:
    .. table::

        ==============  ===========================  =================
        timestamp       fields                       values
        ==============  ===========================  =================
        1               ['temperature', 'pressure']   [0.1, 0.2]
        2               ['temperature', 'pressure']   [0.3, 0.4]
        ...             ...                           ...
        ==============  ===========================  =================

    On the contrary, **native** data representation looks like this:
    .. table::

        ==============  ===========  =================
        timestamp       temperature  pressure
        ==============  ===========  =================
        1               0.1          0.2
        2               0.3          0.4
        ...             ...          ...
        ==============  ===========  =================

    If your data is already in native representation,
    then you can stop reading, since you do **NOT** need this class.

    Default Parameter Values
    ^^^^^^^^^^^^^^^^^^^^^^^^
    Any new `CompactData` instance, comes with the following defaults:
    - `fields_column`: `'fields'`
    - `values_column`: `'values'`
    - `values_dtype`: `float`

    *NOTE:* The data type of fields is expected to be `list[str]`, since
    they refer to a list of column names. For example,
    `['temperature', 'pressure']`.

    If you are happy with these defaults, simply use `CompactData()` to
    create a new instance. To modify any of these default values, use
    the provided `with_{parameter}` functions. For example, use
    `with_fields_column('a_column_name')` to overwrite the fields column name.

    *NOTE:* All `with_{parameter}` functions return a self reference, enabling
    easy, readable chaining as shown in the example below.

    Example
    ^^^^^^^
    Assuming the following table of streaming data in compact representation:
    .. table::

        ==============  ===========================  =================
        timestamp       my_fields                    my_values
        ==============  ===========================  =================
        1               ['temperature', 'pressure']   [1, 2]
        2               ['temperature', 'pressure']   [3, 4]
        ...             ...                           ...
        ==============  ===========================  =================
    one can readily monitor this data with Stream DaQ **without the
    need to transform it** to native. Just pass the following
    argument to the `configure()` function:
    >>> daq = StreamDaQ().configure(
        ...
        time_column='timestamp',
        ...
        compact_data=CompactData()
            .with_fields_column('my_fields') # omit if 'fields'
            .with_values_column('my_values') # omit if 'values'
            .with_values_dtype(int)          # omit if float
    )
    # ... define windows, checks and everything else as usual!
    # ... Stream DaQ takes care of handling your compact data as deserved!
    """

    def __init__(self):
        """Default parameters:
        - `fields_column`: `'fields'`
        - `values_column`: `'values'`
        - `values_dtype`: `float`
        """
        self._fields_column = "fields"
        self._values_column = "values"
        self._values_dtype = float
        self._type_exceptions = None

    def with_fields_column(self, fields_column: str) -> Self:
        """Set the name that contains the field names. You do not
        need to use it if the column names is `'fields'` (case sensitive).

        Args:
            fields_column (str): the name of the column containing the fields.

        Returns:
            Self: a self reference to accommodate chain calls.
        """
        self._fields_column = str(fields_column)
        return self

    def with_values_column(self, values_column: str) -> Self:
        """Set the name that contains the values. You do not
        need to use it if the column name is `'values'` (case sensitive).

        Args:
            values_column (str): the name of the column containing the values.

        Returns:
            Self: a self reference to accommodate chain calls.
        """
        self._values_column = str(values_column)
        return self

    def with_values_dtype(self, values_dtype: Type, exceptions: Optional[dict[str, Type]] = None) -> Self:
        """Set the data type of the individual values. You do not
        need to use it if the data type of all your values is float.

        Args:
            values_dtype (Type): the (default) data type of the
            individual values.
            exceptions (Optional[dict[str, Type]], optional): A mapping 
            between column names and data types that deviate from the
            `values_dtype` example. Use this parameter if the majority 
            of your values are of type `values_dtype` but there are some
            exceptions that should be manually defined.
            Column names that do not correspond to existing ones will be 
            ignored. Defaults to None.

        Returns:
            Self: a self reference to accommodate chain calls.
        """
        self._values_dtype = values_dtype
        self._type_exceptions = exceptions
        return self
