import pathway as pw

# User-defined lower and upper bounds
LOWER_BOUND = 10
UPPER_BOUND = 100

# User-defined functions for simplistic DQ measures
def is_out_of_range(value):
    if value is None:
        return False # do not count missing values as "out of range"
    return not LOWER_BOUND <= value <= UPPER_BOUND

def main():
    # Create a sample input table using pw.debug.table_from_markdown()
    input_table = pw.debug.table_from_markdown('''
           | value
        1  | 50
        2  | 
        3  | 150
        4  | 5
        5  | 4893
        6  | 44
        7  | 66
        8  | 44
        9  | 34.6
        10 | 675
        11 | 3.5
        12 | 
        13 | 55.6
    ''')

    # Add columns to identify missing and out-of-range values using with_columns()
    input_table = input_table.with_columns(
        # Check for missing values (only None are considered missing for simplicity)
        is_missing=input_table.value.is_none(),
        # Check for out-of-range values with custom function
        is_out_of_range=pw.apply_with_type(is_out_of_range, bool, pw.this.value),
    )

    # Count the number of missing values
    missing_values_count_table = input_table.filter(input_table.is_missing).reduce(
        total_missing=pw.reducers.count()
    )

    # Count the number of out-of-range values
    out_of_range_count_table = input_table.filter(input_table.is_out_of_range).reduce(
        total_out_of_range=pw.reducers.count()
    )

    # Print the counts
    print("Missing Values Count:")
    pw.debug.compute_and_print(missing_values_count_table, include_id=False)

    print("\nOut-of-Range Values Count:")
    pw.debug.compute_and_print(out_of_range_count_table, include_id=False)



if __name__ == '__main__':
    main()