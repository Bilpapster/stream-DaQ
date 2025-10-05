# pip install streamdaq
import pathway as pw

## These will be hidden in streamdaq
STATE = dict()

def update(row):
    if FIELDS_COLUMN in STATE:
        # print("I already know the fields. Bye!")
        # print(STATE)
        # print()
        return
    
    STATE[FIELDS_COLUMN] = row[FIELDS_COLUMN]
    # print("Thank you for letting me know the fields. Now I know!")
    # print(STATE)
    # print()

def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
    print(f"Invoking for row {row}")
    update(row)

def on_end():
    pass

## This is just for demo, it works for any pw.Connector!
class FileStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        import time
        
        fields = ["temperature", "humidity", "pressure"]
        nof_compact_rows = 5
        start = 0
        for _ in range(nof_compact_rows):
            message = {
                FIELDS_COLUMN: fields,
                VALUES_COLUMN: [start + i for i in range(len(fields))],
            }
            ## This just generates a bunch of data for demonstration, skip it
            start += len(fields)
            self.next(**message)
            time.sleep(0.5)

## Your job starts here!
## You only need to specify which column has the fields and which column has the values
FIELDS_COLUMN = "fields"
VALUES_COLUMN = "values"
schema_dict = {
    FIELDS_COLUMN: list[str],
    VALUES_COLUMN: list[int]
}
schema = pw.schema_from_dict(schema_dict)

table = pw.io.python.read(
    FileStreamSubject(),
    schema=schema,
)
## Your job ends here, streamdaq handles the rest!

## These will also be hidden in streamdaq
pw.io.subscribe(table, on_change=on_change, on_end=on_end)
pw.run(monitoring_level=pw.MonitoringLevel.NONE)

compact_to_native_transformations = {
    STATE[FIELDS_COLUMN][i]: pw.this.values.get(i) for i in range(len(STATE[FIELDS_COLUMN]))
}

## This is just for demo. It works for any sink and any existing data quality measure!
result = table.with_columns(**compact_to_native_transformations) \
    .without(VALUES_COLUMN, FIELDS_COLUMN)
print("Compact representation")
pw.debug.compute_and_print(table)
print()
print("Native representation")
pw.debug.compute_and_print(result)