# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
import time
import pathway as pw

json_data = [
    {"timestamp": 1, "genus": "otocolobus", "epithet": "manul"},
    {"timestamp": 2, "genus": "felis", "epithet": "catus"},
    {"timestamp": 3, "genus": "lynx", "epithet": "lynx"},
    {"timestamp": 4, "genus": "otocolobus", "epithet": "manul"},
    {"timestamp": 5, "genus": "felis", "epithet": "catus"},
    {"timestamp": 6, "genus": "lynx", "epithet": "lynx"},
    {"timestamp": 7, "genus": "otocolobus", "epithet": "manul"},
    {"timestamp": 8, "genus": "felis", "epithet": "catus"},
    {"timestamp": 9, "genus": "lynx", "epithet": "lynx"},
    {"timestamp": 10, "genus": "otocolobus", "epithet": "manul"},
]


class FileStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        for line in json_data:
            self.next(**line)
            time.sleep(1)


class InputSchema(pw.Schema):
    timestamp: int
    genus: str
    epithet: str


data = pw.io.python.read(
    FileStreamSubject(),
    schema=InputSchema,
)


def write_to_jsonlines(data: pw.internals.Table) -> None:
    # replace the code in this function with a suitable sink operation for your use case.
    # A complete list of pathway connectors can be found here: https://pathway.com/developers/api-docs/pathway-io
    # Here, we just write the output as jsonlines to 'output.jsonlines'.
    # New quality assessment results are written (appended) to the file on the fly, when window processing is finished.
    pw.io.jsonlines.write(data, "output.jsonlines")
    # pw.debug.compute_and_print(data)

daq = StreamDaQ().configure(
    source=data,
    window=Windows.sliding(hop=1, duration=5, origin=0),
    # instance="genus",
    time_column="timestamp",
    wait_for_late=1,
    sink_operation=write_to_jsonlines,
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('genus'), "count_genus") \
    .add(dqm.mean_length("epithet"), "mean_length_epithet")

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
