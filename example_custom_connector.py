import json
import time
import pathway as pw

class FileStreamSubject(pw.io.python.ConnectorSubject):
    def run(self):
        with open("cats.jsonl") as file:
            for line in file:
                data = json.loads(line)
                self.next(**data)
                time.sleep(1)
