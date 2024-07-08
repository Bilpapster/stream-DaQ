from first_try_pathway_cookiecutter.config import get_settings
from first_try_pathway_cookiecutter.input import input
from first_try_pathway_cookiecutter.output import output
from first_try_pathway_cookiecutter.pipeline import pipeline

import pathway as pw

if __name__ == "__main__":
    get_settings()

    input_table = input()
    output_table = pipeline(input_table)
    output(output_table)

    pw.run(monitoring_level=pw.MonitoringLevel.ALL)
