import csv
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, target, Field

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)


def greetings(wildcards):
    """The greetings to process: one per line of greetings.csv."""
    with open(Path(workflow.basedir) / 'greetings.csv') as fp:
        for row in csv.reader(fp):
            yield dict(greeting=row[0])


class SayHello(SnakeRule):
    """Write a greeting to a file."""
    result_template = '{greeting}'

    class OutputModel:
        text: Path = Field('hello.txt')

    def run(self, job, input, output, params, wildcards):
        with open(output.text, 'w') as fp:
            fp.write(wildcards.greeting + '\n')


class ConvertToUpper(SnakeRule):
    """Convert a text file to upper case."""
    result_template = '{greeting}'

    class InputModel:
        text: Path

    class OutputModel:
        upper: Path = Field('upper.txt')

    def run(self, job, input, output, params, wildcards):
        job.shell(f"tr '[:lower:]' '[:upper:]' < {input.text} > {output.upper}")


say_hello = SayHello()
convert_to_upper = ConvertToUpper().set_input(text=say_hello.get_output('text'))
everything = target(convert_to_upper.foreach(greetings))

snakeplusplus.build(locals())
