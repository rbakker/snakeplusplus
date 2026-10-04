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


class GreetingRule(SnakeRule):
    """Base class for the rules that run once per greeting."""
    result_template = '{greeting}'


class SayHello(GreetingRule):
    """Write a greeting to a file."""
    class OutputModel:
        text: Path = Field('hello.txt')

    def run(self, job, input, output, params, wildcards):
        with open(output.text, 'w') as fp:
            fp.write(wildcards.greeting + '\n')


class ConvertToUpper(GreetingRule):
    """Convert a text file to upper case."""
    class InputModel:
        text: Path

    class OutputModel:
        upper: Path = Field('upper.txt')

    def run(self, job, input, output, params, wildcards):
        job.shell(f"tr '[:lower:]' '[:upper:]' < {input.text} > {output.upper}")


class CollectGreetings(SnakeRule):
    """Put all greetings in one file, and count them."""
    class InputModel:
        files: list[Path]

    class OutputModel:
        collected: Path = Field('all_greetings.txt')
        count: int

    def run(self, job, input, output, params, wildcards):
        with open(output.collected, 'w') as fp:
            for f in input.files:
                fp.write(f.read_text())
        output(count=len(input.files))


say_hello = SayHello()
convert_to_upper = ConvertToUpper().set_input(text=say_hello.get_output('text'))
collect_greetings = CollectGreetings().set_input(
    files=convert_to_upper.foreach(greetings).get_output('upper'),
)

everything = target(collect_greetings)
uppercase = target(convert_to_upper.foreach(greetings))

snakeplusplus.build(locals())
