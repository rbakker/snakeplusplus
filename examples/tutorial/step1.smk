from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, target, Field

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)


class SayHello(SnakeRule):
    """Write a greeting to a file."""
    result_template = '{greeting}'

    class OutputModel:
        text: Path = Field('hello.txt')

    def run(self, job, input, output, params, wildcards):
        with open(output.text, 'w') as fp:
            fp.write(wildcards.greeting + '\n')


say_hello = SayHello()
everything = target(say_hello.foreach(greeting=['Hello', 'Bonjour', 'Hola']))

snakeplusplus.build(locals())
