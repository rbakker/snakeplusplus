"""An example rule: replace it with the pipeline's own rules.

Rule classes live in Python modules like this one, imported by the Snakefile. Here, Snakemake's
keywords (report, use, rule, ...) are ordinary names, unlike in the Snakefile itself.
"""
import sys
from pathlib import Path

from snakeplusplus import SnakeRule, Field


class Greet(SnakeRule):
    """Write a greeting for one sample."""
    result_template = 'sample-{name}'
    # conda_env = 'envs/tool.yaml'   # run the jobs in this conda environment (snakemake --use-conda)

    class ParamModel:
        greeting: str = Field('Hi', description='the word to greet with')

    class OutputModel:
        greeting: Path = Field('greeting.txt')

    def run(self, job, input, output, params, wildcards):
        job.log(f'Greeting {wildcards.name}')
        job.run([sys.executable, 'workflow/scripts/greet.py', params.greeting, wildcards.name, output.greeting])
