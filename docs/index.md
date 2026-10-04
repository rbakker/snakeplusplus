# Snake++

Modular, object-oriented pipelines on top of [Snakemake](https://snakemake.readthedocs.io).

## Why Snake++

[Snakemake](https://snakemake.readthedocs.io) is a great tool for running data processing
pipelines that make optimal use of the available compute power, all within a Pythonic
environment. It does, however, have a steep learning curve, because every connection in the
pipeline graph is defined in terms of parameterized file names (*wildcards*, in Snakemake terms).
Snake++ takes the file name juggling out of your hands: rules are proper Python classes, connected
with `set_input()` and `get_output()`.

On top of that, every job keeps one log file whose name shows its state, and a failed job never
stops the run: fix the cause, run again, and only what failed is redone.

## In short

- **Rules are Python classes** with typed inputs, outputs and parameters (pydantic models), and
  a `result_template` that says where each job writes; they can be reused, inherited and documented.
- **Rules are connected explicitly** with `set_input(x=other.get_output('y'))` and
  `foreach(...)`; connection mistakes are caught before anything runs.
- **One log file per job is the only thing Snakemake tracks.** Its extension shows the job's
  state (`.queued`, `.running`, `.log`, `.error`, `.stale`), so the log folder is a live overview of the
  pipeline. Delete a log file to rerun that job.
- **Errors never crash the run.** A failed job leaves an `.error` file; jobs that depend on it
  fail with the root cause, or receive it as a `JobError` value if their input type admits it. Failed jobs are retried on
  the next run.

```python
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class SayHello(SnakeRule):
    result_template = '{greeting}'

    class OutputModel(Fixed):
        text: Path = Field('{greeting}-output.txt')

    def run(self, job, input, output, params, wildcards):
        job.shell(f"echo '{wildcards.greeting}' > {output.text}")

say_hello = SayHello()
runall = target(say_hello.foreach(greeting=['Hello', 'Bonjour', 'Hola']))

snakeplusplus.build(locals())
```

## Installation

```
pip install snakeplusplus
```

## Where to go next

- The [tutorial](getting-started.md) builds a small pipeline step by step.
- [Concepts](concepts/rules.md) explains how rules, connections, log files and reruns work.
- The [comparison](comparison.md) sets Snake++ next to Snakemake, Nextflow and Pydra.
