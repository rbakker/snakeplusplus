# Snake++ (`snakeplusplus`)

Modular, object-oriented pipelines on top of [Snakemake](https://snakemake.readthedocs.io).

- **Rules are Python classes** with typed wildcards, inputs, outputs and parameters (pydantic
  models), so they can be reused, inherited and documented.
- **Rules are connected explicitly** with `set_input(x=other.get_output('y'))` and
  `foreach(...)`; connection mistakes are caught before anything runs.
- **One log file per job is the only thing Snakemake tracks.** Its extension shows the job's
  state (`.running`, `.log`, `.error`, `.stale`), so the log folder is a live overview of the
  pipeline. Delete a log file to rerun that job.
- **Errors never crash the run.** A failed job leaves an `.error` file; jobs that depend on it
  fail with the root cause, or continue if they allow failed inputs. Failed jobs are retried on
  the next run.

```python
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, TargetRule, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class SayHello(SnakeRule):
    class WildcardModel(Fixed):
        greeting: str = Field('{}')

    class OutputModel(Fixed):
        text: Path = Field('{greeting}-output.txt')

    def run(self, job, input, output, params, wildcards):
        job.shell(f"echo '{wildcards.greeting}' > {output.text}")

say_hello = SayHello()
runall = TargetRule().set_input(_=say_hello.foreach(greeting=['Hello', 'Bonjour', 'Hola']))

include: snakeplusplus.SNAKEFILE
snakeplusplus.build(locals())
```

## Installation

```
pip install snakeplusplus
```

For development: `pip install -e ".[test]"`, then `pytest`.

## Documentation of a pipeline

```
snakeplusplus-doc path/to/Snakefile -o docs/pipeline.html
```

generates an HTML overview of the pipeline: a diagram of the rules and how they are connected,
and a reference of each rule's wildcards, inputs, outputs and parameters.
