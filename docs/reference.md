# Reference

Everything that `from snakeplusplus import *` provides. `Field` and `Json` are pydantic's
[`Field`](https://docs.pydantic.dev/latest/concepts/fields/) and
[`Json`](https://docs.pydantic.dev/latest/api/types/#pydantic.types.Json), for convenience.

## Rules

::: snakeplusplus.SnakeRule
    options:
      members: [__init__, set_input, get_output, foreach, where, run, log_path, result_path, describe]

::: snakeplusplus.SnakeCheckpoint
    options:
      members: false

::: snakeplusplus.OutputPromise
    options:
      members: [when]

## Models

::: snakeplusplus.Fixed
    options:
      members: false

::: snakeplusplus.Extensible
    options:
      members: false

::: snakeplusplus.PathPrefix
    options:
      members: [matches]

## In the Snakefile

::: snakeplusplus.configure

::: snakeplusplus.target

::: snakeplusplus.build

## In run()

::: snakeplusplus.JobMonitor
    options:
      members: [run, shell, log, error, tmpdir]

::: snakeplusplus.JobResult
    options:
      members: [from_checkpoint]

::: snakeplusplus.JobLoop
    options:
      members: [from_pattern, items, where, get, groupby, to_pandas]

::: snakeplusplus.JobError
    options:
      members: false
