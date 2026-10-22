# Connecting rules

Rules are connected explicitly: you say which output of which rule goes to which input, and
Snake++ works out the file names. Mistakes are reported when the Snakefile is loaded, before
anything runs.

## `set_input()`

`set_input()` takes one keyword per field of the rule's `InputModel`, and returns the rule, so
that it can directly follow the constructor:

```python
--8<-- "examples/concepts/dwi.smk:graph"
```

A value can be:

| Value | The input gets |
|---|---|
| `fit.get_output('fa')` | output `fa` of the `fit` job with the same wildcards |
| `fit` (a rule) | the whole `JobResult` of that job: `input.x.fa`, `input.x.md` |
| `fit.foreach(...).get_output('fa')` | the outputs of many jobs, see [Loops and checkpoints](loops-and-checkpoints.md) |
| `fit.where(subject='template').get_output('fa')` | the output of one specific job |
| `'data/sub-{subject}/dwi.mif'` | a file name; the wildcards of the job are filled in |
| a function of the wildcards | file names, or a connection; see below |

Every input of the `InputModel` without a default must be given; an input with a default, such as
`mask: Path | None = None`, can be left out. A wrong name is reported with the names that exist:

```
ValueError: Count.set_input(): unknown inputs: m; missing inputs: n. The inputs are: n
```

## Wildcards are passed on

A job receives the outputs of the jobs with the same wildcards: the `fit` job for subject 01 gets
the `denoise` output of subject 01. A rule may have fewer wildcards than the rule it receives
from, if a loop or `where()` provides the rest; every wildcard of the source must come from
somewhere, otherwise:

```
TypeError: Group.scans: Scan has wildcard(s) {session}, which neither the connection nor Group provides;
use .foreach(...) or .where(...) on Scan
```

## Types are checked

The type of an output must suit the type of the input it is connected to:

```
TypeError: Type mismatch: Denoise.denoised produces Path, but Count.n expects int
```

When a job starts, the inputs it actually receives are checked once more against the
`InputModel`; a mismatch then ends up in the job's `.error` file:

```
these inputs of Count have a wrong type:
  n: Input should be a valid integer, unable to parse string as an integer (got 'not a number')
```

## Parsers: converting an output

A parser converts an output before it is passed on, e.g. reads a number from a file:

```python
def read_float(path: Path) -> float:
    return float(open(path).read())

stats = Stats().set_input(snr=snr_rule.get_output('snr_file', parser=read_float))
```

The type hints of the parser are used to check the connection, so they are required. A parser
with a second argument also gets the wildcards of the job: `def tag(path: Path, wildcards) -> str`.

## Choosing an output per job

When the output to use depends on a wildcard of the receiving rule, give a pattern instead of a
name:

```python
--8<-- "examples/concepts/dwi.smk:pattern"
```

For the job with `hemi='L'`, `tck` is the output `DRT_L`. This is checked when the Snakefile is
loaded: `{hemi}` must be a wildcard of the receiving rule, at least one output must fit the
pattern, and all fitting outputs must suit the type of the input.

For choices that a pattern cannot express (a different rule per subject, or a hand-corrected
file when there is one), an input function can return a connection; see
[Choose an input per job](../how-to/inputs-per-job.md). Its type is then only checked when the
job runs.

## Keys work like wildcards

An output with several files from one job, e.g. `tracts: JobLoop[Path] = Field('tract_{hemi}.tck')`,
has keys (`hemi`) that name the files within the job. A key is a wildcard of the output instead of
the job, and it is filled in like one: a receiving rule with a wildcard `{hemi}` gets the one file
for its `hemi`; a rule without it gets all files.

```python
split = Split()                                                     # sub-{subject}, tract_{hemi}.tck
per_hemi = PerHemi().set_input(tract=split.get_output('tracts'))     # sub-{subject}/hemi-{hemi}: a Path
all_hemis = AllHemis().set_input(tracts=split.get_output('tracts'))  # sub-{subject}: a JobLoop[Path]
group = Group().set_input(tracts=split.foreach(subject=SUBJECTS).get_output('tracts'))
                                                                    # hemi-{hemi}: one file per subject
```

So the type of the input depends on the receiving rule: `Path` when all keys are its wildcards,
`JobLoop[Path]` otherwise. It is checked when the Snakefile is loaded. If the job did not produce
the file for a key value, the receiving job fails (or gets a `JobError`, if its input admits one):

```
Split did not produce 'tracts' for hemi=X (it made: L, R; log-file: logs/sub-01_split.log)
```

## Connections that depend on a parameter

An input that is only needed for some parameter values, e.g. one for an alternative method that
takes long to compute, is connected with `when()`:

```python
class Fit(SnakeRule):
    class ParamModel:
        use_alt: bool = False
    class InputModel:
        extra: Path | None = None

fit = Fit().set_input(extra=expensive.get_output('x').when(use_alt=True))
```

For a job whose `use_alt` is not `True`, the `expensive` job is not run, and `input.extra` is
`None`. The condition uses the job's final parameters: given to the constructor, from the
config, the default, or a function of the wildcards. Changing the parameter reruns the job, as
any parameter change does; switched on, the `expensive` jobs are run first.

When the Snakefile is loaded, the parameters must exist in the receiving rule's `ParamModel`
with valid values, and the input must admit `None`:

```
TypeError: Fit.extra is connected with when(), so it is None when the condition does not hold;
annotate it as optional, e.g. `extra: Path | None = None`
```

In the diagram, such a connection is a dashed arrow, labelled with its condition.

## What a job receives

In `run()`, `input` is an instance of the `InputModel`, with the values of this job:

- a connected output arrives as its type says: a `Path`, a number, a `JobResult`;
- a loop arrives as a `JobLoop`, or as a plain list if the input is annotated `list[...]`;
- if a job upstream failed, this job fails too, with the original error in its log; unless the
  type of the input admits `JobError`, e.g. `JobLoop[float | JobError]`. See
  [Continue when some jobs fail](../how-to/failed-inputs.md).
