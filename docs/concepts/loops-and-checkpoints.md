# Loops and checkpoints

A rule with wildcards stands for many jobs: one per subject, per session, per tract. A **loop**
selects some of those jobs, so that another rule can receive all their outputs at once. A
**checkpoint** is a job whose result decides what a loop contains, e.g. the subjects listed in a
file that the pipeline itself produces.

The examples on this page come from a small study with subjects and sessions, in
`examples/concepts/loops.smk`. Its main rule measures one value per subject and session:

```python
--8<-- "examples/concepts/loops.smk:measure"
```

## Making a loop

`foreach()` adds one loop level per call, in one of four forms:

```python
--8<-- "examples/concepts/loops.smk:loops"
```

| form | the loop runs over |
|---|---|
| `foreach(session=['a', 'b'])` | these values of one wildcard |
| `foreach(session=function)` | the values that `function(wildcards)` returns |
| `foreach([dict(subject='01', session='b'), ...])` | these combinations of wildcards |
| `foreach(function)` | the dicts that `function(wildcards)` returns |

Chained calls are nested loops, outer first: `foreach(subject=SUBJECTS).foreach(session=['a'])`
gives every combination. A function in an inner level gets the wildcards of the outer levels, so
`sessions_of` above is called once per subject, with `wildcards.subject` set, and each subject
gets its own sessions.

`where()` fixes wildcards to single values instead: `measure.where(subject='01', session='a')`
is one job. It is useful for a test run, or for a connection to one particular job, e.g. a
template subject that all other subjects are registered to. `where()` and `foreach()` can be
combined: `track.where(subject='01').foreach(hemi=['L', 'R'])`.

### One wildcard per call

`foreach()` takes one wildcard at a time:

```
TypeError: Measure.foreach() takes one wildcard at a time, got subject, session. For all combinations,
chain the loops: .foreach(subject=[...]).foreach(session=[...]); for specific combinations, give a list
of dicts: .foreach([dict(subject=..., session=...), ...])
```

Snakemake's `expand()` would take all combinations of `subject=[...], session=[...]`, while
Python's `zip()` would pair them. Both are common, so snakeplusplus does not guess: a chain means
all combinations, and a list of dicts means exactly those. Likewise, a single value is not a loop:
`foreach(subject='01')` is an error that suggests `where(subject='01')`.

## Connecting a loop

A loop is connected like a rule, with `get_output()`:

```python
--8<-- "examples/concepts/loops.smk:connections"
```

Wildcards are passed on from the receiving job, as for single connections. `SubjectMean` has the
wildcard `{subject}`, so the loop in `subject_mean` only needs to add the sessions: the job for
subject 03 gets the values of sessions a, b and c of subject 03. Every wildcard of the looped rule
must come from the loop or from the receiving rule; this is checked when the Snakefile is loaded:

```
TypeError: GroupTable.values: Measure has wildcard(s) {session}, which neither the connection nor
GroupTable provides; use .foreach(...) or .where(...) on Measure
```

## What the receiving job gets

An input annotated `JobLoop[...]` gets the outputs in loop order, as a list that also knows the
wildcards of each item:

```python
--8<-- "examples/concepts/loops.smk:summaries"
```

| | |
|---|---|
| `for wc, value in input.values.items()` | pairs of wildcards and value; `wc.subject`, `wc.session` |
| `input.values.where(subject='03')` | the items with these wildcard values, again a `JobLoop` |
| `input.values.get(subject='01', session='a')` | the one value with these wildcard values; an error if there is none or more than one |
| `input.values.groupby('subject')` | a dict from each subject to a `JobLoop` of its items |
| `input.values.to_pandas()` | a table with a column per wildcard and a column `value` (needs pandas) |

An input annotated `list[...]` gets a plain list, as `Count` does. The type of the items is
checked like any other connection: `JobLoop[float]` for `value: float`, `JobLoop[Path]` for file
outputs. With `JobLoop[float | JobError]`, the receiving job also runs when some jobs of the loop
failed; see [Continue when some jobs fail](../how-to/failed-inputs.md).

A `JobLoop` can also be made from files that another pipeline produced, with
`JobLoop.from_pattern('other/sub-{subject}/fa.nii.gz')`; see the [Reference](../reference.md).

## Checkpoints

Sometimes the loop's contents are only known once the pipeline has run part of its work: the
subjects that passed quality control, or the sessions listed in a file. A `SnakeCheckpoint` is a
rule like any other, except that a loop function can read its result with
`JobResult.from_checkpoint()`:

```python
--8<-- "examples/concepts/loops.smk:checkpoint"
```

`checkpoints.find_sessions` is Snakemake's name for the checkpoint `find_sessions` in the
Snakefile. The loop function returns the combinations of wildcards, so it is used in the
`foreach(function)` form:

```python
--8<-- "examples/concepts/loops.smk:checkpoint_loop"
```

Snakemake first runs `find_sessions`, then calls `found_sessions` to find out which `measure`
jobs `found_table` needs, runs those, and then `found_table`.

## When a loop function fails

A loop function is ordinary Python, and can fail, e.g. for a subject that is missing from a
dictionary. When that happens before any job runs, while Snakemake works out which jobs are
needed, the error is shown in the console and nothing runs:

```
LoopError: Wildcard iterator of loop over rule 'measure' failed:
    return SESSIONS[wildcards.subject]
KeyError: '04'
```

A loop function that reads a checkpoint is only called once the checkpoint has run, so its errors
happen while other jobs are already running. Then, as everywhere in snakeplusplus, Snakemake does
not stop: the error is reported in the `.error` file of the job that uses the loop, here
`logs/found_table.error`, and in a warning in the console. If the checkpoint itself failed, that
message says so, with the checkpoint's error.
