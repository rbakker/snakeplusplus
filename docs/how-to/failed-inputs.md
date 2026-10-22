# Continue when some jobs fail

A group analysis over 50 subjects should not wait for the 2 whose data are broken. By default, a
job whose input comes from a failed job does not run (see [Where errors go](../concepts/errors.md)).
An input that admits `JobError` changes that: the job runs, and gets the error in place of the
failed job's output.

```python
--8<-- "examples/howto/Snakefile:group"
```

`JobLoop[float | JobError]` says: a list of floats, where a failed subject is a `JobError`. In
`examples/howto/Snakefile`, the data of subject 03 contain a word where a number should be, so
`smooth` and `analyse` fail for 03. `group_mean` still runs, with the other two subjects, and its
log says what was left out:

```
[0:00:00] Left out, because their analysis failed: 03
```

A `JobError` is an exception: `str(error)` is the original error message, which can be written to
the result or the log.

## After fixing the failed jobs

When the data of 03 are fixed, the next run reruns `smooth` and `analyse` for 03, and then
`group_mean`, because a job it depends on ran again. The group result then includes all three
subjects, without any change to the Snakefile.

## When not to do this

Only admit `JobError` where a missing input still gives a meaningful result. For an analysis that
needs every subject, e.g. a group template, let it fail: the `.error` file then lists which
subjects failed and why.

Other forms: `list[Path | JobError]` for a plain list, and `Path | JobError` for a single input,
e.g. an optional extra measurement.
