# Where errors go

Mistakes that can be known in advance are reported in the console, before any job runs. Once jobs
run, Snakemake does not stop for errors: each one goes to the `.error` file of the job where it
happened, and the other jobs continue.

## Before the run: the console

When the Snakefile is loaded, also in a dry run (`snakemake -n`), snakeplusplus checks everything
that does not depend on data:

| mistake | message |
|---|---|
| a misspelled parameter | `CountWords: unknown parameter(s) ['min_word']; CountWords.ParamModel has ['min_words']` |
| connected types that do not fit | `Type mismatch: CountWords.foreach(...).words produces JobLoop[int], but Report.words expects JobLoop[Path]` |
| an unknown input or output name | `Count.set_input(): unknown inputs: m; missing inputs: n. The inputs are: n` |
| a wildcard that nothing provides | `GroupTable.values: Measure has wildcard(s) {session}, which neither the connection nor GroupTable provides; use .foreach(...) or .where(...) on Measure` |
| a name in `run()` that is not defined | `CountWords.run, logs.smk line 31: name 'shutil' is not defined (missing import?)` |
| a rule name that hides a name of Python or Snakemake | `'all' would hide Python's built-in all()` |
| files of another user that block the run | see [Share a pipeline with colleagues](../how-to/shared-pipeline.md) |

These are Python exceptions, which Snakemake shows with the line of the Snakefile where they
happened. Nothing has run yet, so fixing the Snakefile and starting again is all it takes.

## During the run: the log files

Everything that goes wrong inside a job ends up in its `.error` file, under `Errors occurred`:

- **an exception in `run()`**, with the lines of your own code where it happened (Snakefile line
  numbers, not those of snakeplusplus);
- **a command that failed**, with its output above the error:

    ```
    wc exited with code 1 (log-file: text-fox_count_words.error)
      File "logs.smk", line 30, in run
        code, out = job.run(['wc', '-w', input.text, 'missing.txt'])
    ```

- **missing outputs**, checked after `run()`:

    ```
    these required outputs of CountWords are missing:
      upper: results/text-fox/count_words/upper.txt
    (a file output is optional if annotated `Path | None`; another output is optional if it has a default value)
    ```

- **inputs of the wrong type**, which could only be checked when the job got them, e.g. the
  result of an input function.

For Snakemake, the job succeeded, so it carries on with all jobs that do not depend on it.

### Your own errors

To stop a job with a clear message and without a traceback, raise `JobError`:

```python
if words < params.min_words:
    raise JobError(f'{input.text} has {words} words, fewer than min_words={params.min_words}')
```

To report a problem and let `run()` continue, e.g. to check all subjects before failing, use
`job.error('...')`: the job still ends as `.error`.

## When a job it depends on failed

A job whose input comes from a failed job does not run. Its `.error` file repeats the original
error, so you do not need to look further:

```
"word_report" did not run because 1 job(s) it depends on failed:
- input 'words': logs/text-short_count_words
    texts/short.txt has 1 words, fewer than min_words=2
    (log-file: text-short_count_words.error)
```

A rule that can do without some of its inputs, e.g. a group table that leaves out a failed
subject, annotates them with `JobError`: `JobLoop[float | JobError]`. It then runs, and gets the
error in place of the value; see [Continue when some jobs fail](../how-to/failed-inputs.md).

## Fixing and running again

Failed jobs run again on the next run (unless `configure(..., retry_failed=False)`), together with
the jobs that depend on them; see [When jobs rerun](reruns.md). Errors in loop functions, which
can happen before or during the run, are described in
[Loops and checkpoints](loops-and-checkpoints.md#when-a-loop-function-fails).
