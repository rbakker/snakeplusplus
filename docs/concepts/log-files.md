# Log files

Every job has exactly one log file. Its extension shows the state of the job, so the log folder is
an overview of the whole pipeline, and the file itself tells what the job did.

The examples on this page come from `examples/concepts/logs.smk`, which counts the words of a few
texts:

```python
--8<-- "examples/concepts/logs.smk:count"
```

## The states

```
logs/
    everything.error
    text-fox_count_words.log
    text-hamlet_count_words.log
    text-short_count_words.error
    word_report.error
```

| extension | the job |
|---|---|
| `.queued` | will run in the current Snakemake run, and has not started yet |
| `.running` | is running |
| `.log` | succeeded |
| `.error` | failed; the file says why |
| `.stale` | was interrupted, e.g. with Ctrl-C, or was queued to run again when the run stopped |

A job never has more than one of these files. When a job is queued again, its previous file is
renamed to `.queued`, with a note at the end; when it starts, the file is replaced.

The name of a log file comes from the rule's `result_template`, with `/` replaced by `_` (see
[Where a job writes](rules.md#where-a-job-writes-result_template)), and the log folder is
`logs` in `configure()`.

## What a log file contains

A log file is written as Markdown, so it reads well as plain text and in a Markdown viewer:

````
"count_words(CountWords)<name=fox>" started at 2026-10-22 11:37:19, saving output to
results/text-fox/count_words/
and using parameters
{"min_words": 1}
[0:00:00] Running command:

```
wc
    -w texts/fox.txt
```

Output:

```
9 texts/fox.txt
```

[0:00:00] "count_words(CountWords)<name=fox>" completed in 0:00:00 (h:mm:ss).

Output mapping

```json
{
  "by_name": {
    "upper": "upper.txt",
    "words": 9
  }
}
```
````

- **The header**: which job this is (variable name, class and wildcards), when it started, its
  result folder, and the parameters on line 4, for reading (Snakemake keeps its own record of
  them, to rerun the job when they change; see [When jobs rerun](reruns.md)).
- **The body**: what `run()` did. `job.run()` writes each command and its output; `job.log()`
  adds your own lines. Messages get the time since the job started; the output of commands is
  written as it is.
- **Errors**, if any, under `Errors occurred`, with the lines of your own code where they
  happened (see [Where errors go](errors.md)).
- **The output mapping**: the outputs of the job. File outputs are relative to the result folder;
  values like `words` are stored as they are.

## How the next job reads it

The output mapping is how jobs pass on their results. When `word_report` runs, it reads the log
files of the three `count_words` jobs and finds `words` in each: there is no separate file for
the count. A file output is found the same way, as the result folder (line 2) plus the name in
the mapping. A job whose log file is `.error` has no usable outputs: the jobs that depend on it
fail too, or receive a `JobError` (see [Where errors go](errors.md)).

## Behind the scenes: stamp files

Snakemake decides what to run from files and their dates. Snakeplusplus gives it one small
**stamp file** per job, in `.snakemake/snakeplusplus/stamps/`, and derives these stamps from the log
files each time the Snakefile is loaded: a job with a `.log` file gets its stamp, a job that needs
to run loses it. So the log files are what counts. You never need to touch the stamps; if
`.snakemake/` is deleted, they are recreated from the log files. Snakemake's record of each job's
parameters is lost with it, though: a parameter change then goes unnoticed for jobs that have not
run again since.
