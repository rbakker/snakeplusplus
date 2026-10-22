# When jobs rerun

Snakeplusplus decides what to run from the [log files](log-files.md), the parameters and the
inputs of each job, not from the dates of result files, nor from code. A job runs again when:

| | |
|---|---|
| its log file is missing | it never ran, or you deleted the log file |
| its log file is `.error` | it failed; with `configure(..., retry_failed=False)` it stays failed until you delete the file |
| its log file is `.stale` | it was interrupted |
| its parameters changed | Snakemake keeps the parameters of each job's last run, and compares them; line 4 of the log shows them |
| a job it depends on runs | its inputs may change |
| an input file changed | for inputs given as a file name, like `'texts/{name}.txt'`: Snakemake notices a changed file (a newer date and different content) |
| its set of inputs changed | e.g. a loop got another member, or an input function returns another file |

And it does **not** run again when:

- **the code changed.** After fixing a bug in `run()`, only the failed jobs run again, and those
  that depend on them; the jobs that succeeded keep their results. To rerun the jobs of a rule
  after a change that matters, use Snakemake's `-R`: `snakemake -R count_words`.
- **a file changed that the job reads but does not have as an input**, e.g. a lookup table that
  `run()` opens from a fixed location. Delete the log file of the job that reads it.

## An example

In `examples/concepts/logs.smk`, three texts are counted. Raising the minimum number of words
for all of them, in the config:

```bash
snakemake --config 'params={"CountWords": {"min_words": 2}}'
```

runs the three `count_words` jobs again, since their parameter changed, and `word_report` after
them. The text `short.txt` has only one word, so its job fails:

```
logs/
    everything.error
    text-fox_count_words.log
    text-hamlet_count_words.log
    text-short_count_words.error
    word_report.error
```

After adding a word to `short.txt`, the same command runs only `count_words` for `short`, which
had failed, and the two jobs that depend on it. The other two jobs are up to date.

## Running a job again by hand

Delete its log file: `rm logs/text-fox_count_words.log`. The next run does that job and the jobs
that depend on it. A dry run (`snakemake -n`) always shows the same plan as the real run that
follows it.

## A clean start for each run

Before a job runs again, the outputs of its previous run are removed, as listed in its log file,
so that no old file is mistaken for a new one:

```
[0:00:00] Removed outputs of the previous run: upper.txt
```

Only the outputs listed in the previous log are removed, and to protect files that the job did
not make, only if they are inside the job's result folder, were changed after the previous run
started, and are not (and do not contain) one of the job's inputs. Outputs of the previous run in
the result folder that do not meet these conditions are kept, and listed in the log.

After `run()`, every required output must exist. A file output that `run()` did not use must also have been
made by this run: a file that was already there is reported as `exists, but was not created by
this run`. See [Where errors go](errors.md) for the other messages.
