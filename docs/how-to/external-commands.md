# Run external commands

Run command line tools with their commands and output in the job's log file.

```python
--8<-- "examples/howto/Snakefile:commands"
```

## `job.run()`: a command as a list

`job.run(['sort', '-g', f'--parallel={job.threads}', '-o', output.sorted, input.smoothed])` runs the command directly, without
a shell: each item is one argument, so file names with spaces need no quoting. Paths can be
given as they are. The command and its output go into the log:

````
[0:00:00] Running command:

```
sort
    -g
    --parallel=2
    -o results/sub-02/summarize/sorted.txt
    results/sub-02/smooth/smoothed.txt
```
````

`job.run()` returns the exit code and the output (stdout and stderr together), e.g. to read a
number that a tool prints: `code, text = job.run(['mrstats', input.fa, '-output', 'mean'])`.

## `job.shell()`: pipes and redirection

For a pipe or a redirection, use `job.shell("...")` with a shell script as a string. It is run
with `bash`; the job fails if its last command fails. File names are put in with an f-string, so
names with spaces need quotes in the script.

The script is run as it is, so braces need no special care, except that an f-string turns `{{`
into `{`: `awk '{{print $2, $1}}'` in an f-string is `awk '{print $2, $1}'` in the shell.

## Your own messages in the log

`job.log()` writes a line to the log, with the time since the job started, e.g. what `run()`
decided or found:

```python
job.log(f'{len(streamlines)} streamlines left after filtering')
```

```
[0:12:31] 48112 streamlines left after filtering
```

`print()` goes to Snakemake's console output instead, among that of all other jobs.

## When a command fails

A nonzero exit code stops the job: the log file becomes `.error`, with the command's output
above the error, and the line of your code:

```
wc exited with code 1 (log-file: text-fox_count_words.error)
  File "logs.smk", line 30, in run
    code, out = job.run(['wc', '-w', input.text, 'missing.txt'])
```

With `fail_on_error=False`, the error is recorded but `run()` continues, e.g. to run the other
checks of a quality-control step; the job still ends as `.error`. For a tool whose nonzero exit
code is not an error, like `grep` that found nothing, use `job.shell("grep ... || true")`.

## Environment variables

`extra_env` adds variables to the environment of a command, e.g. for the number of threads of a
tool that reads it from the environment:

```python
job.run(['antsRegistration', ...], extra_env={'ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS': job.threads})
job.shell(f"mrcalc ... | mrconvert - {output.fa}", extra_env={'MRTRIX_NTHREADS': job.threads})
```

Values are converted to strings, and `None` removes a variable, e.g. `extra_env={'PYTHONPATH':
None}` to keep a tool from picking up the pipeline's Python packages. The variables are written to
the log with the command:

```
[0:00:00] Running command (with ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS=4):
```

`env` replaces the whole environment instead, as in Python's `subprocess`: the command then gets
only these variables, without `PATH`. That is only useful for a deliberately clean environment.
Variables that should not appear in the logs, such as licence keys, are best set before
starting Snakemake: every job inherits them.

## Long commands

By default the output is written to the log when the command ends. With `update_interval=60`,
it is written every minute while the command runs, so that you can follow it in the log file.
Together with `update_interval`, a `formatter` gets the lines and returns the text to write, e.g. to leave out progress bars:
`formatter=lambda lines: '\n'.join(l for l in lines if 'progress' not in l)`.

## Threads

`threads_budget` on the rule says how many CPU cores a job may use; `job.threads` is the number
it got, to pass on to the tool, as `--parallel={job.threads}` above.

| `threads_budget` | a job gets |
|---|---|
| `1` (default) | one core |
| `8` | 8 cores, or all if fewer were given with `-c` |
| `'cores'` or `'all'` | all cores given with `snakemake -c ...` |
| `'cores/2'`, `'cores-2'` | half of them, all but two (at least one) |

Snakemake runs as many jobs at once as fit in the cores given with `-c`.

## Conda environments

`conda_env = 'envs/mrtrix.yaml'` on the rule runs its jobs in that conda environment, as
Snakemake's `conda:` directive does, when Snakemake is started with `--use-conda`.
