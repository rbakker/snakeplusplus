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
