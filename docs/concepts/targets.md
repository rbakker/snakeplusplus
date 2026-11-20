# Targets

A target names a set of jobs that Snakemake should produce, so that you can run part of a
pipeline by name: `snakemake test_run`.

```python
--8<-- "examples/concepts/loops.smk:targets"
```

## `target()`

`target()` takes rules and loops, any number of them. Snakemake runs all their jobs, and the jobs
these depend on:

- **a rule without wildcards**, like `group_table`: its one job;
- **a loop**, like `subject_mean.foreach(subject=SUBJECTS)`: the jobs of the loop;
- **a single job**, like `measure.where(subject='01', session='a')`: just that one.

A rule with wildcards needs a loop or `where()`, since otherwise it is not clear which jobs to run:

```
ValueError: target(): rule SubjectMean has wildcards {subject}; use foreach(...)
```

## A diagram and a timeline

When a target's job runs, it writes two pages into its result folder, e.g. `results/test_run/`:

- **`diagram.html`**: the rules that this target needs and how they are connected, with a
  description of each rule, as `snakeplusplus-diagram` makes it for the whole pipeline;
- **`timeline.html`**: when each of the target's jobs ran, with the commands inside each job,
  failed jobs in red, and a link to each job's log file. It also gives the totals: the compute
  time (the sum of all job durations), the critical path (the longest chain of jobs that wait for
  each other, so the shortest possible duration with enough cores), and a table per rule with
  the median and maximum time and memory of its jobs.

The timeline is made from the log files of the jobs that the target depends on, whichever
Snakemake runs made them. So a target gives the same totals whether it was made in one run, or
in several, e.g. after a preliminary target that shares part of its jobs. Long waits between
runs are drawn shortened, with their duration.

The pages are also written when a job that the target depends on failed: the target still ends
as `.error`, but its timeline shows which jobs failed, and why. A target's job runs again
whenever one of its jobs ran, so its pages are always up to date.

Both can be switched off in the config, in a `snakeplusplus:` section that the Snakefile passes
to `configure()`:

```yaml title="config/config.yaml"
snakeplusplus:
  diagram: true
  timeline: false
```

```python
snakeplusplus.configure(workflow.pathvars, config.get('params'), options=config.get('snakeplusplus'))
```

`snakeplusplus-timeline` writes the timeline of a target without running anything, to the same
place: `snakeplusplus-timeline -t test_run` writes `results/test_run/timeline.html`. It can be
used at any time, also while the pipeline runs: see
[Following a run](log-files.md#following-a-run).

## Names

The name of a target is the name of its variable: `test_run = target(...)` is run with
`snakemake test_run`. A target is a rule of its own, so it also gets a log file,
`logs/test_run.log`, which shows when the target was last completed.

A rule without wildcards is a target by itself: `snakemake count` runs the `count` job and what
it needs, without a `target()` around it.

When Snakemake gets no target name, it runs the **default target**: the first `target()` in the
Snakefile, or, if there is none, the first rule without wildcards. In the example,
`snakemake` runs `everything`.

Like the names of rules, target names cannot hide a name of Python or of Snakemake, because a
Snakefile runs in Snakemake's own namespace. The usual Snakemake name `all` is therefore refused:

```
NameError: These rule names cannot be used, because the Snakefile runs in Snakemake's own namespace:
  'all' would hide Python's built-in all()
Choose other names, e.g. with a suffix: 'filter_tracts' instead of 'filter'.
```

Use a name like `everything` or `all_subjects` instead.

!!! tip "Snakemake's `-q` takes several values"

    `snakemake -q rules test_run` reads `test_run` as a second value of `-q`, and fails. Put the
    target first: `snakemake test_run -q rules`.
