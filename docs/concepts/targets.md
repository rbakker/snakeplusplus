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
