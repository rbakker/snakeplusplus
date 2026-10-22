# Outputs that are not one file

Most outputs are a single file with a fixed name, `Field('smoothed.txt')`. This page shows the
other kinds, all in one rule from `examples/howto/Snakefile`:

```python
--8<-- "examples/howto/Snakefile:outputs"
```

| output | declared as | in `run()` |
|---|---|---|
| a value | `mean: float` | `output(mean=...)`; stored in the log file, no file is made |
| a folder | `plots: Path = Field('plots')` | create it, and write into it |
| an optional file | `warnings: Path \| None = Field(...)` | write it, or not |
| files with a common prefix | `parts: PathPrefix = Field('part_')` | give `output.parts` to a tool as its prefix |
| one file per key | `values: JobLoop[Path] = Field('value_{index}.txt')` | `output.values(index=i)` for each file |
| a name known only at run time | `best: Path` (no default) | `output(best=f'best_{best:g}.txt')` |

After `run()`, each output is checked: files and folders must exist, values must have been given
and have the right type. An optional output that was not made is simply left out, and the
receiving job gets `None`.

## What the receiving job gets

```python
--8<-- "examples/howto/Snakefile:receive"
```

- **A prefix** arrives as a `PathPrefix`, a `Path` with `matches()`: the files that start with it,
  here `part_aa`, `part_ab` and `part_ac`, as `split` named them.
- **Files per key** arrive as a `JobLoop[Path]`, with the keys as wildcards: `wc.index`. If the
  receiving rule has a wildcard with the name of a key, it gets just that one file; see
  [Keys work like wildcards](../concepts/connecting.md#keys-work-like-wildcards).
- **An optional file** arrives as `None` when it was not made, so its input must be optional too
  (`Path | None`); this is checked when the Snakefile is loaded.
- **A value** arrives as the value: `mean: float` in the receiving `InputModel`.

## Tools that write the files themselves

For `JobLoop[Path]` outputs, a tool that writes its own files can get `output.values.prefix` (the
path up to the first key) or `output.values.pattern` (the full path with `{index}`). When `run()`
records no files with `output.values(...)`, the files that match the pattern and were made by
this job are found after it, and the keys are read from their names.
