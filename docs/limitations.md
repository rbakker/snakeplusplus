# Limitations

Known limitations, and how to work around them.

## Names in a Snakefile

A Snakefile runs in Snakemake's own namespace, so the names you choose share it with Python and
with Snakemake:

- **Rule and target names** that hide a Python built-in (`all`, `filter`, `input`, ...) or a name
  of Snakemake are refused when the Snakefile is loaded, with a suggestion; see
  [Targets](concepts/targets.md#names).
- **Snakemake's keywords** (`rule`, `checkpoint`, `include`, `report`, `localrules`, `onstart`,
  `configfile`, `container`, ...; the full list is on the [Rules](concepts/rules.md#names) page)
  cannot be variable names in a Snakefile, nor names of fields in the models of a rule class
  defined in the Snakefile. Snakemake reads them as its own syntax before Python sees them, so
  snakeplusplus cannot catch them, and the message is Snakemake's: for a variable `report`,
  `SyntaxError: Colon expected after keyword report`; for an output field `report`, an error
  from Snakemake's own `report()`, or another unrelated message. Rule classes defined in an imported
  Python module are not affected.

## Loops over a checkpoint

The jobs of a loop over the result of a checkpoint are only known once the checkpoint has run.
Before that:

- the pipeline cannot check that those jobs have different log files and result folders, which
  it does for all other jobs;
- to decide what is up to date, snakeplusplus finds the existing jobs of such a loop by the names
  of their log files.

## In the log files

- **Commands** are written one argument per line, with an option and the value that follows it
  on one line. A flag followed by a file name looks the same, e.g. `-w texts/fox.txt` for
  `wc -w texts/fox.txt`; only the layout is affected.
- **The output of `job.shell()`** goes straight into the log. A line in it that starts with three
  backticks ends the Markdown code block early; the content is all there, only the layout in a
  Markdown viewer is affected. The output of `job.run()` is protected against this.

## Outputs

- **A `PathPrefix`** matches every file whose name starts with it, also files of another job whose
  prefix is longer, e.g. `sub-1` and `sub-10`. End prefixes with a separator, like `sub-1_`, or
  give each job its own folder.
- **"Made by this run"** is decided from file dates, with a margin of two seconds, to allow for
  file systems with a coarse time resolution. A file made in the two seconds before the job
  started may therefore be taken for a new one.

## Sharing a pipeline

The [check for files of other users](how-to/shared-pipeline.md#when-files-block-a-run) covers all
jobs of the pipeline that need to run, not only those of the target you asked for: files of
another user elsewhere in the pipeline also stop your run.

## `JobMonitor` on its own

`JobMonitor` can be used in a plain Snakemake rule, to get the log files and their states. Without
snakeplusplus rules, the decisions about reruns described in [When jobs rerun](concepts/reruns.md)
do not apply: Snakemake decides as usual, from files and dates.
