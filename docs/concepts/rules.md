# Rules

A rule describes one step of a pipeline: where its jobs write, what they take in, what they
produce, which parameters they have, and what they do. A rule is a Python class derived from
`SnakeRule`; it runs as many *jobs*, one for each combination of wildcard values.

The examples on this page come from `examples/concepts/dwi.smk`, a small diffusion MRI pipeline.

## Anatomy of a rule

```python
--8<-- "examples/concepts/dwi.smk:denoise"
```

| Part | What it says |
|---|---|
| docstring | what the rule does; shown in the [pipeline diagram](../how-to/pipeline-documentation.md) |
| `result_template` | the wildcards, and where each job writes (see below) |
| `threads_budget` | at most how many threads a job uses; see [Run external commands](../how-to/external-commands.md) |
| `InputModel` | the inputs, with their types |
| `OutputModel` | the outputs, with their default file names |
| `ParamModel` | the parameters, with their default values |
| `run()` | the work of one job |

All parts are optional. A rule without wildcards runs once; a rule without inputs, such as a
first step that reads raw data from a fixed location, simply has no `InputModel`.

## The three models

The models are [pydantic](https://docs.pydantic.dev) models, declared as nested classes:

- **Types** are checked: when rules are connected (see [Connecting rules](connecting.md)), when
  parameters are given, and when a job starts. A file is a `Path`; a number an `int` or `float`;
  any type works, including your own classes.
- **`Field(default, description=...)`** gives a default value and a description. The description
  appears in the pipeline diagram.
- **A model declared without base class**, such as `class InputModel:`, only accepts the fields it
  declares (it becomes a `Fixed` model). A misspelled name is an error. Derive from
  `Extensible` instead to also accept undeclared fields, e.g. for a rule that takes any number of
  inputs.
- **A misspelled model name**, such as `outputModel`, is reported when the Snakefile is loaded:
  `did you mean OutputModel?`

### Outputs

An output of type `Path` is a file in the job's result folder; its default is the file name,
relative to that folder. In `run()`, `output.denoised` is the full path:
`results/sub-01/denoise/dwi_denoised.mif`. After `run()`, every output must exist, and must
have been made by this run; otherwise the job fails with a clear message.

Not every output is a single file with a fixed name:

- **Optional outputs**: `mask: Path | None = Field('mask.mif')`; a missing file is no error.
- **Values instead of files**, declared in `run()` with `output(name=value)`:

    ```python
    --8<-- "examples/concepts/dwi.smk:value_output"
    ```

    The value is stored in the job's log file, where other rules read it.

- **A prefix** for tools that write several files with a common start: `PathPrefix`. See
  [Outputs that are not one file](../how-to/outputs.md).
- **Several files from one job**, e.g. one per parameter value, when separate jobs would be too
  small: `tracts: JobLoop[Path] = Field('tract_{param}.tck')`. `{param}` is not a wildcard but a
  key within the job. In `run()`, `output.tracts(param=p)` gives (and records) the path of one
  file; files that a tool writes by itself, e.g. given `output.tracts.prefix`, are found after the
  job by their names. A rule that receives this output gets a `JobLoop` with the keys as
  wildcards; over a loop of jobs, one flat `JobLoop` with the wildcards of the jobs and the keys.
  A receiving rule that has a key as wildcard gets just the file for its value; see
  [Keys work like wildcards](connecting.md#keys-work-like-wildcards).

Inside `run()`, file outputs are `Path` objects, so `output.summary.write_text(...)` works; in a
command list they can be used as they are.

Inside `run()`, a name that is not an output is an error, not `None`:
`there is no output 'denoized'; the outputs of this rule are: denoised, noise`.

## Where a job writes: `result_template`

`result_template` names the wildcards of the rule, and where each job writes, relative to the
`results` folder:

| `result_template` | A job writes to | Its log file |
|---|---|---|
| *(not set)* | `results/denoise/` | `logs/denoise.log` |
| `'sub-{subject}'` | `results/sub-01/denoise/` | `logs/sub-01_denoise.log` |
| `'sub-{subject}/ses-{session}'` | `results/sub-01/ses-1/denoise/` | `logs/sub-01_ses-1_denoise.log` |
| `'sub-{subject}/{hemi}_*'` | `results/sub-01/L_denoise_<output>` | `logs/sub-01_L_denoise.log` |

- **A folder or a prefix.** A template that ends with `*` is a prefix for the job's file names,
  so several rules can write in one folder without deep folder structures. Any other template
  is a folder per job.
- **The rule name** is added as the last part, so that rules with the same wildcards never write
  to the same place. Put `{rule}` in the template to choose its position yourself.
- **The log file** has the same name, with `/` replaced by `_`. Parts that only repeat wildcards
  that come later are left out, so a BIDS-style template such as
  `'sub-{subject}/ses-{session}/sub-{subject}_ses-{session}_*'` gives `sub-01_ses-1_denoise.log`.
  Set `log_template` to choose the log name yourself; it must have the same wildcards.
- **Wildcard values** can be anything but empty, and cannot contain `/`. Two jobs that would end
  up with the same log file or result folder are reported before anything runs.

## Parameters

```python
denoise = Denoise()               # extent = 5, the default
denoise_wide = Denoise(extent=7)  # another instance, with its own name, folders and log files
```

Parameters come from three places, in this order of priority:

1. the constructor: `Denoise(extent=7)`, or `Denoise(params=dict(extent=7))`;
2. the config, per rule class: `{'Denoise': {'extent': 7}}`, see
   [Parameters from a config file](../how-to/parameters.md);
3. the default in `ParamModel`.

Names and values are checked at the line where the rule is created:

```
TypeError: Denoise: unknown parameter(s) ['extnt']; Denoise.ParamModel has ['extent']
ValueError: Denoise: invalid value for parameter 'extent'.
  Input should be a valid integer, unable to parse string as an integer
```

A parameter can also be a function of the wildcards, `Denoise(extent=lambda wildcards: ...)`,
for a value that differs per job; it is checked when the job starts. The parameters of a job are
written on line 4 of its log file. When they change, the job runs again.

## Reusing rules

- **Several instances** of one class, each with its own parameters, are independent rules: each
  gets its own name (the variable name), result folders and log files.
- **Inheritance** shares what rules have in common, e.g. their wildcards:

    ```python
    --8<-- "examples/concepts/dwi.smk:base"
    ```

- **Rules in a module.** Rule classes can live in an ordinary Python module, imported in the
  Snakefile; only creating and connecting them happens in the Snakefile.

## Names

A rule's name is the name of its variable in the Snakefile: `denoise = Denoise()` is the rule
`denoise`, and its jobs show up as `denoise` in Snakemake's output. Because a Snakefile runs in
Snakemake's own namespace, some names cannot be used:

- **Python built-ins and Snakemake's own names**, such as `filter`, `all`, `map`, `touch` or
  `expand`, are refused when the Snakefile is loaded, with a suggestion: overwriting them can make
  Snakemake itself fail.
- **Snakemake's keywords** cannot be variable names in a Snakefile, nor field names of models
  declared in a Snakefile: `rule`, `checkpoint`, `include`, `module`, `use`, `report`, `conda`,
  `container`, `singularity`, `containerized`, `envvars`, `workdir`, `configfile`, `pepfile`,
  `pepschema`, `ruleorder`, `localrules`, `storage`, `pathvars`, `inputflags`, `outputflags`,
  `resource_scopes`, `subworkflow`, and as variable names also `onstart`, `onsuccess`,
  `onerror`, `wildcard_constraints` and `scattergather`. Snakemake reports these as a syntax
  error. Rules in an imported module can use them as field names.

## Other settings

| Attribute | Meaning |
|---|---|
| `threads_budget` | at most this many threads per job: a number, or `'cores'`, `'cores/2'`, `'cores-2'`; the job gets `job.threads` |
| `conda_env` | a conda environment for the jobs, with Snakemake's `--use-conda` |
| `tmpdir_autodelete` | `False` keeps the job's temporary folder, for debugging; also per instance: `denoise.tmpdir_autodelete = False` |
| `description` | a description for the diagram, if the docstring is not what you want to show |
