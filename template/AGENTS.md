# Instructions for AI agents

This is a [snakeplusplus](https://rbakker.github.io/snakeplusplus/) pipeline: Snakemake, with each
step written as a Python class with typed inputs, parameters and outputs. Follow the conventions
below, and the documentation, instead of writing plain Snakemake rules. When in doubt, read the
[Rules](https://rbakker.github.io/snakeplusplus/concepts/rules/) and
[Connecting rules](https://rbakker.github.io/snakeplusplus/concepts/connecting/) pages first.

## Where things go

| path | contents |
|---|---|
| `workflow/Snakefile` | the pipeline: rule instances, their connections, the targets |
| `workflow/rules/*.py` | rule classes, one module per topic, in the package `rules` (keep its `__init__.py`) |
| `workflow/scripts/` | scripts that jobs run with `job.run()`, and helper modules, in the package `scripts` (keep its `__init__.py`) |
| `workflow/envs/*.yaml` | conda environments, for `conda_env = 'envs/<name>.yaml'` |
| `workflow/profiles/default/config.yaml` | default command line options for `snakemake` |
| `config/config.yaml` | settings for a run, including `params:` per rule class |
| `config/samples.json` | the sample sheet |
| `resources/` | reference data the pipeline uses but does not make (`RESOURCES` in the Snakefile) |
| `results/`, `logs/` | made by the pipeline: never edit them by hand |

Paths in the code are relative to the pipeline folder (the Snakefile sets `workdir:`), e.g.
`'workflow/scripts/greet.py'`. Conda environment files are relative to `workflow/`.

Imports need no changes to `sys.path`: Snakemake puts `workflow/` first on Python's path. In the
Snakefile, in rule modules and in `run()`, import helpers as `from scripts.volume_tools import
Volume`. A script that a job runs, e.g. `workflow/scripts/fit.py`, has its own folder on the path,
so it imports the same helper as `from volume_tools import Volume`.

## Starting from this template

Replace the example: `workflow/rules/hello.py`, `workflow/scripts/greet.py`, the `Greet` rule in
the Snakefile and in `config/config.yaml`, the entries of `config/samples.json`, and
`workflow/envs/tool.yaml` if no rule needs it. Rewrite `README.md` for the pipeline, and leave
`LICENSE.md` to the user. Keep the folders and these conventions.

## Writing rules

- **A rule is a class** derived from `SnakeRule` (or `SnakeCheckpoint`), with `InputModel`,
  `ParamModel` and `OutputModel`, a `result_template` with the wildcards, and
  `run(self, job, input, output, params, wildcards)`. Give every class a docstring and every field
  a `Field(description=...)`: the diagram and the documentation are made from them.
- **Rule classes go in a module in `workflow/rules/`**, imported by the Snakefile:
  `from rules.dwi import Denoise`.
- **Rule classes in the Snakefile itself are allowed**, and the user may write one there. That is
  not a mistake: do not move it unless asked. But code in a Snakefile must not use Snakemake's
  keywords as names, neither as variables (also not inside functions such as `run()`) nor as
  fields of the models: `rule`, `checkpoint`, `include`, `module`, `use`, `report`, `conda`,
  `container`, `singularity`, `containerized`, `envvars`, `workdir`, `configfile`, `pepfile`,
  `pepschema`, `ruleorder`, `localrules`, `storage`, `pathvars`, `inputflags`, `outputflags`,
  `resource_scopes`, `subworkflow`, and as variables also `onstart`, `onsuccess`, `onerror`,
  `wildcard_constraints` and `scattergather`. Snakemake reads these as its own syntax, before any
  Python runs, and stops with a syntax error that snakeplusplus cannot catch. In a module they
  are ordinary names: that is why rule classes are best put in modules.
- **Rule and target names** (the variable names in the Snakefile) cannot hide a name of Python
  or Snakemake: not `all`, `filter`, `input`, `map`, `expand`, ... Use e.g. `everything` as the
  name of the main target.
- **Commands**: `job.run(['tool', '-option', input.x, output.y])`, with the command as a list and
  no quoting; `job.shell(f"...")` only for pipes and redirection. Pass `job.threads` to tools
  that can use several cores, with `threads_budget` on the rule. Environment variables go in
  `extra_env={...}`. Messages for the log: `job.log('...')`, not `print()` and not
  `job.run(['echo', ...])`.
- **Errors**: raise `JobError('...')` for a clear message. The job then fails on its own, and
  the rest of the pipeline continues.
- **Parameters**: in the `ParamModel`, with a default. Values for a run go in
  `config/config.yaml`, under `params:` and the rule's class name, not in the code.

## Connecting rules

- `b = B().set_input(x=a.get_output('y'))`: connect outputs to inputs; the types are checked
  when the Snakefile is loaded. A plain file name, e.g. `'data/sub-{subject}/T1w.nii.gz'`, is an
  input too.
- Loops: `a.foreach(subject=SUBJECTS)`, one wildcard per call; chain the calls for all
  combinations: `a.foreach(subject=SUBJECTS).foreach(session=SESSIONS)`. For specific
  combinations, e.g. subjects that each have other sessions, give a list of dicts:
  `a.foreach([dict(subject='01', session='a'), dict(subject='02', session='b')])`, or, in a chain,
  a function of the outer wildcards: `.foreach(session=lambda wc: SESSIONS[wc.subject])`. Never
  pass two wildcards to one `foreach()`: that is an error, since it could mean all combinations
  or pairs. `a.where(subject='01')` is a single job.
- An optional input can be connected to `None`, e.g. `set_input(mask=config.get('mask'))`.
- Avoid input functions where a pattern, `when()` or a checkpoint output can say the same: they
  lose the checks when the Snakefile is loaded, and the arrows in the diagram.

## Running

- **Always do a dry run first**: `snakemake -n`. Without `-q`, it shows the reason why each job
  would run. To keep it short, put the target first: `snakemake everything -n -q rules`
  (`-q` takes several values, so a target after it is read as one of them).
- **One log file per job** in `logs/`: `.log` succeeded, `.error` failed (read it: it says why),
  `.queued`, `.running`, `.stale` (interrupted). Snakemake itself does not stop for a failed job.
- **Never touch `.snakemake/`** or the stamp files in it: snakeplusplus derives them from the
  log files.
- **To rerun a job**, delete its log file. **To rerun all jobs of a rule**, use
  `snakemake -R <rule>`.
- **A change in a rule's code does not rerun its jobs.** After a change that affects the
  results, rerun the rule with `snakemake -R <rule>`, or tell the user that this is needed. Do
  not claim that changed rules rerun by themselves.
- **Ask before deleting results** or log files in bulk: some jobs may have taken hours.

## Checking your work

- `snakemake -n` loads the Snakefile and checks connections, types, parameter names and names
  used in `run()`, without running anything.
- `snakeplusplus-diagram` writes `Snakefile-diagram.html`: a diagram of the pipeline, and a
  description of every rule.
- Each target, when it finishes, writes `results/<target>/diagram.html` and
  `results/<target>/timeline.html`: the timeline shows which jobs failed, how long each job and
  command took, and their peak memory. `snakeplusplus-timeline -t <target>` writes it without
  running anything.
