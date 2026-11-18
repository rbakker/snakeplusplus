# Start a new pipeline

[Download the pipeline template](snakeplusplus-template.zip){ .md-button }

The template is an empty snakeplusplus pipeline, with the folder layout that Snakemake
[recommends](https://snakemake.readthedocs.io/en/stable/snakefiles/deployment.html), adapted to
snakeplusplus, and a hello-world rule to replace:

```
my-pipeline/
├── README.md, LICENSE.md
├── AGENTS.md               # conventions for AI agents (CLAUDE.md points to it)
├── config/
│   ├── config.yaml         # settings for a run, with params per rule class
│   └── samples.json        # the sample sheet
├── workflow/
│   ├── Snakefile           # rule instances, connections and targets
│   ├── rules/              # rule classes, as a Python package
│   │   ├── __init__.py
│   │   └── hello.py
│   ├── scripts/            # scripts that jobs run, and helper modules
│   │   ├── __init__.py
│   │   └── greet.py
│   ├── envs/tool.yaml      # conda environments
│   └── profiles/default/config.yaml   # default command line options
├── resources/              # reference data
├── results/
└── logs/
```

Unzip it, rename the folder, and run it:

```bash
cd my-pipeline
snakemake -n        # what would run
snakemake           # Hello, world!
```

## How it differs from a plain Snakemake pipeline

- **Rule classes are Python modules** in `workflow/rules/`, not `.smk` files. Snakemake's parser
  does not read them, so its keywords are ordinary names there (see
  [Rule classes in a module](concepts/rules.md#rule-classes-in-a-module)). The `__init__.py`
  makes `rules` a regular Python package, so that no installed package of the same name can take
  its place; the same goes for `scripts`.
- **Imports need no `sys.path` changes**: Snakemake puts `workflow/` first on Python's path, so
  the Snakefile and the rule modules import a helper as `from scripts.volume_tools import Volume`.
  A script that a job runs has its own folder on the path, and imports it as
  `from volume_tools import Volume`.
- **The Snakefile sets `workdir:`** to the pipeline folder, so all paths in the code are relative
  to it, wherever `snakemake` is started: `config/config.yaml`, `workflow/scripts/greet.py`.
- **No `pathvars:` block is needed**: Snakemake's default pathvars already give `results/`,
  `resources/` and `logs/` in the pipeline folder, and `configure(workflow.pathvars, ...)` takes
  the log and result folders from them. Add a `pathvars:` block only to put one of them
  elsewhere, e.g. the results on a scratch disk.
- **There is no `report/` folder**: Snakemake's reports mark output files with `report()`, but
  in snakeplusplus Snakemake only sees the stamp files. The log files and `snakeplusplus-diagram`
  take that role; a report of the results is a rule like any other, writing into `results/`.
- **There is no `notebooks/` folder**: Snakemake's `notebook:` directive has no counterpart in
  snakeplusplus.

## With an AI agent

`AGENTS.md` tells coding agents how a snakeplusplus pipeline is organized: where rule classes go,
which names to avoid, to start with a dry run, that a change in a rule's code does not rerun its
jobs, and never to touch `.snakemake/`. Agents that read `AGENTS.md` (or `CLAUDE.md`, which points
to it) then build on the template instead of inventing their own layout. Give the agent the
template, and ask it to replace the example with your pipeline.
