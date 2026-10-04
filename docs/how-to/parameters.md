# Parameters from a config file

Set parameters per rule class in Snakemake's config, and override them per run.

!!! note "Outline: to be written"

    - `configure(workflow.pathvars, config.get('params'))` and a `config.yaml` with parameters per rule class
    - Priority: constructor, then config, then the default in `ParamModel`
    - `--config` on the command line
    - A changed parameter reruns the affected jobs
