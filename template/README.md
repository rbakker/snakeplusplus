# My pipeline

What this pipeline does, in a few sentences.

It is a [snakeplusplus](https://rbakker.github.io/snakeplusplus/) pipeline, and runs with Snakemake.

## Running it

```bash
pip install snakeplusplus
snakemake -n              # a dry run: what would run, and why
snakemake                 # run it
snakeplusplus-diagram     # a diagram of the pipeline, in Snakefile-diagram.html
```

Settings for a run are in `config/config.yaml`, and the samples in `config/samples.json`.
Results go to `results/`, and one log file per job to `logs/`: its extension shows whether the
job succeeded (`.log`) or failed (`.error`).

## Folders

| folder | contents |
|---|---|
| `config/` | settings and the sample sheet, for one run |
| `workflow/` | the code: `Snakefile`, rule classes in `rules/`, scripts, conda environments |
| `resources/` | reference data that the pipeline uses but does not make |
| `results/` | the results, one folder per job |
| `logs/` | one log file per job |
