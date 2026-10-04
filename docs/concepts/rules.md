# Rules

A rule is a Python class that describes one step: its wildcards, inputs, outputs and parameters.

!!! note "Outline: to be written"

    - Anatomy of a `SnakeRule`: `result_template`, three models and `run()`
    - `Fixed` and `Extensible` models; a model without base class is `Fixed`
    - `Field`: default values, file name patterns and descriptions
    - `result_template`: the wildcards, and where each job writes: a folder (`'sub-{subject}'`) or a file name prefix (`'sub-{subject}/{hemi}_*'`); where the rule name goes; `log_template`
    - Wildcard values: anything except an empty value or `/`; two jobs may not end up with the same names
    - Parameters: in `ParamModel`, given to the constructor (`Denoise(extent=7)`) or from the config
    - Reuse: one class, several instances with different parameters; inheritance for shared wildcards
    - Names to avoid: a rule name is a variable in Snakemake's own namespace, so Python built-ins (`filter`, `all`) and Snakemake's names (`touch`, `expand`) are refused; Snakemake keywords (`report`, `input`) give a syntax error

    Examples from: `tests/test_pipeline.smk`, `examples/comparison`
