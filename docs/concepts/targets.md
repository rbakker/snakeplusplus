# Targets

A target names a set of jobs that Snakemake should produce: `snakemake preprocessing`.

!!! note "Outline: to be written"

    - `target(...)`: rules without wildcards, and loops
    - The name of the target is the name of the variable
    - The default target: the first one in the Snakefile
    - A rule without wildcards is a target by itself
    - Why `all = target(...)` gives a warning, and which names to use instead
