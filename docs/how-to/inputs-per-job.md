# Choose an input per job

When the connection depends on the job.

!!! note "Outline: to be written"

    - A pattern: `get_output('DRT_{hemi}')`; checked when the Snakefile is loaded
    - An input function that returns a connection, e.g. a different rule per subject
    - A manual override: a hand-corrected file if it exists, else the output of a rule
    - What you lose with a function: type checks only at run time, no edge in the pipeline diagram
