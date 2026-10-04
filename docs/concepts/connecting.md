# Connecting rules

Rules are connected explicitly with `set_input()`; mistakes are reported when the Snakefile is loaded.

!!! note "Outline: to be written"

    - What `set_input()` accepts: outputs of other rules, loops, file names with wildcards, input functions
    - `get_output('name')`, and `get_output()` for the whole `JobResult`
    - Type checking at load time: what is checked, and what the error looks like
    - Parsers: converting an output before it is passed on (`get_output('value', parser=read_int)`), with or without wildcards
    - Picking an output per job: `get_output('DRT_{hemi}')`
    - How a job receives its inputs: an instance of `InputModel`, validated when the job starts
