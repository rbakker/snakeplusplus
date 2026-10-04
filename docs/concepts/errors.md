# Where errors go

Mistakes that can be known in advance are reported in the console; once jobs run, errors go to log files and Snakemake continues.

!!! note "Outline: to be written"

    - Before the run (console): type mismatches, unknown parameters or outputs, wildcards without `foreach`, undefined names in `run()`
    - During the run (log files): exceptions in `run()`, failed commands, missing outputs, input type mismatches
    - How an error looks in a log: the message, and the lines of your own code
    - `raise JobError('...')` for a clear message without traceback
    - A failed job upstream: downstream jobs fail with the original error in their log
    - Retrying failed jobs, and turning that off
