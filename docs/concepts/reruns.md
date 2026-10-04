# When jobs rerun

Snakeplusplus decides what to run from the log files, not from file dates or code.

!!! note "Outline: to be written"

    - A job runs again if its log file is missing (never ran, or deleted), `.stale`, or `.error` (with `retry_failed`)
    - ... or its parameters changed, or a job it depends on runs again
    - Code changes do not trigger reruns: after a bug fix, only the failed jobs run again
    - Deleting a log file as the way to rerun a job; a dry run shows the same plan as the real run
    - Before a rerun: the outputs of the previous run are removed, and the guards that protect other files
    - After `run()`: outputs are checked, and only files made by this run count
