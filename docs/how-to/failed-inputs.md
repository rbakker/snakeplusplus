# Continue when some jobs fail

A group analysis over 50 subjects should not wait for the 2 that failed.

!!! note "Outline: to be written"

    - `list[Path | JobError]` in `InputModel`: failed jobs arrive as `JobError`
    - Separating results and failures in `run()`, and recording which subjects were left out
    - What happens after the failed jobs are fixed: the group job reruns
    - When not to do this: analyses that need every subject
