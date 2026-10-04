# Loops and checkpoints

A loop connects the outputs of many jobs of one rule; a checkpoint decides at run time what the loop contains.

!!! note "Outline: to be written"

    - `foreach` with lists: `rule.foreach(subject=['01', '02'])`; several wildcards of equal length; scalar loops
    - `foreach` with a function that yields wildcard dicts
    - Wildcards that are passed on from the receiving job (e.g. loop over sessions within a subject)
    - `SnakeCheckpoint` and `JobResult.from_checkpoint()`
    - What happens when a loop's function fails: in the console before the run, in the log during the run

    Examples from: `tests/test_pipeline.smk`
