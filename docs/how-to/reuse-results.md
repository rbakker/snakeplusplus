# Reuse costly results

Keep a result that is expensive to compute when a job reruns.

!!! note "Outline: to be written"

    - Why outputs are removed before a rerun
    - The pattern: compute into a cache folder, make the output a symbolic link to it
    - Links are removed as links, the cache stays
