# Log files

Every job has exactly one log file. Its extension shows the state of the job, so the log folder is an overview of the whole pipeline.

!!! note "Outline: to be written"

    - The states: `.queued`, `.running`, `.log`, `.error`, `.stale`
    - The header: job name and start time, result folder, parameters on lines 3 and 4
    - The body: commands, their output, errors; the Markdown layout (renders well in a Markdown viewer)
    - The output mapping at the end, and how downstream jobs read it
    - Log folder and result folders: where each job writes
    - Behind the scenes: the hidden stamp files that Snakemake tracks (`.snakemake/snakeplusplus/stamps/`)
