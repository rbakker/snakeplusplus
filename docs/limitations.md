# Limitations

Known limitations, and how to work around them.

!!! note "Outline: to be written"

    - Rule names are variables in Snakemake's own namespace: built-ins and Snakemake's names are refused, Snakemake keywords give a syntax error; field names in imported modules are fine
    - Loops over the result of a checkpoint cannot be expanded before the checkpoint has run; existing jobs are found by their log file names
    - In the log, `format_command` cannot tell a flag from an option with a value
    - Output written by `job.shell` is not protected against lines that look like a Markdown code fence
    - A `PathPrefix` also matches files of a job whose prefix starts with yours
    - Timestamps: file systems with a coarse time resolution
    - `JobMonitor` without snakeplusplus rules: logs and states work, reruns and retries do not
