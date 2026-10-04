# Run external commands

Run command line tools with their output in the job's log.

!!! note "Outline: to be written"

    - `job.run([...])`: a command as a list, no quoting needed
    - `job.shell("...")` for pipes and redirection
    - Long commands: `update_interval`, and a `formatter` to leave out progress bars
    - Exit codes: `fail_on_error`
    - Threads: `threads_budget` on the rule (`8`, `'cores'`, `'cores/2'`), and `job.threads` passed on to the tool
    - `conda_env` on the rule
