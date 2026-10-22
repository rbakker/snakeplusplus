# Share a pipeline with colleagues

When several people run the same pipeline in a shared folder, each of them must be able to
replace the files that the others made: a job that runs again replaces its log file and removes
the outputs of its previous run. On Linux, three things decide whether that works:

- **the group** of new files: normally the creator's own group. Set the *setgid* bit on the
  pipeline folder once, and new files and folders get the folder's group instead, e.g. a group
  that the whole team is in:

    ```bash
    chgrp -R journalclub /data/pipeline
    find /data/pipeline -type d -exec chmod g+s {} +
    ```

- **the permissions** of new files: the *umask* of the person who runs the pipeline, often `022`,
  which leaves the group without write permission. `configure(umask=...)` sets it for the whole
  pipeline, whoever runs it:

    ```python
    snakeplusplus.configure(workflow.pathvars, config.get('params'), umask=0o002)
    ```

    It applies to everything the pipeline creates: logs, result folders, Snakemake's own files,
    and the files of the tools that jobs run. `0o002` gives the group write permission;
    `0o007` does that too, and gives others no access at all.

- **who can enter** the folder at all: the permissions of the folder itself, e.g. `chmod 2770`
  for the owner and the group only.

## When files block a run

If files of another user stop the pipeline, snakeplusplus says so before any job runs, with the
owner and the command that fixes it:

```
The pipeline cannot run here, because you (bob) cannot write in:
  logs/      owner alice, group journalclub, drwxr-sr-x
  results/   owner alice, group journalclub, drwxr-sr-x
Ask alice to make their files writable for a group you share, e.g.:
  find logs results -user alice -exec chmod g+w {} +      (run by alice)
To prevent this, use configure(..., umask=0o002) in the Snakefile, and set the setgid bit on the
pipeline's folders (chmod g+s) so that new files get their group.
```

The same check is made for each job that needs to run. For jobs that are up to date but could
not run again, e.g. after a parameter change, it is a warning. A job that fails on a file it
cannot write reports the owner in its `.error` file.

A umask only affects new files. A tool that sets restrictive permissions itself, e.g. a temporary
file with `0600`, still needs a `chmod` afterwards.
