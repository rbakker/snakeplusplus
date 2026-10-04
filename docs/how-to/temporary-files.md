# Temporary files

A temporary folder per job, deleted when the job ends.

!!! note "Outline: to be written"

    - `job.tmpdir()`, subfolders and file names
    - Unique names with `tempfile.mkstemp(dir=job.tmpdir())`
    - Keeping the folder for debugging: `myrule.tmpdir_autodelete = False`
    - Where it is: `TMPDIR`, which matters on clusters
