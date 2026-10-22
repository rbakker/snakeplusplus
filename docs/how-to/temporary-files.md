# Temporary files

Every job can have its own temporary folder, which is deleted when the job ends, also when it
fails.

```python
--8<-- "examples/howto/Snakefile:smooth"
```

- `job.tmpdir()` is the folder, as a `Path`. It is created on first use, so a job that does not
  need one does not get one.
- For a unique file name, e.g. for several files from a loop, use
  `tempfile.mkstemp(dir=job.tmpdir())`.
- The folder is in the system's temporary location, given by the `TMPDIR` environment variable.
  On a cluster, set `TMPDIR` to a disk of the compute node itself, for speed and to keep the
  shared file system clean.

## Keeping the folder, for debugging

```python
smooth = Smooth().set_input(numbers='data/sub-{subject}.txt')
smooth.tmpdir_autodelete = False
```

The folders of this rule's jobs are then kept, and the log file says where:

```
[0:00:00] Temporary folder kept: /tmp/smooth_Smooth_subject_01_-1fdglbs5
```

`tmpdir_autodelete` can also be set on the class, for all rules of that class.
