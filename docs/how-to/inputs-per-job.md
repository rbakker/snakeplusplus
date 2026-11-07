# Choose an input per job

Usually all jobs of a rule get their inputs from the same place. When the source depends on the
job, there are several ways, from the most to the least checked.

## An input that is not always given

An optional input (`mask: Path | None` in the `InputModel`) can be connected to `None`, so a
value from the config can be passed as it is:

```python
fit = Fit().set_input(dwi=denoise.get_output('denoised'), mask=config.get('mask'))
```

When the config has no `mask`, the jobs get `None`. For an input that is not optional, `None` is
an error when the Snakefile is loaded.

## An output that depends on a wildcard

When a rule has outputs per hemisphere, `DRT_L` and `DRT_R`, a pattern picks the one for each
job: `get_output('DRT_{hemi}')`. For outputs with several files, the keys are filled in by the
wildcards of the receiving rule. Both are checked when the Snakefile is loaded; see
[Choosing an output per job](../concepts/connecting.md#choosing-an-output-per-job).

## An input that depends on a parameter

`get_output('x').when(use_alt=True)` makes the connection only for jobs with that parameter
value; see [Connections that depend on a parameter](../concepts/connecting.md#connections-that-depend-on-a-parameter).

## Files that a checkpoint found

When the files come from a checkpoint, e.g. the results of another pipeline, the checkpoint can
record them as an output with keys, with `output.data.add(path, subject=s)`; a receiving rule
with the wildcard `subject` gets its own file. See
[Checkpoints](../concepts/loops-and-checkpoints.md#checkpoints).

## Anything else: an input function

An input can be a function of the wildcards that returns a file name, or a connection to another
rule. A common case is a hand-corrected file that replaces a computed one, when it exists:

```python
--8<-- "examples/howto/Snakefile:manual"
```

When `manual/sub-01-smoothed.txt` appears, the `model_checked` job for subject 01 runs again with
it; the other subjects keep the output of `smooth`. A function can also pick a different rule per
subject, e.g. for subjects with a different scan protocol.

What you give up with a function:

- **type checks** happen when the job runs, not when the Snakefile is loaded;
- **the diagram** shows no arrow for it, since the connection is only known per job; the rule's
  card lists the function.

So use a function only when a pattern or `when()` cannot express the choice.
