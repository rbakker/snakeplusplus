# Document a pipeline

`snakeplusplus-diagram` writes an HTML page with a diagram of the pipeline and a description of
every rule.

```bash
snakeplusplus-diagram                 # finds the Snakefile like Snakemake does
snakeplusplus-diagram workflow/Snakefile -o docs/pipeline.html
```

Without `-o`, the page is `<Snakefile name>-diagram.html` in the pipeline folder: `Snakefile-diagram.html`
next to the Snakefile, or in the folder above `workflow/`.

Nothing is run, and no file of the pipeline is changed: the Snakefile is only loaded. So the
checks of the [console](../concepts/errors.md#before-the-run-the-console) are made, except those
that look at the files of a run (file permissions, and jobs that would share a log file or result
folder), and a pipeline that does not load gives no diagram.

## What it shows

- **The diagram**: a box per rule with its class and wildcards; arrows for connections, labelled
  with the output; a circle for each loop, with the wildcards it runs over; checkpoints and
  targets in their own shapes; a dashed arrow for a [connection that depends on a parameter](../concepts/connecting.md#connections-that-depend-on-a-parameter).
- **A card per rule**: its description, the log and result folders, the connected inputs, the
  parameters set in the pipeline, and a table of all wildcards, inputs, outputs and parameters with
  their types, defaults and descriptions.

The descriptions come from the code: the docstring of the rule class, and
`Field(description=...)` on each field:

```python
--8<-- "examples/concepts/dwi.smk:denoise"
```

The tutorial shows an example: [Step 5](../getting-started.md#step-5-a-diagram-of-the-pipeline).
