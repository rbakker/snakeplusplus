# Parameters from a config file

Set parameters per rule class in Snakemake's config file, and change them per run on the command
line, without editing the Snakefile.

## A config file with parameters per rule class

```yaml title="config.yaml"
--8<-- "examples/howto/config.yaml:config"
```

```python title="Snakefile"
--8<-- "examples/howto/Snakefile:configure"
```

`configure()` gets the part of the config under `params`, a dict from rule **class** names to
their parameters. All rules of class `Smooth` get `width: 3`, unless they set it themselves.

## Which value wins

For each parameter of a rule, in this order:

1. the value given to the constructor: `Smooth(width=9)`;
2. the value in the config, for the rule's class;
3. the default in the `ParamModel`.

```python
--8<-- "examples/howto/Snakefile:graph"
```

Here `smooth` gets `width: 3` from the config, and `smooth_wide` keeps its `9`. The values a job
used are on line 4 of its log file:

```
"smooth_wide(Smooth)<subject=01>" started at 2026-10-22 11:50:29, saving output to
results/sub-01/smooth_wide/
and using parameters
{"method": "mean", "width": 9}
```

Names and values from the config are checked when the Snakefile is loaded. A parameter that the
class does not have, or a value of the wrong type, is an error in the console:

```
Rule smooth has unused parameters: {'widht'}
```

A class name that no rule of the pipeline has gives a warning, since a config file may be shared
by several pipelines:

```
Warning: the config has parameters for Smoth, but no rule of this pipeline has that class; did you mean Smooth?
```

## On the command line

Snakemake's `--config` changes the config for one run; nested values are merged with the config
file, so this only changes `width`:

```bash
snakemake --config 'params={"Smooth": {"width": 4}}'
```

A changed parameter reruns the jobs that use it, and the jobs that depend on them; see
[When jobs rerun](../concepts/reruns.md). Here that is every `smooth` job and everything
downstream, but not `smooth_wide`, whose `width` comes from its constructor.
