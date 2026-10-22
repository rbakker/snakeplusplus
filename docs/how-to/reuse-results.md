# Reuse costly results

Before a job runs again, the outputs of its previous run are removed (see
[When jobs rerun](../concepts/reruns.md#a-clean-start-for-each-run)). That keeps results
consistent, but sometimes part of a result took hours and has not changed, e.g. a registration
that is the same for every value of a later parameter.

The pattern: compute the costly part into a cache folder outside the result folder, and make the
output a symbolic link to it.

```python
--8<-- "examples/howto/Snakefile:cache"
```

When the job runs again, e.g. after `label` changed, the link is removed and made again, but the
cache file stays, so the job reuses it:

```
[0:00:00] Removed outputs of the previous run: model.txt
[0:00:00] Reusing cache/sub-01-edaf2429.txt
```

Symbolic links are always removed as links: what they point to is never touched.

The cache is your responsibility: snakeplusplus does not know when it is outdated. Put everything
that the cached result depends on in its name, so that a change gives a new file instead of a wrong
one. The example uses a checksum of the input: when `model_checked` gets a hand-corrected file
instead of the output of `smooth`, it computes a new model. Settings that change the result
belong in the name too. Delete the cache folder to start over.

To use results from another pipeline as inputs, without copying them, see `JobLoop.from_pattern()`
in the [Reference](../reference.md).
