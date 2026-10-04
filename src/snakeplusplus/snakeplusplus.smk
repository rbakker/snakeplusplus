# Snakemake part of snakeplusplus, included by snakeplusplus.build(locals()) at the end of a Snakefile.
#
# This file defines inject_rule(), which build() uses to turn every SnakeRule object in the
# Snakefile into a Snakemake rule. It must be a Snakefile (not Python), because rule and
# checkpoint definitions need Snakemake's syntax.


def checkpoint_magic(fn):
    return lambda wildcards: fn(wildcards)

def inject_rule(r):
    if r.checkpoint:
        checkpoint:
            name: r.name
            input: r.input
            params: **r.params
            output: r.output
            wildcard_constraints: **r.wildcard_constraints
            threads: r.threads
            default_target: r.default_target
            conda: r.conda
            run:
                r.run_job(locals())
    else:
        rule:
            name: r.name
            input: r.input
            params: **r.params
            output: r.output
            wildcard_constraints: **r.wildcard_constraints
            threads: r.threads
            default_target: r.default_target
            conda: r.conda
            run:
                r.run_job(locals())
