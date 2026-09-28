# How Snake++ compares

Snake++ (`snakeplusplus`) is a layer on top of Snakemake. It keeps Snakemake's scheduler, cluster executors
and conda support, but changes how you describe a pipeline: rules are Python classes with typed
inputs and outputs, you connect them explicitly, and each job's log file is the only thing the DAG
tracks. This page compares those design choices with plain Snakemake, Nextflow and Pydra, so you can
decide which tool fits your project.

## Design choices at a glance

| | **Snake++** | **Snakemake** | **Nextflow** | **Pydra** |
|---|---|---|---|---|
| **Language** | Python classes inside a Snakefile | Snakefile (Python + rule syntax) | Groovy-based DSL | Plain Python |
| **What the DAG tracks** | One log file per job | Files, matched by filename pattern | Channels of values and files | Python objects (lazy outputs) |
| **Output files known in advance?** | No. The job reports what it created, in its log | Yes, as patterns (`directory()` as escape hatch) | Partly: glob patterns such as `path '*.bam'` | Yes, as typed output fields (shell tasks use templates) |
| **How steps are connected** | Explicitly: `set_input(x=rule.get_output('y'))` | Implicitly, by matching filenames | Explicitly, by passing channels | Explicitly, by passing lazy outputs |
| **Type checking** | Pydantic models, checked when the Snakefile is parsed | None | Optional static types (typed processes, 26.04+) | Type annotations, checked when the workflow is built |
| **Looping over subjects** | `rule.foreach(...)` | Wildcards + `expand()` | Channels; every item flows through | `split()` / `combine()` |
| **DAG that depends on results** | Checkpoints, wrapped by `foreach` | Checkpoints | Native (dataflow) | Native (lazy evaluation) |
| **When a job fails** | Never stops the run. Error goes to `.error`; downstream jobs fail with the root cause, or continue via `allow_failed_inputs` | Stops the run; `--keep-going` continues independent jobs | Configurable per process: `terminate` (default), `finish`, `ignore`, `retry` | Raises an error; completed tasks stay cached |
| **Deciding what to rerun** | Snakemake's own logic applied to the log files: changed params, newer upstream logs; failed jobs are retried | File timestamps, plus changed params, code or inputs | Hash of each task's inputs and script (`-resume`) | Hash of each task's inputs (cache directory) |
| **Monitoring a run** | Built in: one file per job in the log folder, named by state: `.running`, `.log`, `.error`, `.stale`. A file browser or `ls logs/*.error` is the dashboard | Terminal output and one Snakemake log per run; per-job logs if you declare them | Terminal output, `nextflow log`, execution reports, and Seqera Platform (web) | Terminal output; results and errors in the cache directories |
| **Rerunning one step by hand** | Delete its log file | Delete its output files, or `--forcerun` | Change it and use `-resume`, or clear its work dir | Delete its cache directory |
| **Where results go** | One folder per rule and wildcard set, derived automatically | Wherever your output patterns say | Isolated work dirs; you publish selected outputs | Hash-named cache dirs |
| **Cluster / cloud** | Everything Snakemake supports | Executor plugins (SLURM, cloud, …) | Many executors, strong on cloud | Workers (SLURM, SGE, Dask, …) |
| **Software environments** | Conda (via Snakemake) | Conda, containers, env modules | Containers, conda | Containers in shell tasks |
| **Maturity** | In-house | Large community, mature | Large community (nf-core), commercial backing | Nipype successor; 1.0 in pre-release |

## The same pipeline in each tool

The "Hello pipeline" from the [Hello Nextflow training](https://training.nextflow.io/latest/info/hello_pipeline/),
written independently for each tool: read greetings from a CSV file, write each greeting to its own
file, convert each file to upper case, and collect the results in one file plus a report with the
number of greetings. Each version is a runnable example in `examples/comparison/`, and all four
produce the same result.

| | Lines (no comments or blank lines) | How steps are connected | Where results go |
|---|---|---|---|
| Snakemake | 31 | Filename patterns; needs a wildcard constraint so that `UPPER-Hello-output.txt` is not also read as a greeting | One flat `results/` folder, as you name it |
| Nextflow | 47 | Channels passed between processes | Hash-named work dirs; `publish:` copies the final files to `results/` |
| Pydra | 32 | Lazy outputs; `.split()` / `.combine()` for the loop | Hash-named cache dirs |
| Snake++ | 45 | `set_input(... get_output(...))`, `foreach()` for the loop | One folder per rule and greeting, plus one log file per job |

Snake++ is the longest because every rule declares its wildcards, inputs and outputs as typed
models. That is also what gives it build-time type checks, and self-describing rules for the
generated documentation. Rules that share wildcards can inherit them from a common base class
(`GreetingRule` here). The Snakemake-specific part is two lines at the end.

=== "Snake++"

    ```python
    # The Hello pipeline with Snake++ (snakeplusplus).
    # Run from this folder: snakemake -c1
    import csv
    from os import path as op
    from pathlib import Path

    import snakeplusplus
    from snakeplusplus import SnakeRule, TargetRule, Field, Fixed

    pathvars:
        logs = op.abspath('logs'),
        results = op.abspath('results')

    snakeplusplus.configure(workflow.pathvars, config.get('params', {}))


    def greetings(wildcards):
        with open(config.get('input', '../greetings.csv')) as fp:
            for row in csv.reader(fp):
                yield dict(greeting=row[0])


    class GreetingRule(SnakeRule):
        """Base class for rules that run once per greeting."""
        class WildcardModel(Fixed):
            greeting: str = Field('{}')


    class SayHello(GreetingRule):
        class OutputModel(Fixed):
            text: Path = Field('{greeting}-output.txt')

        def run(self, job, input, output, params, wildcards):
            job.shell(f"echo '{wildcards.greeting}' > {output.text}")


    class ConvertToUpper(GreetingRule):
        class InputModel(Fixed):
            text: Path

        class OutputModel(Fixed):
            upper: Path = Field('UPPER-{greeting}-output.txt')

        def run(self, job, input, output, params, wildcards):
            job.shell(f"tr '[a-z]' '[A-Z]' < {input.text} > {output.upper}")


    class CollectGreetings(SnakeRule):
        class InputModel(Fixed):
            files: list[Path]

        class OutputModel(Fixed):
            collected: Path = Field('COLLECTED-output.txt')
            # note: a field cannot be called `report`, which is a Snakemake keyword
            summary: Path = Field('report.txt')

        def run(self, job, input, output, params, wildcards):
            job.shell(f"cat {' '.join(input.files)} > {output.collected}")
            job.shell(f"echo 'There were {len(input.files)} greetings in this batch.' > {output.summary}")


    say_hello = SayHello()
    convert_to_upper = ConvertToUpper().set_input(text=say_hello.get_output('text'))
    collect_greetings = CollectGreetings().set_input(
        files=convert_to_upper.foreach(greetings).get_output('upper')
    )
    runall = TargetRule().set_input(_=collect_greetings)


    # turn the SnakeRule objects above into Snakemake rules
    include: snakeplusplus.SNAKEFILE
    snakeplusplus.build(locals())
    ```

=== "Snakemake"

    ```python
    # The Hello pipeline in plain Snakemake.
    # Run from this folder: snakemake -c1
    import csv

    GREETINGS = [row[0] for row in csv.reader(open(config.get("input", "../greetings.csv")))]

    # without this, "UPPER-Hello-output.txt" would also match "{greeting}-output.txt"
    wildcard_constraints:
        greeting="[^-/]+",


    rule all:
        input:
            "results/COLLECTED-output.txt",
            "results/report.txt",


    rule say_hello:
        output:
            "results/{greeting}-output.txt",
        shell:
            "echo '{wildcards.greeting}' > {output}"


    rule convert_to_upper:
        input:
            "results/{greeting}-output.txt",
        output:
            "results/UPPER-{greeting}-output.txt",
        shell:
            "tr '[a-z]' '[A-Z]' < {input} > {output}"


    rule collect_greetings:
        input:
            expand("results/UPPER-{greeting}-output.txt", greeting=GREETINGS),
        output:
            collected="results/COLLECTED-output.txt",
            report="results/report.txt",
        params:
            n=len(GREETINGS),
        shell:
            """
            cat {input} > {output.collected}
            echo 'There were {params.n} greetings in this batch.' > {output.report}
            """
    ```

=== "Nextflow"

    ```groovy
    // The Hello pipeline in Nextflow (after the "Hello Nextflow" training, training.nextflow.io).
    // Run from this folder: nextflow run main.nf

    params {
        input: Path = '../greetings.csv'
    }

    process sayHello {
        input:
        val greeting

        output:
        path "${greeting}-output.txt"

        script:
        """
        echo '${greeting}' > '${greeting}-output.txt'
        """
    }

    process convertToUpper {
        input:
        path input_file

        output:
        path "UPPER-${input_file}"

        script:
        """
        tr '[a-z]' '[A-Z]' < ${input_file} > UPPER-${input_file}
        """
    }

    process collectGreetings {
        input:
        path input_files

        output:
        path 'COLLECTED-output.txt', emit: collected
        path 'report.txt', emit: report

        script:
        """
        cat ${input_files} > COLLECTED-output.txt
        echo 'There were ${input_files.size()} greetings in this batch.' > report.txt
        """
    }

    workflow {
        main:
        greetings = channel.fromPath(params.input).splitCsv().map { row -> row[0] }
        sayHello(greetings)
        convertToUpper(sayHello.out)
        collectGreetings(convertToUpper.out.collect())

        publish:
        collected = collectGreetings.out.collected
        report = collectGreetings.out.report
    }

    output {
        collected {
            path '.'
        }
        report {
            path '.'
        }
    }
    ```

=== "Pydra"

    ```python
    """The Hello pipeline in Pydra 1.0.

    Run from this folder: python hello.py
    """
    import csv
    from pathlib import Path

    from fileformats.generic import File
    from pydra.compose import python, workflow


    @python.define
    def SayHello(greeting: str) -> File:
        out = Path(f"{greeting}-output.txt").absolute()
        out.write_text(greeting + "\n")
        return out


    @python.define
    def ConvertToUpper(input_file: File) -> File:
        out = Path(f"UPPER-{Path(input_file).name}").absolute()
        out.write_text(Path(input_file).read_text().upper())
        return out


    @python.define(outputs=["collected", "report"])
    def CollectGreetings(input_files: list[File]) -> tuple[File, File]:
        collected, report = Path("COLLECTED-output.txt").absolute(), Path("report.txt").absolute()
        collected.write_text("".join(Path(f).read_text() for f in input_files))
        report.write_text(f"There were {len(input_files)} greetings in this batch.\n")
        return collected, report


    @workflow.define(outputs=["collected", "report"])
    def Hello(greetings: list[str]):
        say_hello = workflow.add(SayHello().split(greeting=greetings), name="say_hello")
        upper = workflow.add(ConvertToUpper(input_file=say_hello.out).combine("say_hello.greeting"))
        collect = workflow.add(CollectGreetings(input_files=upper.out))
        return collect.collected, collect.report


    if __name__ == "__main__":
        with open("../greetings.csv") as fp:
            greetings = [row[0] for row in csv.reader(fp)]
        outputs = Hello(greetings=greetings)(cache_root=Path("cache").absolute())
        print(outputs.collected, outputs.report, sep="\n")
    ```


## When to choose what

**Plain Snakemake** is the better choice when every step produces files whose names you can write
down in advance, and when you want its fine-grained rerun logic: change a parameter and exactly
the affected jobs run again. It is also what most collaborators and reviewers will already know.

**Nextflow** fits pipelines that must run on many platforms (cloud, HPC) and be shared widely, for
example through nf-core. Its dataflow model handles dynamic results without checkpoints, and
`-resume` is reliable. The price is a Groovy-based DSL and a work-directory model that takes some
getting used to.

**Pydra** fits projects that want to stay in plain Python and build workflows programmatically,
especially in neuroimaging, where it inherits from Nipype. Its split/combine semantics are
expressive. It has a smaller ecosystem than the other two, and version 1.0 is still in pre-release.

**Snake++** fits projects where:

- tools produce outputs whose names you only know after running them;
- you process many subjects and a few failures should not stop the rest, while every failure
  must still be traceable to its root cause;
- you want steps to be reusable, documented classes with typed inputs and outputs, and you want
  wiring mistakes caught before anything runs;
- you want to see the state of a run at a glance: the log folder has one file per job, and its
  extension (`.running`, `.log`, `.error`, `.stale`) tells you where each job is. Deleting a log
  file reruns exactly that job, plus what depends on it;
- you already run Snakemake on your cluster and do not want to switch schedulers.

## Trade-offs to be aware of

- **Rerun decisions are per job, not per file.** Snakemake reruns a job when its parameters
  change (including defaults), when an upstream job produced a newer log, or when it failed in a
  previous run (switch off with `--config retry_failed=False`). Unlike plain Snakemake, it cannot
  see that an individual result file was deleted or modified by hand.
- **Code changes do not trigger reruns, by design.** After fixing a bug that made some jobs fail,
  only those jobs run again, not every job that uses the fixed code. If a bug produced wrong
  results *without* raising an error, force the affected rule with `snakemake --forcerun <rule>`.
- **Output existence is not guaranteed.** An output listed in the log may be missing (for
  example, if it was deleted afterwards). Downstream rules should check files they depend on.
- **Two layers to learn.** Error messages about rule definitions may come from Snakemake, not
  from Snake++, and debugging sometimes requires knowing both.
- **Small user base.** No external community or long-term maintenance guarantee.
