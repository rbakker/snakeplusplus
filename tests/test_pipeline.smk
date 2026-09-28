# Test pipeline for snakeplusplus: checkpoints, loops, parsers and failure propagation.
# Run from tests/test_snakeplusplus.py, which runs Snakemake in a temporary directory.
import os
from os import path as op
import json
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, SnakeCheckpoint, TargetRule, JobResult, Field, Fixed

project_root = os.getcwd()
pathvars:
    logs = op.join(project_root, 'logs'),
    results = op.join(project_root, 'results')

snakeplusplus.configure(workflow.pathvars, config.get('params', {}), retry_failed=config.get('retry_failed', True))


# --- rules --- #

class MakeSubjectList(SnakeCheckpoint):
    class OutputModel(Fixed):
        subjects: Path = Field('subjects.json', description='JSON list of subject ids')

    class ParamModel(Fixed):
        subjects: list[str] = Field(['a', 'b', 'c'])
        fail: bool = False

    def run(self, job, input, output, params, wildcards):
        if params.fail:
            raise RuntimeError('checkpoint failed on purpose')
        with open(output.subjects, 'w') as fp:
            json.dump(params.subjects, fp)


class Measure(SnakeRule):
    class WildcardModel(Fixed):
        subject: str = Field('sub-{}')

    class OutputModel(Fixed):
        value: Path = Field('value.txt')

    class ParamModel(Fixed):
        fail_on: list[str] = Field([])

    def run(self, job, input, output, params, wildcards):
        if wildcards.subject in params.fail_on or op.exists(f'fail_{wildcards.subject}'):
            raise ValueError(f'measurement failed for subject {wildcards.subject}')
        with open(output.value, 'w') as fp:
            fp.write(str(ord(wildcards.subject)))


def read_int(path: Path) -> int:
    # parser without wildcards
    return int(open(path).read())


def tag_subject(path: Path, wildcards) -> str:
    # parser with wildcards
    return f'{wildcards.subject}={open(path).read()}'


class Double(SnakeRule):
    class WildcardModel(Fixed):
        subject: str = Field('sub-{}')

    class InputModel(Fixed):
        value: int
        tagged: str

    class OutputModel(Fixed):
        result: Path = Field('double.json')

    def run(self, job, input, output, params, wildcards):
        with open(output.result, 'w') as fp:
            json.dump({'double': 2 * input.value, 'tagged': input.tagged}, fp)


def subjects_from_checkpoint(wildcards):
    res = JobResult.fromCheckpoint(checkpoints.make_subject_list, wildcards)
    for s in json.load(open(res.subjects)):
        yield dict(subject=s)


class TolerantSummary(SnakeRule):
    allow_failed_inputs = {'results'}

    class InputModel(Fixed):
        results: list[Path]

    class OutputModel(Fixed):
        summary: Path = Field('summary.json')

    def run(self, job, input, output, params, wildcards):
        ok = [json.load(open(r)) for r in input.results if r is not None]
        with open(output.summary, 'w') as fp:
            json.dump({'ok': ok, 'failed': job.failed_inputs}, fp, indent=2)


class StrictSummary(TolerantSummary):
    allow_failed_inputs = False


# --- graph --- #

make_subject_list = MakeSubjectList()
measure = Measure()
double = Double().set_input(
    value=measure.get_output('value', parser=read_int),
    tagged=measure.get_output('value', parser=tag_subject),
)
tolerant_summary = TolerantSummary().set_input(
    results=double.foreach(subjects_from_checkpoint).get_output('result'),
)
strict_summary = StrictSummary().set_input(
    results=double.foreach(subjects_from_checkpoint).get_output('result'),
)
runall = TargetRule().set_input(
    a=tolerant_summary,
    b=strict_summary,
)


# turn the SnakeRule objects above into Snakemake rules
include: snakeplusplus.SNAKEFILE
snakeplusplus.build(locals())
