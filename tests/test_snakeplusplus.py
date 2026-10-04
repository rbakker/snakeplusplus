"""End-to-end tests: run tests/test_pipeline.smk with Snakemake in a temporary directory.

    pytest tests/
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).parent
SNAKEFILE = HERE / 'test_pipeline.smk'


def jobs_run(p):
    """Names of the rules that were executed, from Snakemake's output."""
    import re
    return sorted(re.findall(r'^(?:local)?(?:rule|checkpoint) (\w+):', p.stdout + p.stderr, re.M))


def run_snakemake(workdir, params=None, *extra):
    cmd = [sys.executable, '-m', 'snakemake', '-s', str(SNAKEFILE), '-c4', '--directory', str(workdir)]
    if params:
        cmd += ['--config', f'params={json.dumps(params)}']
    cmd += list(extra)
    return subprocess.run(cmd, capture_output=True, text=True)


def test_all_succeed(tmp_path):
    p = run_snakemake(tmp_path)
    assert p.returncode == 0, p.stderr
    assert not list((tmp_path / 'logs').glob('*.error'))
    summary = json.loads((tmp_path / 'results/tolerant_summary/summary.json').read_text())
    # parser without wildcards (read_int) and with wildcards (tag_subject)
    assert summary['ok'][0] == {'double': 2 * ord('a'), 'tagged': f"a={ord('a')}"}
    assert len(summary['ok']) == 3 and summary['failed'] == []


def test_failure_propagates_but_snakemake_continues(tmp_path):
    p = run_snakemake(tmp_path, {'Measure': {'fail_on': ['b']}})
    assert p.returncode == 0, p.stderr
    errors = {f.name for f in (tmp_path / 'logs').glob('*.error')}
    assert errors == {'sub-b_measure.error', 'sub-b_double.error', 'strict_summary.error', 'runall.error'}
    # the root cause is visible downstream
    assert 'measurement failed for subject b' in (tmp_path / 'logs/runall.error').read_text()
    # list[Path | JobError]: runs anyway and knows what failed, and why
    summary = json.loads((tmp_path / 'results/tolerant_summary/summary.json').read_text())
    assert len(summary['ok']) == 2
    [failed] = summary['failed']
    assert 'measurement failed for subject b' in failed and 'sub-b_double.error' in failed
    assert 'passed on as JobError' in (tmp_path / 'logs/tolerant_summary.log').read_text()


def test_job_error_type():
    from pydantic import TypeAdapter
    from snakeplusplus import JobError
    from snakeplusplus import _admits_job_error
    e = JobError('boom')
    assert isinstance(e, Exception) and str(e) == 'boom' and repr(e) == "JobError('boom')"
    # never converted by pydantic, in whatever position of a union
    for ann in (Path | JobError, JobError | Path, str | JobError, list[Path | JobError]):
        value = [e] if typing_list(ann) else e
        out = TypeAdapter(ann).validate_python(value)
        assert type(out[0] if typing_list(ann) else out) is JobError
    assert _admits_job_error(list[Path | JobError]) and _admits_job_error(JobError | None)
    assert not _admits_job_error(list[Path]) and not _admits_job_error(str)


def typing_list(ann):
    import typing
    return typing.get_origin(ann) is list


def test_input_type_mismatch_fails_job(tmp_path):
    snakefile = tmp_path / 'Snakefile'
    snakefile.write_text(f"""
import snakeplusplus
from snakeplusplus import SnakeRule, target, Fixed, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

from pathlib import Path

class Make(SnakeRule):
    class OutputModel(Fixed):
        out: Path = Field('out.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.out, 'w').write('not a number')

def read(path: Path) -> int:  # the annotation promises an int, but it returns a str
    return open(path).read()

class Count(SnakeRule):
    class InputModel(Fixed):
        n: int
    def run(self, job, input, output, params, wildcards):
        pass

make = Make()
count = Count().set_input(n=make.get_output('out', parser=read))
runall = target(count)

snakeplusplus.build(locals())
""")
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(snakefile), '-c1', '--directory', str(tmp_path)],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    error = (tmp_path / 'logs/count.error').read_text()
    assert 'these inputs of Count have a wrong type:' in error and "n: " in error
    assert 'File "' not in error  # a JobError is logged without traceback


def test_failed_checkpoint(tmp_path):
    p = run_snakemake(tmp_path, {'MakeSubjectList': {'fail': True}})
    assert p.returncode == 0, p.stderr
    errors = {f.name for f in (tmp_path / 'logs').glob('*.error')}
    assert {'make_subject_list.error', 'tolerant_summary.error', 'strict_summary.error'} <= errors
    assert 'checkpoint failed on purpose' in (tmp_path / 'logs/strict_summary.error').read_text()


def test_params_in_log(tmp_path):
    p = run_snakemake(tmp_path, {'Measure': {'fail_on': ['x']}})
    assert p.returncode == 0, p.stderr
    lines = (tmp_path / 'logs/sub-a_measure.log').read_text().splitlines()
    assert lines[2:4] == ['and using parameters', '{"fail_on": ["x"]}']
    lines = (tmp_path / 'logs/runall.log').read_text().splitlines()
    assert lines[2:4] == ['', '']  # no parameters


def test_nothing_to_do_after_success(tmp_path):
    run_snakemake(tmp_path)
    p = run_snakemake(tmp_path)
    assert jobs_run(p) == [], jobs_run(p)


def test_retry_failed_jobs(tmp_path):
    (tmp_path / 'fail_b').touch()  # failure that does not depend on parameters
    run_snakemake(tmp_path)
    assert (tmp_path / 'logs/sub-b_measure.error').exists()

    # opt-out: nothing is retried
    p = run_snakemake(tmp_path, None, '--config', 'retry_failed=False')
    assert jobs_run(p) == []

    # default: only the failed job and what depends on it are retried
    (tmp_path / 'fail_b').unlink()
    p = run_snakemake(tmp_path)
    assert p.returncode == 0, p.stderr
    assert jobs_run(p) == ['double', 'measure', 'runall', 'strict_summary', 'tolerant_summary']
    assert not list((tmp_path / 'logs').glob('*.error'))

    # and then it converges
    assert jobs_run(run_snakemake(tmp_path)) == []


def test_one_file_per_job(tmp_path):
    (tmp_path / 'fail_b').touch()
    run_snakemake(tmp_path)
    files = sorted(f.name for f in (tmp_path / 'logs').iterdir())
    jobs = {f.rsplit('.', 1)[0] for f in files}
    assert len(files) == len(jobs), files
    assert 'sub-b_measure.error' in files and 'sub-a_measure.log' in files


def test_retry_when_only_tolerant_rule_depends_on_failure(tmp_path):
    # the failure does not propagate to the target, so nothing downstream forces a rerun
    (tmp_path / 'fail_c').touch()
    run_snakemake(tmp_path, None, 'tolerant_summary')
    assert (tmp_path / 'logs/sub-c_double.error').exists()
    assert (tmp_path / 'logs/tolerant_summary.log').exists()
    (tmp_path / 'fail_c').unlink()
    p = run_snakemake(tmp_path, None, 'tolerant_summary')
    assert jobs_run(p) == ['double', 'measure', 'tolerant_summary']


def test_delete_log_reruns_that_job(tmp_path):
    run_snakemake(tmp_path)
    (tmp_path / 'logs/sub-c_measure.log').unlink()
    p = run_snakemake(tmp_path)
    assert jobs_run(p) == ['double', 'measure', 'runall', 'strict_summary', 'tolerant_summary']


def test_delete_log_then_dry_run_then_run(tmp_path):
    # a dry run must not change what the real run does afterwards
    run_snakemake(tmp_path)
    (tmp_path / 'logs/sub-c_measure.log').unlink()
    expected = ['double', 'measure', 'runall', 'strict_summary', 'tolerant_summary']
    assert jobs_run(run_snakemake(tmp_path, None, '-n')) == expected
    assert jobs_run(run_snakemake(tmp_path, None, '-n')) == expected
    assert jobs_run(run_snakemake(tmp_path)) == expected
    assert jobs_run(run_snakemake(tmp_path)) == []


def test_foreign_files_in_log_folder_are_ignored(tmp_path):
    # e.g. another Snakefile using the same log folder
    run_snakemake(tmp_path)
    logs = tmp_path / 'logs'
    (logs / 'other_pipeline_job.running').write_text('busy')
    (logs / 'other_pipeline_job2.error').write_text('failed')
    assert jobs_run(run_snakemake(tmp_path)) == []
    assert (logs / 'other_pipeline_job.running').exists()


def test_deleting_snakemake_folder_reruns_nothing(tmp_path):
    import shutil
    run_snakemake(tmp_path)
    shutil.rmtree(tmp_path / '.snakemake')
    assert jobs_run(run_snakemake(tmp_path)) == []


def test_interrupted_job_becomes_stale_and_reruns(tmp_path):
    run_snakemake(tmp_path)
    logs = tmp_path / 'logs'
    (logs / 'sub-a_double.log').rename(logs / 'sub-a_double.running')  # as if the job was killed
    run_snakemake(tmp_path, None, '-n')
    assert (logs / 'sub-a_double.stale').exists() and not (logs / 'sub-a_double.running').exists()
    p = run_snakemake(tmp_path)
    assert 'double' in jobs_run(p) and 'measure' not in jobs_run(p)
    assert (logs / 'sub-a_double.log').exists() and not (logs / 'sub-a_double.stale').exists()


def test_queued_during_run(tmp_path):
    (tmp_path / 'snapshot_logs').touch()
    run_snakemake(tmp_path, None, '-c1')
    # while the first measure job ran (after the checkpoint), every job still to come was queued,
    # including the ones that only became known through the checkpoint
    snapshots = [json.loads(f.read_text()) for f in tmp_path.glob('snapshot_*.json')]
    first = max(snapshots, key=lambda snap: sum(f.endswith('.queued') for f in snap))
    running = [f for f in first if f.endswith('.running')]
    assert len(running) == 1 and running[0].endswith('_measure.running')
    assert sorted(first) == sorted(
        ['make_subject_list.log', running[0], 'runall.queued', 'strict_summary.queued', 'tolerant_summary.queued']
        + [f'sub-{s}_{r}.queued' for s in 'abc' for r in ('measure', 'double')
           if f'sub-{s}_{r}.running' != running[0]])
    # after the run: one file per job, nothing queued
    files = os.listdir(tmp_path / 'logs')
    assert not [f for f in files if f.endswith('.queued')] and len(files) == 10

    # rerun one job: its previous log becomes .queued, jobs that do not rerun keep their .log
    (tmp_path / 'logs' / 'sub-b_measure.log').unlink()
    for f in tmp_path.glob('snapshot_*.json'):
        f.unlink()
    run_snakemake(tmp_path, None, '-c1')
    snapshot = json.loads((tmp_path / 'snapshot_b.json').read_text())
    assert {'sub-b_double.queued', 'runall.queued', 'sub-a_double.log', 'sub-a_measure.log'} <= set(snapshot)


def test_dry_run_creates_no_queued_files(tmp_path):
    run_snakemake(tmp_path)
    (tmp_path / 'logs' / 'sub-b_measure.log').unlink()
    run_snakemake(tmp_path, None, '-n')
    assert not [f for f in os.listdir(tmp_path / 'logs') if f.endswith('.queued')]


def test_leftover_queued_files_are_cleaned_up(tmp_path):
    run_snakemake(tmp_path)
    logs = tmp_path / 'logs'
    (logs / 'sub-a_double.log').rename(logs / 'sub-a_double.queued')   # rerun was pending, then stopped
    (logs / 'sub-b_double.log').unlink()
    (logs / 'sub-b_double.queued').touch()                             # never ran, then stopped
    run_snakemake(tmp_path, None, '-n')
    assert (logs / 'sub-a_double.stale').exists() and not (logs / 'sub-b_double.queued').exists()


def test_cancel_inside_job_gives_stale(tmp_path):
    from snakeplusplus.jobmonitor import JobMonitor
    log = tmp_path / 'logs' / 'x.log'
    with pytest.raises(KeyboardInterrupt):
        with JobMonitor(str(log), 'x', params={'b': 2, 'a': 1}):
            raise KeyboardInterrupt
    assert [f.name for f in log.parent.iterdir()] == ['x.stale']
    lines = (log.parent / 'x.stale').read_text().splitlines()
    assert lines[2:4] == ['and using parameters', '{"a": 1, "b": 2}']


def test_rerun_on_param_change(tmp_path):
    run_snakemake(tmp_path)
    p = run_snakemake(tmp_path, {'Measure': {'fail_on': ['x']}})
    # all three subjects of measure, plus everything downstream
    assert jobs_run(p).count('measure') == 3
    assert 'make_subject_list' not in jobs_run(p)


def test_dry_run(tmp_path):
    p = run_snakemake(tmp_path, None, '-n')
    assert p.returncode == 0, p.stderr
    assert not (tmp_path / 'logs').exists()


def test_documentation(tmp_path):
    out = tmp_path / 'pipeline.html'
    p = subprocess.run(['snakeplusplus-doc', str(SNAKEFILE), '-o', str(out)],
                       capture_output=True, text=True, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    page = out.read_text()
    assert ':::checkpointStyle' in page and 'defines items' in page
    assert not (tmp_path / 'logs').exists()  # nothing was executed


def test_parser_wildcards_detection():
    from snakeplusplus import _parser_wants_wildcards

    def one(x: str) -> str: ...
    def two(x: str, wildcards) -> str: ...
    def with_default(x: str, sep: str = ',') -> str: ...
    def named_default(x: str, wildcards=None) -> str: ...
    assert not _parser_wants_wildcards(one)
    assert _parser_wants_wildcards(two)
    assert not _parser_wants_wildcards(with_default)
    assert _parser_wants_wildcards(named_default)


def test_comparison_example(tmp_path):
    example = HERE.parent / 'examples' / 'comparison'
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(example / 'snakeplusplus' / 'Snakefile'),
                        '-c1', '--directory', str(tmp_path), '--config', f"input={example / 'greetings.csv'}"],
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    result = tmp_path / 'results' / 'collect_greetings'
    assert (result / 'COLLECTED-output.txt').read_text() == 'HELLO\nBONJOUR\nHOLA\n'
    assert (result / 'report.txt').read_text() == 'There were 3 greetings in this batch.\n'


def test_readme_example(tmp_path):
    import re
    readme = (HERE.parent / 'README.md').read_text()
    (tmp_path / 'Snakefile').write_text(re.search(r"```python\n(.*?)```", readme, re.S).group(1))
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1', '--directory', str(tmp_path)],
                       capture_output=True, text=True, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    assert (tmp_path / 'results' / 'Bonjour' / 'say_hello' / 'Bonjour-output.txt').read_text() == 'Bonjour\n'
    assert sorted(f.name for f in (tmp_path / 'logs').iterdir()) == [
        'Bonjour_say_hello.log', 'Hello_say_hello.log', 'Hola_say_hello.log', 'runall.log']


def test_jobresult_as_input_type():
    import pydantic
    from snakeplusplus import SnakeRule, Fixed, Field, JobResult

    class Upstream(SnakeRule):
        result_template = 'sub-{subject}'

    class Downstream(SnakeRule):
        class InputModel(Fixed):
            one: JobResult
            many: list[JobResult]

    class Mismatch(SnakeRule):
        class InputModel(Fixed):
            one: JobResult

    up = Upstream()
    Downstream().set_input(one=up, many=up.foreach(subject=['a', 'b']))   # rule / loop -> JobResult(s)
    with pytest.raises(TypeError):
        Mismatch().set_input(one=up.foreach(subject=['a', 'b']))          # list where one is expected

    class PlainModel(pydantic.BaseModel):                                  # also outside Snake++ models
        result: JobResult
    with pytest.raises(pydantic.ValidationError):
        PlainModel(result='not a JobResult')


def test_jobresult_input_end_to_end(tmp_path):
    (tmp_path / 'Snakefile').write_text('''
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed, JobResult

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class Write(SnakeRule):
    result_template = '{name}'
    class OutputModel(Fixed):
        text: Path = Field('{name}.txt')
    def run(self, job, input, output, params, wildcards):
        Path(output.text).write_text(wildcards.name)

class Concat(SnakeRule):
    class InputModel(Fixed):
        results: list[JobResult]
    class OutputModel(Fixed):
        joined: Path = Field('joined.txt')
    def run(self, job, input, output, params, wildcards):
        Path(output.joined).write_text(''.join(Path(r.text).read_text() for r in input.results))

write = Write()
concat = Concat().set_input(results=write.foreach(name=['x', 'y']))
runall = target(concat)

snakeplusplus.build(locals())
''')
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1'], capture_output=True, text=True, cwd=tmp_path)
    assert p.returncode == 0, p.stderr
    assert not list((tmp_path / 'logs').glob('*.error')), [f.read_text() for f in (tmp_path / 'logs').glob('*.error')]
    assert (tmp_path / 'results' / 'concat' / 'joined.txt').read_text() == 'xy'


BROKEN_LOOP_SNAKEFILE = '''
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

def subjects(wildcards):
    raise FileNotFoundError('subjects.tsv not found')
    yield

class Step(SnakeRule):
    result_template = '{subject}'
    def run(self, job, input, output, params, wildcards):
        pass

step = Step()
runall = target(step.foreach(subjects))

include: snakeplusplus.SNAKEFILE
snakeplusplus.build(locals())
'''


@pytest.mark.parametrize('dry_run', [True, False])
def test_loop_error_before_execution_stops_snakemake(tmp_path, dry_run):
    (tmp_path / 'Snakefile').write_text(BROKEN_LOOP_SNAKEFILE)
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1'] + (['-n'] if dry_run else []),
                       capture_output=True, text=True, cwd=tmp_path)
    assert p.returncode != 0
    assert 'subjects.tsv not found' in p.stdout + p.stderr
    assert not (tmp_path / 'logs').exists() or not os.listdir(tmp_path / 'logs')  # nothing ran


def test_invalid_param_value_is_reported_before_execution(tmp_path):
    p = run_snakemake(tmp_path, {'Measure': {'fail_on': 5}}, '-n')
    assert p.returncode != 0
    out = p.stdout + p.stderr
    assert 'invalid parameters' in out and 'fail_on' in out


UNDEFINED_NAMES_SNAKEFILE = '''
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class Step(SnakeRule):
    def run(self, job, input, output, params, wildcards):
        import json                                      # local import: fine
        values = [len(str(x)) for x in range(3)]         # comprehension and builtins: fine
        helper_defined_later(values)                     # defined further down: fine
        print(np.mean(values), json.dumps(values))       # np was never imported
        self.helper()

    def helper(self):
        return undefined_function()                      # never defined

def helper_defined_later(values):
    return values

step = Step()
runall = target(step)

snakeplusplus.build(locals())
'''


def test_undefined_names_reported_before_execution(tmp_path):
    (tmp_path / 'Snakefile').write_text(UNDEFINED_NAMES_SNAKEFILE)
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1', '-n'], capture_output=True, text=True, cwd=tmp_path)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "Step.run" in out and "name 'np' is not defined" in out and 'line 17' in out
    assert "Step.helper" in out and "name 'undefined_function' is not defined" in out
    for fine in ('json', 'helper_defined_later', 'range', "'x'"):
        assert f"name {fine!r} is not defined" not in out if not fine.startswith("'") else f"name {fine} is not" not in out


def test_stamp_paths_one_level_per_wildcard(tmp_path):
    run_snakemake(tmp_path)
    stamps = tmp_path / '.snakemake' / 'snakeplusplus' / 'stamps'
    found = {str(f.relative_to(stamps)) for f in stamps.rglob('*') if f.is_file()}
    assert {'measure/a/job', 'double/c/job', 'make_subject_list/job', 'runall/job'} <= found
    assert len(found) == len(list((tmp_path / 'logs').iterdir()))  # one stamp per job


def test_rule_names_ending_alike_are_not_confused(tmp_path):
    snakefile = """
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

class Step(SnakeRule):
    result_template = 'sub-{subject}'
    class OutputModel:
        out: Path = Field('out.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.out, 'w').write(self.name + ' ' + wildcards.subject)

clean = Step()
tck_clean = Step()
everything = target(tck_clean.foreach(subject=['1']), clean.foreach(subject=['2_tck']))
"""
    # the log names differ (sub-1_tck_clean, sub-2_tck_clean), but with the old stamps Snakemake
    # found sub-1_tck_clean ambiguous: rule tck_clean with subject 1, or rule clean with subject 1_tck
    p = run_targets(tmp_path, snakefile)
    assert p.returncode == 0, p.stdout + p.stderr
    assert (tmp_path / 'results/sub-1/tck_clean/out.txt').read_text() == 'tck_clean 1'
    assert (tmp_path / 'results/sub-2_tck/clean/out.txt').read_text() == 'clean 2_tck'
    # really the same log name: reported before anything runs
    p = run_targets(tmp_path, snakefile.replace("'2_tck'", "'1_tck'"), '-n')
    assert p.returncode != 0
    assert "Two jobs would use the same log file" in p.stdout + p.stderr


def test_params_as_keyword_arguments():
    from snakeplusplus import SnakeRule, Fixed, Field

    class Rule_n4bfc(SnakeRule):
        class ParamModel(Fixed):
            modality: str = Field('anat')
            iterations: int = 50

    assert Rule_n4bfc(modality='T1w').params == {'modality': 'T1w'}
    assert Rule_n4bfc(params=dict(modality='T1w')).params == {'modality': 'T1w'}   # still accepted
    assert Rule_n4bfc().params == {}
    assert Rule_n4bfc(iterations=lambda wildcards: 10).params['iterations']        # functions: checked per job
    with pytest.raises(TypeError, match='not both'):
        Rule_n4bfc(params=dict(modality='T1w'), iterations=10)
    with pytest.raises(TypeError, match=r"unknown parameter\(s\) \['modaliti'\]"):
        Rule_n4bfc(modaliti='T1w')
    with pytest.raises(ValueError, match="invalid value for parameter 'iterations'"):
        Rule_n4bfc(iterations='many')


def test_param_error_points_at_constructor_line(tmp_path):
    (tmp_path / 'Snakefile').write_text('''
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class Step(SnakeRule):
    class ParamModel(Fixed):
        modality: str = 'anat'
    def run(self, job, input, output, params, wildcards):
        pass

step = Step(modaliti='T1w')
runall = target(step)

snakeplusplus.build(locals())
''')
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1', '-n'], capture_output=True, text=True, cwd=tmp_path)
    out = p.stdout + p.stderr
    assert p.returncode != 0
    assert "unknown parameter(s) ['modaliti']" in out and 'line 17' in out


OUTPUTS_SNAKEFILE = '''
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, Fixed

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars)

class Produce(SnakeRule):
    result_template = '{case}'
    class OutputModel(Fixed):
        main: Path = Field('main.txt')
        extra: Path = Field('extra.txt')           # written by "a tool", never referenced in run()
        mask: Path | None = Field('mask.txt')      # optional
        n_lines: int                               # plain value, must be declared
        label: str = 'default label'               # plain value with default: optional
    def run(self, job, input, output, params, wildcards):
        if wildcards.case == 'with_mask':
            output(label='custom')
        if wildcards.case != 'nothing':
            output(n_lines=3)
        if wildcards.case != 'nothing':
            Path(output.main).write_text('main')
            (Path(output()) / 'extra.txt').write_text('extra')
        if wildcards.case == 'with_mask':
            Path(output.mask).write_text('mask')
        else:
            output.mask                            # referenced, but not created

class Consume(SnakeRule):
    result_template = '{case}'
    class InputModel(Fixed):
        mask: Path | None
        n_lines: int
        label: str
    class OutputModel(Fixed):
        seen: Path = Field('seen.txt')
    def run(self, job, input, output, params, wildcards):
        Path(output.seen).write_text(f'{input.mask} {input.n_lines} {input.label}')

produce = Produce()
consume = Consume().set_input(mask=produce.get_output('mask'), n_lines=produce.get_output('n_lines'),
                              label=produce.get_output('label'))
runall = target(consume.foreach(case=['with_mask', 'without_mask', 'nothing']))

snakeplusplus.build(locals())
'''


def test_promised_outputs(tmp_path):
    (tmp_path / 'Snakefile').write_text(OUTPUTS_SNAKEFILE)
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-c1'], capture_output=True, text=True, cwd=tmp_path)
    assert p.returncode == 0, p.stdout + p.stderr
    from snakeplusplus.jobmonitor import JobResult
    logs = tmp_path / 'logs'

    # all present: the unreferenced but existing output is added to the log
    res = JobResult(str(logs / 'with_mask_produce.log'))
    assert sorted(res._named_outputs) == ['extra', 'label', 'main', 'mask', 'n_lines']
    # plain values arrive downstream as values; an undeclared value with a default gets the default
    assert (tmp_path / 'results/with_mask/consume/seen.txt').read_text().endswith(' 3 custom')

    # optional output missing: no error, left out of the log, None downstream
    res = JobResult(str(logs / 'without_mask_produce.log'))
    assert sorted(res._named_outputs) == ['extra', 'main', 'n_lines']
    assert (tmp_path / 'results/without_mask/consume/seen.txt').read_text() == 'None 3 default label'

    # required outputs missing: the job fails and says which
    error = (logs / 'nothing_produce.error').read_text()
    assert 'these required outputs of Produce are missing' in error and 'main:' in error and 'extra:' in error
    assert 'n_lines: not declared' in error
    listed = error.split('are missing')[1].split('(a file output')[0]
    assert 'mask' not in listed and 'label' not in listed


def test_declared_output_with_wrong_type(tmp_path):
    (tmp_path / 'Snakefile').write_text(OUTPUTS_SNAKEFILE.replace("output(n_lines=3)", "output(n_lines='three')"))
    subprocess.run([sys.executable, '-m', 'snakemake', '-c1'], capture_output=True, text=True, cwd=tmp_path)
    error = (tmp_path / 'logs' / 'with_mask_produce.error').read_text()
    assert 'these outputs of Produce have a wrong type' in error and "n_lines" in error and "'three'" in error


def test_optional_output_needs_optional_input(tmp_path):
    from snakeplusplus import SnakeRule, Fixed, Field

    class Produce(SnakeRule):
        class OutputModel(Fixed):
            mask: Path | None = Field('mask.txt')

    class Strict(SnakeRule):
        class InputModel(Fixed):
            mask: Path

    with pytest.raises(TypeError, match='Type mismatch'):
        Strict().set_input(mask=Produce().get_output('mask'))


def test_format_command():
    from snakeplusplus.jobmonitor import format_command
    cmd = ['tckgen', 'in.mif', '-force', '-seed_image', 'seed.nii.gz', 'out.tck', '-angle', '-45',
           '-include', 'a b.nii.gz']
    assert format_command(cmd).splitlines() == [
        'tckgen', '    in.mif', '    -force', '    -seed_image seed.nii.gz', '    out.tck',
        '    -angle -45', "    -include 'a b.nii.gz'"]


def test_failed_command_log(tmp_path):
    from snakeplusplus.jobmonitor import JobMonitor, JobResult
    log = tmp_path / 'logs' / 'x.log'
    script = tmp_path / 'tool.sh'
    script.write_text('echo "tool: [ERROR] image is empty"\nexit 3\n')
    with JobMonitor(str(log), 'x') as job:
        job.run(['sh', str(script)])
    text = (tmp_path / 'logs' / 'x.error').read_text()
    assert text.count('image is empty') == 1                     # the output appears once
    assert 'sh exited with code 3' in text
    assert 'jobmonitor.py' not in text and '__init__.py' not in text   # no internal frames
    assert 'test_snakeplusplus.py' in text                       # but the caller's line is there
    assert JobResult(str(tmp_path / 'logs' / 'x.error'))._errors[0].startswith('sh exited with code 3')


def test_escape_fences():
    from snakeplusplus.jobmonitor import escape_fences
    assert escape_fences('a\n```\n  ````  \nx ```\n```python') == 'a\n    ```\n      ````  \nx ```\n```python'


def test_log_is_valid_markdown_structure(tmp_path):
    # every code block that is opened is closed, also with fence-like lines in the tool output,
    # and the output mapping is still read back correctly
    from snakeplusplus.jobmonitor import JobMonitor, JobResult
    script = tmp_path / 'tool.sh'
    script.write_text("echo 'line 1'\necho '```'\necho 'tool: [ERROR] failed'\nexit 2\n")
    log = tmp_path / 'logs' / 'x.log'
    with JobMonitor(str(log), 'x') as job:
        job.result(n=1)
        job.run(['sh', str(script)])
    text = (tmp_path / 'logs' / 'x.error').read_text()
    fences = [l for l in text.splitlines() if l.startswith('```')]
    assert len(fences) % 2 == 0, fences                       # balanced
    assert '\n    ```\n' in text                              # the tool's fence line was escaped
    assert '(log-file: x.error)' in text
    assert '```json' in text
    res = JobResult(str(tmp_path / 'logs' / 'x.error'))
    assert res._named_outputs == {'n': 1} and res._errors[0].startswith('sh exited with code 2')


CLEANUP_SNAKEFILE = """
import os, json
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Fixed, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars, config.get('params', {}))

class Clean(SnakeRule):
    class InputModel(Fixed):
        data: Path
    class OutputModel(Fixed):
        main: Path = Field('main.txt')
        opt: Path | None = Field('opt.txt')
        leftover: Path | None = Field('leftover.txt')  # never mentioned by run()
        figs: Path = Field('figs')
        data: Path = Field('data.txt')                 # an input that is edited in place
    class ParamModel(Fixed):
        make_opt: bool = True
        version: int = 1
    def run(self, job, input, output, params, wildcards):
        open(output.main, 'w').write(str(params.version))
        if params.make_opt:
            open(output.opt, 'w').write('x')
        os.makedirs(output.figs, exist_ok=True)
        open(os.path.join(output.figs, 'f.png'), 'w').write('x')
        with open(output.data, 'a') as fp:
            fp.write('+')

clean = Clean().set_input(data='results/clean/data.txt')
runall = target(clean)

snakeplusplus.build(locals())
"""


def test_rerun_removes_previous_outputs_only(tmp_path):
    import os, time
    (tmp_path / 'Snakefile').write_text(CLEANUP_SNAKEFILE)
    res = tmp_path / 'results' / 'clean'
    res.mkdir(parents=True)
    (res / 'data.txt').write_text('d')
    old = time.time() - 3600
    (res / 'leftover.txt').write_text('old')
    os.utime(res / 'leftover.txt', (old, old))

    def run(params):
        return subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(tmp_path / 'Snakefile'), '-c1',
                               '--directory', str(tmp_path), '--config', f'params={json.dumps(params)}'],
                              capture_output=True, text=True)

    assert run({}).returncode == 0
    log = (tmp_path / 'logs/clean.log').read_text()
    assert '"leftover"' not in log  # an old file that run() did not mention is not an output
    assert (res / 'opt.txt').exists()

    (res / 'precious.txt').write_text('mine')        # not an output: never touched
    os.utime(res / 'main.txt', (old, old))           # listed, but older than the previous run
    p = run({'Clean': {'make_opt': False}})
    assert p.returncode == 0, p.stderr
    log = (tmp_path / 'logs/clean.log').read_text()
    assert 'Removed outputs of the previous run: opt.txt, figs/' in log
    assert 'f.png' not in log  # files inside a removed folder are not listed
    assert 'Kept outputs of the previous run that it may not have created: main.txt, data.txt' in log
    assert not (res / 'opt.txt').exists() and (res / 'figs/f.png').exists()
    assert (res / 'precious.txt').read_text() == 'mine' and (res / 'leftover.txt').read_text() == 'old'
    assert (res / 'data.txt').read_text() == 'd++'


TARGETS_SNAKEFILE = """
import os
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Fixed, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

class Prep(SnakeRule):
    result_template = 'sub-{subject}'
    class OutputModel(Fixed):
        out: Path = Field('prep.txt')
    def run(self, job, input, output, params, wildcards):
        scratch = job.tmpdir() / 'scratch.txt'
        scratch.write_text('tmp')
        open(output.out, 'w').write(str(scratch))

class Report(SnakeRule):
    class OutputModel(Fixed):
        out: Path = Field('report.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.out, 'w').write('report')

prep = Prep()
prep.tmpdir_autodelete = False
summary = Report()

preprocessing = target(prep.foreach(subject=['a', 'b']))
everything = target(prep.foreach(subject=['a', 'b']), summary)
"""


def run_targets(tmp_path, snakefile, *targets):
    (tmp_path / 'Snakefile').write_text(snakefile + "\nsnakeplusplus.build(locals())\n")
    return subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(tmp_path / 'Snakefile'), '-c1',
                           '--directory', str(tmp_path), *targets], capture_output=True, text=True)


def test_targets(tmp_path):
    # the first target is the default
    p = run_targets(tmp_path, TARGETS_SNAKEFILE)
    assert p.returncode == 0, p.stderr
    assert jobs_run(p) == ['prep', 'prep', 'preprocessing']
    # tmpdir_autodelete = False on the instance: the folder is kept and its location logged
    log = (tmp_path / 'logs/sub-a_prep.log').read_text()
    assert 'Temporary folder kept: ' in log
    assert Path((tmp_path / 'results/sub-a/prep/prep.txt').read_text()).exists()
    # a named target
    p = run_targets(tmp_path, TARGETS_SNAKEFILE, 'everything')
    assert p.returncode == 0, p.stderr
    assert jobs_run(p) == ['everything', 'summary']


def test_target_needs_foreach_for_wildcards(tmp_path):
    p = run_targets(tmp_path, TARGETS_SNAKEFILE.replace("preprocessing = target(prep.foreach(subject=['a', 'b']))",
                                                        "preprocessing = target(prep)"))
    assert p.returncode != 0
    assert 'target(): rule Prep has wildcards {subject}; use foreach(...)' in p.stderr


def test_rule_names_that_would_break_snakemake(tmp_path):
    p = run_targets(tmp_path, TARGETS_SNAKEFILE.replace('everything =', 'all =').replace('summary', 'touch'), '-n')
    assert p.returncode != 0
    out = p.stdout + p.stderr
    assert "'all' would hide Python's built-in all()" in out
    assert "'touch' would replace Snakemake's own 'touch'" in out


def test_tmpdir_is_removed_by_default(tmp_path):
    p = run_targets(tmp_path, TARGETS_SNAKEFILE.replace('prep.tmpdir_autodelete = False\n', ''))
    assert p.returncode == 0, p.stderr
    assert 'Temporary folder' not in (tmp_path / 'logs/sub-a_prep.log').read_text()
    assert not Path((tmp_path / 'results/sub-a/prep/prep.txt').read_text()).exists()


PREFIX_SNAKEFILE = """
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Fixed, Field, PathPrefix
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars, config.get('params', {}))

class Track(SnakeRule):
    result_template = '{tract}/{hemi}'
    class OutputModel(Fixed):
        prefix: PathPrefix = Field('{tract}({hemi})_')
    class ParamModel(Fixed):
        files: list[str] = ['tracts.tck', 'stats.txt']
    def run(self, job, input, output, params, wildcards):
        for name in params.files:
            open(f'{output.prefix}{name}', 'w').write(name)

class Count(SnakeRule):
    class InputModel(Fixed):
        prefix: PathPrefix
    class OutputModel(Fixed):
        n: Path = Field('n.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.n, 'w').write(str(len(input.prefix.matches())))

track = Track()
count = Count().set_input(prefix=track.foreach(tract='AF', hemi='L').get_output('prefix'))
everything = target(count)
"""


def test_path_prefix(tmp_path):
    def run(params=None):
        return run_targets(tmp_path, PREFIX_SNAKEFILE, '--config', f'params={json.dumps(params or {})}')
    p = run()
    assert p.returncode == 0, p.stderr
    res = tmp_path / 'results/AF/L/track'
    assert sorted(f.name for f in res.iterdir()) == ['AF(L)_stats.txt', 'AF(L)_tracts.tck']
    assert (tmp_path / 'results/count/n.txt').read_text() == '2'
    assert '"prefix": "AF(L)_"' in (tmp_path / 'logs/AF_L_track.log').read_text()

    # rerun with fewer files: the files of the previous run are removed first
    p = run({'Track': {'files': ['tracts.tck']}})
    assert p.returncode == 0, p.stderr
    assert sorted(f.name for f in res.iterdir()) == ['AF(L)_tracts.tck']
    assert 'Removed outputs of the previous run: AF(L)_stats.txt, AF(L)_tracts.tck' in (tmp_path / 'logs/AF_L_track.log').read_text()
    assert (tmp_path / 'results/count/n.txt').read_text() == '1'

    # no files at all: the prefix output is missing
    p = run({'Track': {'files': []}})
    assert p.returncode == 0, p.stderr
    assert 'prefix: results/AF/L/track/AF(L)_* (no files with this prefix were created)' in (tmp_path / 'logs/AF_L_track.error').read_text()


DYNAMIC_SNAKEFILE = """
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

class Track(SnakeRule):
    class OutputModel:                       # no base class: becomes a Fixed model
        DRT_L: Path = Field('left.txt')
        DRT_R: Path = Field('right.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.DRT_L, 'w').write('L')
        open(output.DRT_R, 'w').write('R')

class Filter(SnakeRule):
    result_template = '{hemi}'
    class InputModel:
        tck: Path
    class OutputModel:
        out: Path = Field('out.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.out, 'w').write(open(input.tck).read())

track = Track()
filter_ = Filter().set_input(
    tck=lambda wildcards: track.get_output(f'DRT_{wildcards.hemi}'),  # a connection chosen per job
)
everything = target(filter_.foreach(hemi=['L', 'R']))
"""


def test_dynamic_connection_and_plain_model_classes(tmp_path):
    p = run_targets(tmp_path, DYNAMIC_SNAKEFILE)
    assert p.returncode == 0, p.stderr
    assert (tmp_path / 'results/L/filter_/out.txt').read_text() == 'L'
    assert (tmp_path / 'results/R/filter_/out.txt').read_text() == 'R'
    # the connection is part of the job graph: deleting the upstream log reruns the downstream jobs
    (tmp_path / 'logs/track.log').unlink()
    p = run_targets(tmp_path, DYNAMIC_SNAKEFILE)
    assert jobs_run(p) == ['everything', 'filter_', 'filter_', 'track']


def test_output_key_pattern(tmp_path):
    snakefile = DYNAMIC_SNAKEFILE.replace(
        "tck=lambda wildcards: track.get_output(f'DRT_{wildcards.hemi}'),  # a connection chosen per job",
        "tck=track.get_output('DRT_{hemi}'),")
    p = run_targets(tmp_path, snakefile)
    assert p.returncode == 0, p.stderr
    assert (tmp_path / 'results/L/filter_/out.txt').read_text() == 'L'
    assert (tmp_path / 'results/R/filter_/out.txt').read_text() == 'R'
    # checked when the Snakefile is loaded
    p = run_targets(tmp_path, snakefile.replace("'DRT_{hemi}'", "'DRT_{side}'"), '-n')
    assert "Filter.tck: get_output('DRT_{side}') uses {side}, which is not a wildcard of Filter" in p.stdout + p.stderr
    p = run_targets(tmp_path, snakefile.replace("'DRT_{hemi}'", "'FOD_{hemi}'"), '-n')
    assert "Track has no output that matches 'FOD_{hemi}'" in p.stdout + p.stderr
    # a wildcard value without matching output: reported in the job's log
    p = run_targets(tmp_path, snakefile.replace("hemi=['L', 'R']", "hemi=['L', 'X']"))
    assert p.returncode == 0, p.stderr
    assert "Track has no output 'DRT_X'" in (tmp_path / 'logs/X_filter_.error').read_text()


def test_model_with_other_base_class_is_rejected(tmp_path):
    p = run_targets(tmp_path, DYNAMIC_SNAKEFILE.replace('class InputModel:', 'class InputModel(dict):'), '-n')
    assert p.returncode != 0
    assert 'Filter.InputModel must be a class without base class, or derive from Fixed or Extensible' in p.stdout + p.stderr


def test_output_missing_from_log_uses_default_name(tmp_path):
    # e.g. a log written by an older version, which only listed the outputs that run() referred to
    snakefile = DYNAMIC_SNAKEFILE.replace(
        "tck=lambda wildcards: track.get_output(f'DRT_{wildcards.hemi}'),  # a connection chosen per job",
        "tck=track.get_output('DRT_{hemi}'),")
    assert run_targets(tmp_path, snakefile).returncode == 0
    log = tmp_path / 'logs/track.log'
    log.write_text(log.read_text().replace('"DRT_L": "left.txt",', ''))
    assert '"DRT_L"' not in log.read_text()
    for f in (tmp_path / 'logs').glob('*filter_*'):
        f.unlink()
    p = run_targets(tmp_path, snakefile)
    assert p.returncode == 0, p.stderr
    assert jobs_run(p) == ['everything', 'filter_', 'filter_']  # track itself is not rerun
    assert (tmp_path / 'results/L/filter_/out.txt').read_text() == 'L'

    # and if the file is not there either: a clear message
    (tmp_path / 'results/track/left.txt').unlink()
    (tmp_path / 'logs/L_filter_.log').unlink()
    p = run_targets(tmp_path, snakefile)
    assert p.returncode == 0, p.stderr
    error = (tmp_path / 'logs/L_filter_.error').read_text()
    assert "Track did not produce its output 'DRT_L' (expected " in error and 'results/track/left.txt; log-file: ' in error


TEMPLATE_SNAKEFILE = """
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

class Track(SnakeRule):
    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'
    class OutputModel:
        tracts: Path = Field('tracts.tck')
    def run(self, job, input, output, params, wildcards):
        open(output.tracts, 'w').write('x')

track = Track()
everything = target(track.foreach(subject=['01', '01'], tract=['AF', 'AF'], hemi=['L', 'R']))
"""


def test_result_template_with_prefix(tmp_path):
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE)
    assert p.returncode == 0, p.stdout + p.stderr
    assert sorted(f.name for f in (tmp_path / 'results/sub-01/tract-AF').iterdir()) == \
        ['L_track_tracts.tck', 'R_track_tracts.tck']
    assert (tmp_path / 'logs/sub-01_tract-AF_L_track.log').exists()


def test_log_template(tmp_path):
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace(
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'",
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'\n    log_template = 'sub-{subject}/{rule}_{tract}{hemi}'"))
    assert p.returncode != 0  # two wildcards next to each other
    assert "two wildcards without anything between them" in p.stdout + p.stderr
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace(
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'",
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'\n    log_template = 'sub-{subject}/{rule}_{tract}'"), '-n')
    assert "must have the same wildcards as result_template" in p.stdout + p.stderr
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace(
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'",
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'\n    log_template = 'sub-{subject}/{rule}_{tract}_{hemi}'"))
    assert p.returncode == 0, p.stdout + p.stderr
    assert sorted(f.name for f in (tmp_path / 'logs/sub-01').iterdir()) == ['track_AF_L.log', 'track_AF_R.log']


def test_wildcard_model_is_no_longer_supported(tmp_path):
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace(
        "    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'",
        "    class WildcardModel(Fixed):\n        subject: str = Field('sub-{}')\n        hemi: str = Field('{}')").replace(
        "import SnakeRule, target, Field", "import SnakeRule, target, Field, Fixed"), '-n')
    assert p.returncode != 0
    out = p.stdout + p.stderr
    assert "Track: WildcardModel is no longer supported. Use instead:" in out
    assert "result_template = 'sub-{subject}/{hemi}'" in out


def test_wildcard_value_with_slash(tmp_path):
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace("hemi=['L', 'R']", "hemi=['L', 'R/x']"), '-n')
    assert p.returncode != 0
    assert "wildcard hemi='R/x' is not allowed" in p.stdout + p.stderr


def test_two_jobs_with_the_same_log_name(tmp_path):
    # subject 'a_ses-1' with session '2', and subject 'a' with session '1_ses-2'
    snakefile = TEMPLATE_SNAKEFILE.replace("'sub-{subject}/tract-{tract}/{hemi}_*'", "'sub-{subject}_ses-{tract}/{hemi}_*'") \
        .replace("subject=['01', '01'], tract=['AF', 'AF'], hemi=['L', 'R']",
                 "subject=['a_ses-1', 'a'], tract=['2', '1_ses-2'], hemi=['L', 'L']")
    p = run_targets(tmp_path, snakefile, '-n')
    assert p.returncode != 0
    assert "Two jobs would use the same log file" in p.stdout + p.stderr


def test_bids_style_template_gives_short_log_names(tmp_path):
    p = run_targets(tmp_path, TEMPLATE_SNAKEFILE.replace(
        "'sub-{subject}/tract-{tract}/{hemi}_*'", "'sub-{subject}/tract-{tract}/sub-{subject}_tract-{tract}_{hemi}_*'"))
    assert p.returncode == 0, p.stdout + p.stderr
    assert sorted(f.name for f in (tmp_path / 'results/sub-01/tract-AF').iterdir()) == \
        ['sub-01_tract-AF_L_track_tracts.tck', 'sub-01_tract-AF_R_track_tracts.tck']
    assert sorted(f.name for f in (tmp_path / 'logs').iterdir()) == \
        ['everything.log', 'sub-01_tract-AF_L_track.log', 'sub-01_tract-AF_R_track.log']


def test_log_name_template():
    from snakeplusplus import _log_name_template
    assert _log_name_template('sub-{subject}/') == 'sub-{subject}_{rule}'
    assert _log_name_template('sub-{subject}/{hemi}_*') == 'sub-{subject}_{hemi}_{rule}'
    assert _log_name_template('sub-{s}/ses-{t}/sub-{s}_ses-{t}_*') == 'sub-{s}_ses-{t}_{rule}'
    assert _log_name_template('derivatives/sub-{s}/') == 'sub-{s}_{rule}'      # a fixed folder adds nothing
    assert _log_name_template('{rule}/sub-{s}/') == '{rule}_sub-{s}'          # the rule name always stays
    assert _log_name_template('') == '{rule}'


def test_hint_for_wildcard_model():
    from snakeplusplus import _template_from_wildcard_model, Fixed, Field

    class Plain:
        subject: str = Field('sub-{}')

    class Pydantic(Fixed):
        subject: str = Field('sub-{}')
        session: str = Field('ses-{}')

    assert _template_from_wildcard_model(Plain) == 'sub-{subject}'
    assert _template_from_wildcard_model(Pydantic) == 'sub-{subject}/ses-{session}'


def test_path_prefix_with_empty_name_is_the_jobs_prefix(tmp_path):
    snakefile = PREFIX_SNAKEFILE.replace("    result_template = '{tract}/{hemi}'", "    result_template = '{tract}/{hemi}_*'") \
                                .replace("prefix: PathPrefix = Field('{tract}({hemi})_')", "prefix: PathPrefix = Field('')")
    def run(params=None):
        return run_targets(tmp_path, snakefile, '--config', f'params={json.dumps(params or {})}')
    p = run()
    assert p.returncode == 0, p.stdout + p.stderr
    res = tmp_path / 'results/AF'
    assert sorted(f.name for f in res.iterdir()) == ['L_track_stats.txt', 'L_track_tracts.tck']
    log = (tmp_path / 'logs/AF_L_track.log').read_text()
    assert '"prefix": ""' in log and "Output 'prefix': L_track_stats.txt, L_track_tracts.tck" in log
    assert (tmp_path / 'results/count/n.txt').read_text() == '2'
    # rerun with fewer files: the files of the previous run are removed
    p = run({'Track': {'files': ['tracts.tck']}})
    assert p.returncode == 0, p.stdout + p.stderr
    assert sorted(f.name for f in res.iterdir()) == ['L_track_tracts.tck']


THREADS_SNAKEFILE = """
from pathlib import Path
import snakeplusplus
from snakeplusplus import SnakeRule, target, Field
pathvars:
    logs = 'logs',
    results = 'results'
snakeplusplus.configure(workflow.pathvars)

class Tool(SnakeRule):
    threads_budget = 'cores/2'
    class OutputModel:
        n: Path = Field('threads.txt')
    def run(self, job, input, output, params, wildcards):
        open(output.n, 'w').write(str(job.threads))

tool = Tool()
"""


def test_threads_budget_and_job_threads(tmp_path):
    def threads(snakefile, cores):
        p = run_targets(tmp_path, snakefile, f'-c{cores}', '--forceall')
        assert p.returncode == 0, p.stdout + p.stderr
        return (tmp_path / 'results/tool/threads.txt').read_text()
    assert threads(THREADS_SNAKEFILE, 4) == '2'                                    # cores/2
    assert threads(THREADS_SNAKEFILE.replace("'cores/2'", "'cores-2'"), 3) == '1'  # at least 1
    assert threads(THREADS_SNAKEFILE.replace("'cores/2'", "8"), 3) == '3'          # at most -c
    assert threads(THREADS_SNAKEFILE.replace("    threads_budget = 'cores/2'\n", ""), 4) == '1'  # default


def test_old_threads_attribute_and_bad_expressions(tmp_path):
    p = run_targets(tmp_path, THREADS_SNAKEFILE.replace("threads_budget = 'cores/2'", "threads = 8"), '-n')
    assert p.returncode != 0
    out = p.stdout + p.stderr
    assert "Tool: 'threads' is now called 'threads_budget'" in out and "threads_budget = 8" in out
    p = run_targets(tmp_path, THREADS_SNAKEFILE.replace("'cores/2'", "'half'"), '-n')
    assert "Tool.threads_budget = 'half' is not a valid expression" in p.stdout + p.stderr


TUTORIAL = HERE.parent / 'examples' / 'tutorial'


def run_tutorial(tmp_path, step, *extra):
    return subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(TUTORIAL / f'{step}.smk'), '-c1',
                           '--directory', str(tmp_path), *extra], capture_output=True, text=True)


def test_tutorial_step1(tmp_path):
    p = run_tutorial(tmp_path, 'step1')
    assert p.returncode == 0, p.stderr
    assert sorted(str(f.relative_to(tmp_path)) for f in tmp_path.glob('[lr]*/**/*') if f.is_file()) == [
        'logs/Bonjour_say_hello.log', 'logs/Hello_say_hello.log', 'logs/Hola_say_hello.log', 'logs/everything.log',
        'results/Bonjour/say_hello/hello.txt', 'results/Hello/say_hello/hello.txt', 'results/Hola/say_hello/hello.txt']
    assert jobs_run(run_tutorial(tmp_path, 'step1')) == []
    (tmp_path / 'logs/Hola_say_hello.log').unlink()
    assert jobs_run(run_tutorial(tmp_path, 'step1')) == ['everything', 'say_hello']


def test_tutorial_steps_2_to_4(tmp_path):
    p = run_tutorial(tmp_path, 'step2')
    assert p.returncode == 0, p.stderr
    log = (tmp_path / 'logs/Hello_convert_to_upper.log').read_text()
    assert "tr '[:lower:]' '[:upper:]'" in log and 'Output:' not in log  # no empty output block
    assert (tmp_path / 'results/Hola/convert_to_upper/upper.txt').read_text() == 'HOLA\n'
    # step 3 after step 2: only the new rule and the target run
    assert jobs_run(run_tutorial(tmp_path, 'step3')) == ['collect_greetings', 'everything']
    result = tmp_path / 'results/collect_greetings'
    assert (result / 'all_greetings.txt').read_text() == 'HELLO\nBONJOUR\nHOLA\n'
    assert '"count": 3' in (tmp_path / 'logs/collect_greetings.log').read_text()


def test_tutorial_failure_and_fix(tmp_path):
    p = run_tutorial(tmp_path, 'step4')
    assert p.returncode == 0, p.stderr
    logs = sorted(f.name for f in (tmp_path / 'logs').iterdir())
    assert logs == ['Bonjour_convert_to_upper.log', 'Bonjour_say_hello.log', 'Hello_convert_to_upper.log',
                    'Hello_say_hello.log', 'Hola_convert_to_upper.error', 'Hola_say_hello.log',
                    'collect_greetings.error', 'everything.error']
    error = (tmp_path / 'logs/collect_greetings.error').read_text()
    assert '"collect_greetings" did not run because 1 job(s) it depends on failed:' in error
    assert '(log-file: Hola_convert_to_upper.error)' in error
    # the fix: only what failed runs again
    assert jobs_run(run_tutorial(tmp_path, 'step3')) == ['collect_greetings', 'convert_to_upper', 'everything']


def test_tutorial_named_target_and_typo(tmp_path):
    p = run_tutorial(tmp_path, 'step3', 'uppercase')
    assert p.returncode == 0, p.stderr
    assert jobs_run(p) == ['convert_to_upper'] * 3 + ['say_hello'] * 3 + ['uppercase']
    typo = tmp_path / 'typo.smk'
    typo.write_text((TUTORIAL / 'step2.smk').read_text().replace("get_output('text')", "get_output('txt')"))
    (tmp_path / 'greetings.csv').write_text((TUTORIAL / 'greetings.csv').read_text())
    p = subprocess.run([sys.executable, '-m', 'snakemake', '-s', str(typo), '-c1', '--directory', str(tmp_path)],
                       capture_output=True, text=True)
    assert p.returncode != 0
    assert 'line 48:' in p.stdout + p.stderr and "SayHello has no output 'txt'" in p.stdout + p.stderr
