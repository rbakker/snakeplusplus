"""End-to-end tests: run tests/test_pipeline.smk with Snakemake in a temporary directory.

    pytest tests/
"""
import json
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
    assert len(summary['ok']) == 3 and summary['failed'] == {}


def test_failure_propagates_but_snakemake_continues(tmp_path):
    p = run_snakemake(tmp_path, {'Measure': {'fail_on': ['b']}})
    assert p.returncode == 0, p.stderr
    errors = {f.name for f in (tmp_path / 'logs').glob('*.error')}
    assert errors == {'sub-b_measure.error', 'sub-b_double.error', 'strict_summary.error', 'runall.error'}
    # the root cause is visible downstream
    assert 'measurement failed for subject b' in (tmp_path / 'logs/runall.error').read_text()
    # allow_failed_inputs: runs anyway and knows what failed
    summary = json.loads((tmp_path / 'results/tolerant_summary/summary.json').read_text())
    assert len(summary['ok']) == 2
    assert [Path(f).name for f in summary['failed']['results']] == ['sub-b_double.error']


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
