# A study with subjects and sessions, used for the code fragments on the page "Loops and checkpoints".
# It runs without external tools; a test runs it and checks the results.
import csv
import json
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, SnakeCheckpoint, target, Field, JobLoop, JobResult

snakeplusplus.configure(dict(logs='logs', results='results'), config.get('params'))


# --8<-- [start:measure]
class Measure(SnakeRule):
    """One measurement per subject and session."""
    result_template = 'sub-{subject}/ses-{session}'

    class InputModel:
        data: Path

    class OutputModel:
        value: float

    def run(self, job, input, output, params, wildcards):
        output(value=float(input.data.read_text()))
# --8<-- [end:measure]


# --8<-- [start:summaries]
class SubjectMean(SnakeRule):
    """The mean over the sessions of one subject."""
    result_template = 'sub-{subject}'

    class InputModel:
        values: JobLoop[float]

    class OutputModel:
        mean: float

    def run(self, job, input, output, params, wildcards):
        output(mean=sum(input.values) / len(input.values))


class GroupTable(SnakeRule):
    """All measurements in one table, and the sessions per subject."""
    class InputModel:
        values: JobLoop[float]

    class OutputModel:
        table: Path = Field('table.json')

    def run(self, job, input, output, params, wildcards):
        rows = [dict(subject=wc.subject, session=wc.session, value=v) for wc, v in input.values.items()]
        per_subject = {s: len(loop) for s, loop in input.values.groupby('subject').items()}
        first = input.values.get(subject='01', session='a')
        json.dump(dict(rows=rows, sessions=per_subject, first=first), open(output.table, 'w'), indent=2)


class Count(SnakeRule):
    """A plain list, for a rule that does not need the wildcards."""
    class InputModel:
        values: list[float]

    class OutputModel:
        n: int

    def run(self, job, input, output, params, wildcards):
        output(n=len(input.values))
# --8<-- [end:summaries]


# --8<-- [start:checkpoint]
class FindSessions(SnakeCheckpoint):
    """Find the subjects and their sessions in participants.tsv, and the data file of each session."""
    class InputModel:
        participants: Path

    class OutputModel:
        subjects: list[str]
        sessions: list[dict[str, str]]
        data: JobLoop[Path] = Field('{subject}_{session}')   # keys: subject and session

    def run(self, job, input, output, params, wildcards):
        rows = list(csv.DictReader(open(input.participants), delimiter='\t'))
        sessions = [dict(subject=r['subject'], session=s) for r in rows for s in r['sessions'].split(',')]
        output(subjects=[r['subject'] for r in rows], sessions=sessions)
        for s in sessions:
            output.data.add(f"data/sub-{s['subject']}/ses-{s['session']}.txt", **s)
# --8<-- [end:checkpoint]


# --8<-- [start:loops]
SUBJECTS = ['01', '02', '03']
SESSIONS = {'01': ['a', 'b'], '02': ['a'], '03': ['a', 'b', 'c']}

def sessions_of(wildcards):
    return SESSIONS[wildcards.subject]

measure = Measure().set_input(data='data/sub-{subject}/ses-{session}.txt')

# all combinations: three subjects, each with session a
first_sessions = measure.foreach(subject=SUBJECTS).foreach(session=['a'])
# the sessions of each subject: the function gets the subject of the outer loop
all_sessions = measure.foreach(subject=SUBJECTS).foreach(session=sessions_of)
# specific combinations
some_sessions = measure.foreach([dict(subject='01', session='b'), dict(subject='03', session='c')])
# a single job, e.g. for a test run
one_job = measure.where(subject='01', session='a')
# --8<-- [end:loops]

# --8<-- [start:connections]
# per subject: {subject} comes from the receiving job, the loop adds the sessions
subject_mean = SubjectMean().set_input(values=measure.foreach(session=sessions_of).get_output('value'))
group_table = GroupTable().set_input(values=all_sessions.get_output('value'))
count = Count().set_input(values=some_sessions.get_output('value'))
# --8<-- [end:connections]

# --8<-- [start:checkpoint_loop]
find_sessions = FindSessions().set_input(participants='data/participants.tsv')
# the data files that the checkpoint found: {subject} and {session} pick the file for each job
measure_found = Measure().set_input(data=find_sessions.get_output('data'))
# a loop over the sessions that the checkpoint found
found_table = GroupTable().set_input(
    values=measure_found.foreach(find_sessions.get_output('sessions')).get_output('value'))
# a loop over one wildcard
found_means = SubjectMean().set_input(
    values=measure_found.foreach(session=sessions_of).get_output('value'))
found_subjects = found_means.foreach(subject=find_sessions.get_output('subjects'))
# --8<-- [end:checkpoint_loop]

# --8<-- [start:targets]
everything = target(group_table, subject_mean.foreach(subject=SUBJECTS), count, found_table, found_subjects)
test_run = target(one_job)
# --8<-- [end:targets]

snakeplusplus.build(locals())
