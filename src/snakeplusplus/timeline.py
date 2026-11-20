"""A timeline of a snakeplusplus pipeline: when each job ran, how long its commands took, and how
much memory they used, read from the log files.

    snakeplusplus-timeline                       # the default target, finds the Snakefile
    snakeplusplus-timeline -t everything         # another target
    snakeplusplus-timeline --watch 60            # again every minute, while the pipeline runs

The page shows the jobs that a target depends on, whichever Snakemake runs made them: the same
target gives the same totals, whether it was made in one run or in several. Nothing is run.
"""
import argparse
import html
import os
import re
import statistics
import sys
from datetime import datetime, timedelta
from os import path as op

from .jobmonitor import JOB_STATES, replace_inner_extension, format_memory, parse_memory

_TIME = r'(?:(\d+) days?, )?(\d+):(\d\d):(\d\d)'
_STAMP = re.compile(r'^\[' + _TIME + r'\] (.*)$')
_STARTED = re.compile(r'^"(.*)" started at (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d), saving output to$')
_FINISHED = re.compile(r'^Command finished in ' + _TIME + r': exit code (-?\d+), peak memory (.*)$')
_END = re.compile(r'^"(.*)" (completed in|failed after|was cancelled after) ' + _TIME +
                  r' \(h:mm:ss[^)]*\)(?:, peak memory (.*))?\.$')
_JOB_NAME = re.compile(r'^(\w+)\((\w+)\)(?:<(.*)>)?$')


def _seconds(days, h, m, s):
    return int(days or 0) * 86400 + int(h) * 3600 + int(m) * 60 + int(s)


def _memory(text):
    """Bytes from '2.3 GB', None from 'unknown' or ''."""
    try:
        return parse_memory(text.strip())
    except (ValueError, KeyError, AttributeError):
        return None


def read_log(path):
    """What a job's log file says about its run: when it started, its state, its duration, its
    commands with their start, duration, exit code and peak memory, and its error, if any.
    Returns None for a file that is not a job's log."""
    with open(path, errors='replace') as fp:
        lines = fp.read().split('\n')
    m = _STARTED.match(lines[0]) if lines else None
    if not m:
        return None
    job = dict(path=path, name=m.group(1), started=datetime.strptime(m.group(2), '%Y-%m-%d %H:%M:%S'),
               state=op.splitext(path)[1].lstrip('.'), duration=None, last=0, commands=[],
               peak_commands=None, peak_python=None, error=None)
    n = _JOB_NAME.match(job['name'])
    job['rule'], job['cls'], job['wildcards'] = (n.group(1), n.group(2), n.group(3) or '') if n else (job['name'], '', '')
    command = None
    i = 0
    while i < len(lines):
        s = _STAMP.match(lines[i])
        if s:
            t = _seconds(*s.groups()[:4])
            text = s.group(5)
            job['last'] = t
            if text.startswith(('Running command', 'Running shell script')):
                # the command follows in a code block
                body = []
                if i + 2 < len(lines) and lines[i + 2].startswith('```'):
                    j = i + 3
                    while j < len(lines) and not lines[j].startswith('```'):
                        body.append(lines[j].strip())
                        j += 1
                command = dict(start=t, text=' '.join(body), kind='shell' if 'shell' in text else 'command',
                               duration=None, exit_code=None, peak=None)
                job['commands'].append(command)
            f = _FINISHED.match(text)
            if f and command is not None:
                command['duration'] = _seconds(*f.groups()[:4])
                command['exit_code'] = int(f.group(5))
                command['peak'] = _memory(f.group(6))
                command = None
            e = _END.match(text)
            if e:
                job['duration'] = _seconds(*e.groups()[2:6])
                for part in (e.group(7) or '').split(', '):
                    if part.endswith('(commands)'):
                        job['peak_commands'] = _memory(part[:-len('(commands)')])
                    elif part.endswith('(Python)'):
                        job['peak_python'] = _memory(part[:-len('(Python)')])
            if text.startswith('Errors occurred') and job['error'] is None:
                j = i + 1
                while j < len(lines) and not lines[j].strip() or (j < len(lines) and lines[j].startswith('```')):
                    j += 1
                job['error'] = lines[j].strip() if j < len(lines) else 'error'
        i += 1
    if job['duration'] is None:
        job['duration'] = job['last']  # interrupted, or still running: up to its last message
        job['incomplete'] = True
    if job['peak_commands'] is None:
        peaks = [c['peak'] for c in job['commands'] if c['peak']]
        job['peak_commands'] = max(peaks) if peaks else None
    job['ended'] = job['started'] + timedelta(seconds=job['duration'])
    return job


def _state_file(log_path):
    for state in JOB_STATES:
        f = replace_inner_extension(log_path, state)
        if op.isfile(f):
            return f
    return None


def target_jobs(builder, target):
    """The jobs that `target` (a rule without wildcards, usually a target) depends on, directly or
    through other jobs, with their log files: {log path: (rule, wildcards)}, and the connections
    between them: {log path: [log paths of the jobs it takes input from]}."""
    jobs, upstream = {}, {}
    todo = [(target, {})]
    syncing = builder.syncing
    builder.syncing = True  # lets loops over a finished checkpoint read its result
    try:
        while todo:
            rule, wc = todo.pop()
            log = rule.log_path(wc)
            if log in jobs:
                continue
            jobs[log] = (rule, wc)
            ups = []
            for up_rule, up_wc in builder._upstream_jobs(rule, wc):
                ups.append(up_rule.log_path(up_wc))
                todo.append((up_rule, up_wc))
            upstream[log] = list(dict.fromkeys(ups))
    finally:
        builder.syncing = syncing
    return jobs, upstream


def collect(builder, target, exclude_target=False, now=None):
    """The runs of the jobs of `target`, read from their log files, and the totals.

    A running job lasts until `now`. A queued job's file still holds its previous run, which is
    not shown: the job is listed as queued. For the jobs that are running or queued, the totals
    have an estimate of the time left, from the median duration of the finished jobs per rule."""
    now = now or datetime.now().replace(microsecond=0)
    jobs, upstream = target_jobs(builder, target)
    target_log = target.log_path({})
    runs, not_run = {}, []
    for log, (rule, wc) in jobs.items():
        if exclude_target and log == target_log:
            continue
        f = _state_file(log)
        job = read_log(f) if f else None
        if job is None and f and f.endswith('.queued'):
            # a job that has not run before: an empty file, made when the run started
            job = dict(path=f, name=log, started=now, state='queued', duration=0, last=0, commands=[],
                       peak_commands=None, peak_python=None, error=None, wildcards='', ended=now)
        if job is None:
            not_run.append(log)
        else:
            job['log'] = log
            job['rule'], job['cls'] = rule.name, type(rule).__name__
            if not job['wildcards'] and wc:
                job['wildcards'] = ','.join(f'{k}={v}' for k, v in wc.items())
            if job['state'] == 'running':
                job['duration'] = max(job['duration'], int((now - job['started']).total_seconds()))
                job['ended'] = job['started'] + timedelta(seconds=job['duration'])
            runs[log] = job

    # the critical path: the longest chain of jobs that depend on each other, with the
    # durations of the jobs that succeeded
    longest = {}

    def chain(log):
        if log not in longest:
            longest[log] = (0, [])  # guards against cycles, which a DAG does not have
            best = max((chain(u) for u in upstream.get(log, []) if u in jobs), default=(0, []), key=lambda x: x[0])
            job = runs.get(log)
            own = job['duration'] if job and job['state'] == 'log' else 0
            longest[log] = (best[0] + own, best[1] + [log])
        return longest[log]
    critical_time, critical = chain(target_log)
    critical = [log for log in critical if log in runs]

    succeeded = [j for j in runs.values() if j['state'] == 'log']
    count = lambda state: sum(j['state'] == state for j in runs.values())
    totals = dict(
        jobs=len(jobs) - (1 if exclude_target else 0), succeeded=len(succeeded), failed=count('error'),
        running=count('running'), queued=count('queued'), stale=count('stale'),
        not_run=len(not_run), compute=sum(j['duration'] for j in succeeded), critical=critical_time,
        peak=max((j['peak_commands'] or 0 for j in runs.values()), default=0) or None,
        now=now, target_state=(runs.get(target_log) or {}).get('state'),
    )
    totals.update(_estimate(runs, upstream, jobs))
    return runs, totals, critical, not_run


def _estimate(runs, upstream, jobs):
    """The time left for the jobs that are running or queued: each takes the median duration of
    the finished jobs of its rule (a running job, what is left of it); together they take their
    sum divided over the jobs that run at the same time, but at least their longest chain."""
    medians = {}
    for j in runs.values():
        if j['state'] == 'log':
            medians.setdefault(j['rule'], []).append(j['duration'])
    medians = {rule: statistics.median(d) for rule, d in medians.items()}
    left, unknown = {}, 0
    for log, j in runs.items():
        if j['state'] in ('running', 'queued') and j.get('cls') != 'TargetRule':  # a target takes no time
            if j['rule'] in medians:
                elapsed = j['duration'] if j['state'] == 'running' else 0
                left[log] = max(medians[j['rule']] - elapsed, 0)
            else:
                unknown += 1
    if not left:
        return dict(left=None, left_unknown=unknown)  # no estimate yet
    chains = {}

    def chain(log):
        if log not in chains:
            chains[log] = 0
            chains[log] = left.get(log, 0) + max((chain(u) for u in upstream.get(log, []) if u in jobs), default=0)
        return chains[log]
    longest = max((chain(log) for log in jobs), default=0)
    parallel = max(sum(j['state'] == 'running' for j in runs.values()), 1)
    return dict(left=max(sum(left.values()) / parallel, longest), left_unknown=unknown)


def _fmt_duration(seconds):
    if seconds is None:
        return '?'
    seconds = int(round(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    if h:
        return f'{h}h {m:02d}m'
    if m:
        return f'{m}m {s:02d}s'
    return f'{s}s'


def _fmt_memory(n):
    return format_memory(n) if n else ''


def _esc(text):
    return html.escape(str(text), quote=True)


def _segments(runs, min_gap):
    """Busy periods: [(start, end)] of overlapping or nearby runs; gaps longer than `min_gap`
    seconds between them are drawn compressed."""
    intervals = sorted((j['started'], j['ended'] if j['ended'] > j['started'] else j['started'] + timedelta(seconds=1))
                       for j in runs)
    periods = []
    for start, end in intervals:
        if periods and (start - periods[-1][1]).total_seconds() <= min_gap:
            periods[-1][1] = max(periods[-1][1], end)
        else:
            periods.append([start, end])
    return periods


def render(runs, totals, critical, not_run, title, output_path, refresh=None):
    """The timeline as an HTML page; links to the log files are relative to `output_path`. With
    `refresh`, the page reloads itself every so many seconds."""
    queued = sorted((j for j in runs.values() if j['state'] == 'queued'), key=lambda j: j['name'])
    jobs = sorted((j for j in runs.values() if j['state'] != 'queued'), key=lambda j: (j['started'], j['name']))
    critical = set(critical)
    folder = op.dirname(op.abspath(output_path))

    # the time axis: busy periods, with long gaps between them compressed to GAP pixels
    active = sum((j['duration'] for j in jobs), 0)
    periods = _segments(jobs, min_gap=max(600, 0.05 * active)) if jobs else []
    busy = sum((e - s).total_seconds() for s, e in periods) or 1
    WIDTH, GAP, LABEL, ROW = 900, 40, 330, 18
    scale = (WIDTH - GAP * max(len(periods) - 1, 0)) / busy

    def x(t):
        offset = 0
        for i, (s, e) in enumerate(periods):
            if t <= e or i == len(periods) - 1:
                return LABEL + offset + max(0.0, min((t - s).total_seconds(), (e - s).total_seconds())) * scale
            offset += (e - s).total_seconds() * scale + GAP
        return LABEL

    colors = {'log': '#4c8bd6', 'error': '#d64c4c', 'stale': '#b0a040', 'running': '#60b060', 'queued': '#aaaaaa'}
    rows = []
    height = 40 + ROW * (len(jobs) + len(queued))
    # axis: a tick per busy period start, and per hour inside it
    axis = []
    for i, (s, e) in enumerate(periods):
        x0 = x(s)
        axis.append(f'<text x="{x0:.1f}" y="14" class="tick">{s:%d %b %H:%M}</text>')
        if i:
            gap = s - periods[i - 1][1]
            axis.append(f'<text x="{x0 - GAP / 2:.1f}" y="30" class="gap" text-anchor="middle">'
                        f'{_fmt_duration(gap.total_seconds())}</text>'
                        f'<line x1="{x0 - GAP / 2:.1f}" y1="34" x2="{x0 - GAP / 2:.1f}" y2="{height}" class="gapline"/>')
        span = (e - s).total_seconds()
        step = next((st for st in (60, 300, 900, 1800, 3600, 7200, 21600) if span / st <= 8), 43200)
        t = s.replace(second=0, microsecond=0) + timedelta(seconds=step - (s.minute * 60 + s.second) % step)
        while t < e:
            if x(t) - x0 > 50:
                axis.append(f'<line x1="{x(t):.1f}" y1="34" x2="{x(t):.1f}" y2="{height}" class="grid"/>'
                            f'<text x="{x(t):.1f}" y="30" class="tick" text-anchor="middle">{t:%H:%M}</text>')
            t += timedelta(seconds=step)

    for n, j in enumerate(jobs):
        y = 40 + n * ROW
        x0, x1 = x(j['started']), x(j['ended'])
        href = _esc(op.relpath(j['path'], folder))
        label = j['rule'] + (f" ({j['wildcards']})" if j['wildcards'] else '')
        state = {'log': 'succeeded', 'error': 'failed', 'stale': 'interrupted'}.get(j['state'], j['state'])
        if j['state'] == 'running':
            state = 'running, since'
        tip = [f"{label}: {state}", f"started {j['started']:%Y-%m-%d %H:%M:%S}, {_fmt_duration(j['duration'])}"]
        if j['peak_commands']:
            tip.append(f"peak memory {_fmt_memory(j['peak_commands'])}")
        if j['error']:
            tip.append(j['error'])
        mark = ' critical' if j['log'] in critical else ''
        parts = [f'<a href="{href}"><text x="{LABEL - 8}" y="{y + 12}" class="label" text-anchor="end">'
                 f'{_esc(label)}</text></a>',
                 f'<rect x="{x0:.1f}" y="{y + 2}" width="{max(x1 - x0, 2):.1f}" height="{ROW - 4}" '
                 f'fill="{colors.get(j["state"], "#888")}" class="job{mark}"><title>{_esc(chr(10).join(tip))}</title></rect>']
        for k, c in enumerate(j['commands']):
            if c['duration'] is None:
                continue
            cs = j['started'] + timedelta(seconds=c['start'])
            cx0, cx1 = x(cs), x(cs + timedelta(seconds=c['duration']))
            ctip = [c['text'][:200], f"{_fmt_duration(c['duration'])}, exit code {c['exit_code']}"]
            if c['peak']:
                ctip.append(f"peak memory {_fmt_memory(c['peak'])}")
            parts.append(f'<rect x="{cx0:.1f}" y="{y + 5}" width="{max(cx1 - cx0, 1):.1f}" height="{ROW - 10}" '
                         f'class="cmd c{k % 2}"><title>{_esc(chr(10).join(ctip))}</title></rect>')
        text = _fmt_duration(j['duration']) + (f" / {_fmt_memory(j['peak_commands'])}" if j['peak_commands'] else '')
        if j['state'] == 'running':
            text = 'running ' + text
        parts.append(f'<text x="{x1 + 4:.1f}" y="{y + 12}" class="info">{_esc(text)}</text>')
        rows.append(''.join(parts))
    # queued jobs: waiting for their turn, after the others
    xq = x(totals['now']) if periods else LABEL
    for n, j in enumerate(queued, len(jobs)):
        y = 40 + n * ROW
        label = j['rule'] + (f" ({j['wildcards']})" if j['wildcards'] else '')
        rows.append(f'<text x="{LABEL - 8}" y="{y + 12}" class="label queued" text-anchor="end">{_esc(label)}</text>'
                    f'<text x="{xq + 4:.1f}" y="{y + 12}" class="info">queued</text>')

    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {LABEL + WIDTH + 160} {height + 10}" '
           f'width="100%" style="min-width: 1000px">'
           + ''.join(axis) + ''.join(rows) + '</svg>')

    # per rule: number of jobs, and the median and maximum of duration and memory
    by_rule = {}
    for j in jobs:
        by_rule.setdefault((j['rule'], j['cls']), []).append(j)
    table = []
    for j in queued:
        by_rule.setdefault((j['rule'], j['cls']), []).append(j)
    for (rule, cls), js in sorted(by_rule.items(), key=lambda kv: -sum(j['duration'] for j in kv[1] if j['state'] == 'log')):
        ok = [j for j in js if j['state'] == 'log']
        n = lambda state: sum(j['state'] == state for j in js) or ''
        durations = [j['duration'] for j in ok]
        memories = [j['peak_commands'] for j in ok if j['peak_commands']]
        table.append(
            f"<tr><td>{_esc(rule)}</td><td>{_esc(cls)}</td><td>{len(js)}</td><td>{len(ok) or ''}</td>"
            f"<td>{n('error')}</td><td>{n('running')}</td><td>{n('queued')}</td>"
            f"<td>{_fmt_duration(statistics.median(durations)) if durations else ''}</td>"
            f"<td>{_fmt_duration(max(durations)) if durations else ''}</td>"
            f"<td>{_fmt_duration(sum(durations)) if durations else ''}</td>"
            f"<td>{_fmt_memory(statistics.median(memories)) if memories else ''}</td>"
            f"<td>{_fmt_memory(max(memories)) if memories else ''}</td></tr>")

    missing = []
    states = [(totals['failed'], 'failed'), (totals['running'], 'running'), (totals['queued'], 'queued'),
              (totals['stale'], 'interrupted'), (totals['not_run'], 'not run yet')]
    others = ', '.join(f'{n} {what}' for n, what in states if n)
    if others:
        missing.append(f"The totals count the {totals['succeeded']} jobs that succeeded; {others}.")
    left = ''
    if totals['left'] is not None or totals['left_unknown']:
        known = f"about {_fmt_duration(totals['left'])}" if totals['left'] is not None else 'not known yet'
        left = (f"<tr><td>time left (estimate)</td><td>{known}"
                + (f", not counting {totals['left_unknown']} job{'s' if totals['left_unknown'] > 1 else ''} "
                   f"of rules that have no finished job yet"
                   if totals['left_unknown'] else '') + '</td></tr>')
    span = (max(j['ended'] for j in jobs) - min(j['started'] for j in jobs)).total_seconds() if jobs else 0
    page = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
{f'<meta http-equiv="refresh" content="{refresh}"/>' if refresh else ''}
<title>{_esc(title)}</title>
<style>
body {{ font-family: sans-serif; margin: 40px; color: #333; }}
table {{ border-collapse: collapse; margin: 10px 0 30px; }}
th, td {{ text-align: left; padding: 4px 12px 4px 0; border-bottom: 1px solid #eee; }}
th {{ background: #f4f4f4; }}
.totals td:first-child {{ color: #666; }}
.timeline {{ overflow-x: auto; border: 1px solid #ddd; border-radius: 8px; padding: 10px; }}
.label {{ font-size: 12px; fill: #333; }}
a .label:hover {{ fill: #0645ad; text-decoration: underline; }}
.info {{ font-size: 11px; fill: #666; }}
.tick {{ font-size: 11px; fill: #666; }}
.gap {{ font-size: 10px; fill: #999; font-style: italic; }}
.grid {{ stroke: #eee; }}
.gapline {{ stroke: #bbb; stroke-dasharray: 3 3; }}
.job.critical {{ stroke: #000; stroke-width: 1.5; }}
.cmd.c0 {{ fill: rgba(255,255,255,0.35); }}
.cmd.c1 {{ fill: rgba(0,0,0,0.15); }}
.updated {{ color: #666; }}
.label.queued {{ fill: #999; }}
.legend span {{ display: inline-block; width: 12px; height: 12px; margin: 0 4px 0 14px; vertical-align: middle; }}
</style>
</head>
<body>
<h1>{_esc(title)}</h1>
<p class="updated">{f"Updated every {refresh} seconds; " if refresh else ''}as of {totals['now']:%Y-%m-%d %H:%M:%S}.</p>
<table class="totals">
<tr><td>jobs</td><td>{totals['jobs']}</td></tr>
{left}
<tr><td>compute time (sum of the jobs)</td><td>{_fmt_duration(totals['compute'])}</td></tr>
<tr><td>critical path (with enough cores)</td><td>{_fmt_duration(totals['critical'])}</td></tr>
<tr><td>from first start to last end</td><td>{_fmt_duration(span)}{' (with the gaps between runs)' if len(periods) > 1 else ''}</td></tr>
<tr><td>peak memory of a job</td><td>{_fmt_memory(totals['peak'])}</td></tr>
</table>
<p>{_esc(' '.join(missing))}</p>
<p class="legend"><span style="background:#4c8bd6"></span>succeeded<span style="background:#d64c4c"></span>failed
<span style="background:#b0a040"></span>interrupted<span style="background:#60b060"></span>running<span style="border:1.5px solid #000"></span>on the critical path.
Bars show the commands of a job; hover over them for details, click a job's name for its log file.</p>
<div class="timeline">{svg}</div>
<h2>Per rule</h2>
<table>
<tr><th>rule</th><th>class</th><th>jobs</th><th>succeeded</th><th>failed</th><th>running</th><th>queued</th><th>median time</th><th>max time</th><th>total time</th>
<th>median memory</th><th>max memory</th></tr>
{''.join(table)}
</table>
</body>
</html>
"""
    os.makedirs(folder, exist_ok=True)
    with open(output_path, 'w') as fp:
        fp.write(page)
    return output_path


def create_timeline(builder, target, output_path, title=None, exclude_target=False, refresh=None):
    """Write the timeline of the jobs that `target` depends on to `output_path`; returns the totals."""
    runs, totals, critical, not_run = collect(builder, target, exclude_target)
    render(runs, totals, critical, not_run, title or f'Timeline of {target.name}', output_path, refresh)
    return totals


def summary(totals):
    """One line: '37 of 50 jobs succeeded, 2 failed, 4 running, 7 queued; about 1h 20m left'."""
    parts = [f"{totals['succeeded']} of {totals['jobs']} jobs succeeded"]
    parts += [f"{totals[k]} {what}" for k, what in [('failed', 'failed'), ('running', 'running'),
                                                    ('queued', 'queued'), ('stale', 'interrupted')] if totals[k]]
    text = ', '.join(parts)
    if totals['left'] is not None:
        text += f"; about {_fmt_duration(totals['left'])} left"
    elif totals['left_unknown']:
        text += "; time left not known yet"
    return text


def default_target(builder):
    from . import TargetRule
    targets = [r for r in builder.rules.values() if isinstance(r, TargetRule)]
    if targets:
        return targets[0]
    return next((r for r in builder.rules.values() if not r._wildcard_names()), None)


def main(argv=None):
    from .diagram import load_pipeline, _parse_config, SNAKEFILE_LOCATIONS
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('snakefile', nargs='?', help='default: Snakefile or workflow/Snakefile, like Snakemake')
    parser.add_argument('-t', '--target', help='default: the default target of the pipeline')
    parser.add_argument('-o', '--output', help="default: timeline.html in the target's result folder, "
                                                "where the target writes it when it finishes")
    parser.add_argument('-w', '--watch', type=int, metavar='SECONDS',
                        help='write the timeline again every so many seconds, until no job of the target '
                             'is running or queued; the page reloads itself')
    parser.add_argument('--config', nargs='*', metavar='KEY=VALUE', help='same as snakemake --config')
    parser.add_argument('--configfile', nargs='*', help='same as snakemake --configfile')
    parser.add_argument('--directory', help='working directory, same as snakemake --directory')
    args = parser.parse_args(argv)

    snakefile = args.snakefile or next((f for f in SNAKEFILE_LOCATIONS if op.isfile(f)), None)
    if snakefile is None:
        parser.error(f"no Snakefile found here ({', '.join(SNAKEFILE_LOCATIONS)}); give its path")
    folder = op.dirname(op.abspath(snakefile))
    if op.basename(folder) == 'workflow':
        folder = op.dirname(folder)
    given = op.abspath(args.output) if args.output else None  # before Snakemake changes the working directory
    builder = load_pipeline(snakefile, _parse_config(args.config), args.configfile, args.directory)
    if args.directory:
        os.chdir(args.directory)  # the log files are relative to it
    if args.target:
        target = builder.rules.get(args.target)
        if target is None or target._wildcard_names():
            parser.error(f"{args.target} is not a target or a rule without wildcards; the targets are: "
                         + ', '.join(n for n, r in builder.rules.items() if not r._wildcard_names()))
    else:
        target = default_target(builder)
    output = given or op.abspath(op.join(target.result_path({}), 'timeline.html'))
    if not args.watch:
        totals = create_timeline(builder, target, output)
        print(f"Timeline written to: {output}\n{summary(totals)}")
        return
    import time
    print(f"Writing the timeline to {output} every {args.watch} seconds; stop with Ctrl-C")
    # in a terminal, each update replaces the previous one; in a file, each gets its own line
    same_line = sys.stdout.isatty()
    try:
        while True:
            totals = create_timeline(builder, target, output, refresh=args.watch)
            line = f"{totals['now']:%H:%M:%S} {summary(totals)}"
            if same_line:
                # a line that wraps could not be replaced as a whole
                import shutil
                line = line[:shutil.get_terminal_size().columns - 1]
            print(f"\r\x1b[K{line}" if same_line else line, end='' if same_line else '\n', flush=True)
            if not totals['running'] and not totals['queued']:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        pass
    totals = create_timeline(builder, target, output)  # the last one does not reload itself
    print(f"\r\x1b[KDone: {summary(totals)}" if same_line else f"Done: {summary(totals)}")


if __name__ == '__main__':
    main()
