import os
from os import path as op
from datetime import datetime
import traceback
import subprocess
import re
import json
import time
import tempfile
import shutil
from pathlib import Path
import shlex
import linecache

__all__ = ['JobMonitor', 'JobResult', 'JobError', 'UpstreamFailedError', 'CommandFailedError', 'format_command', 'escape_fences', 'replace_inner_extension', 'params_to_json', 'previous_run',
           'JOB_STATES', 'job_state_file']


class JobError(RuntimeError):
    """A job failed, with a message that says it all: it is logged without Python traceback.

    - Raise it in `run()` to fail a job with a clear message, e.g. `raise JobError('no b-values')`.
    - A downstream rule receives a failed job's output as JobError, with the error message of
      that job, but only for inputs whose InputModel type admits it, e.g.
      `tracts: list[Path | JobError]`. For other inputs, the downstream job fails as well.
    """
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        # an isinstance check: lets JobError be used as a field type in pydantic models
        from pydantic_core import core_schema
        return core_schema.is_instance_schema(cls)

    @classmethod
    def _from_log(cls, log_file):
        """JobError of a failed job (log_file is its `.log` name), or None if it did not fail."""
        errors = read_errors(log_file)
        if errors is None:
            return None
        name = op.basename(replace_inner_extension(log_file, '.error'))
        errors = errors.rstrip('\n')
        if name not in errors:
            errors += f'\n(log-file: {name})'
        return cls(errors)


class UpstreamFailedError(JobError):
    """Raised inside a job when a job it depends on has failed."""


# Replaces extension, but keeps extra extension if equal to `outer_ext`.
# Returns file with new extension.
def replace_inner_extension(fname,new_ext,outer_ext='.md'):
    noext,ext = op.splitext(fname)
    has_outer = (ext==outer_ext)
    if has_outer:
        noext,ext = op.splitext(noext)
    return noext+new_ext+(outer_ext if has_outer else '')


# One file per job, whose extension shows the state of the job:
#   .queued   job will run in the current Snakemake run (empty if it never ran before;
#             otherwise the log of the previous run, with a note appended)
#   .running  job is running (or was killed without cleaning up)
#   .log      job completed successfully
#   .error    job completed with errors
#   .stale    job was cancelled (Ctrl-C), or found `.running` while no Snakemake process was active
JOB_STATES = ('.queued', '.running', '.log', '.error', '.stale')


# Returns the existing state file of a job (log_file is the `.log` name), or None.
def job_state_file(log_file, states=('.log', '.error')):
    for state in states:
        f = replace_inner_extension(log_file, state)
        if op.isfile(f):
            return f
    return None


# Snakemake does not track the log files themselves (their name changes with the job state),
# but an invisible stamp file per job, under .snakemake/ in the working directory, at
# STAMP_DIR/<rule>/<wildcard values>/job (see SnakeRule._stamp_path).
STAMP_DIR = op.join('.snakemake', 'snakeplusplus', 'stamps')


class CommandFailedError(JobError):
    """Raised by JobMonitor.run when a command exits with a nonzero code. Unlike other JobErrors,
    it is logged with the place in the user's code where the command was started."""


_PACKAGE_DIR = op.dirname(op.abspath(__file__))


_CLOSING_FENCE = re.compile(r'^( {0,3}`{3,}[ \t]*)$', re.MULTILINE)

def escape_fences(text):
    """Indent lines that would close a Markdown code block (only backticks, at least three),
    so that a command's output cannot end the code block it is written in."""
    return _CLOSING_FENCE.sub(r'    \1', text)


def format_elapsed(td):
    """A timedelta as h:mm:ss, without fractions of seconds."""
    return str(td).split('.')[0]


def format_command(cmd):
    """A command as readable text: the program, then one argument per line, with an option and
    the one value that follows it on the same line. Arguments are quoted only where needed."""
    import shlex
    words = [shlex.quote(str(c)) for c in cmd]
    if not words:
        return ''
    def is_option(w):
        return len(w) > 1 and w[0] == '-' and (w[1].isalpha() or w[1] == '-')
    lines, current = [], None
    for w in words[1:]:
        if is_option(w) or current is None or not is_option(current[0]) or len(current) > 1:
            if current is not None:
                lines.append(' '.join(current))
            current = [w]
        else:
            current.append(w)
    if current is not None:
        lines.append(' '.join(current))
    return words[0] + ''.join('\n    ' + l for l in lines)


# Formats exceptions with remapping line-numbers the same way as snakemake does internally.
# Frames inside the snakeplusplus package are left out (unless all frames are), so that the
# traceback only shows the user's own code.
def format_exception_remapped(exc_type, exc_value, tb, linemaps=None, skip_internal=True):
    linemaps = linemaps or {}
    lines = []
    frames = traceback.extract_tb(tb)
    if skip_internal:
        own = [f for f in frames if not op.abspath(f.filename).startswith(_PACKAGE_DIR + op.sep)]
        frames = own or frames
    for frame in frames:
        lineno = frame.lineno
        file_map = linemaps.get(frame.filename)
        if file_map and lineno in file_map:
            lineno = file_map[lineno]
            text = linecache.getline(frame.filename, lineno).strip()
        else:
            text = frame.line
        lines.append(f'  File "{frame.filename}", line {lineno}, in {frame.name}\n    {text}\n')
    lines += traceback.format_exception_only(exc_type, exc_value)
    return lines


# Canonical single-line JSON of a parameter dict: keys sorted, so that the same
# parameters always give the same string (usable as identifier of the parameter set).
def params_to_json(params):
    return json.dumps(params, sort_keys=True, default=str, ensure_ascii=False)


# Builds the error message for a job that depends on failed jobs.
# `failed` is a list of (log_file, error_text) tuples, optionally with an input name as 3rd item.
def format_upstream_failures(job_name, failed):
    # group by upstream job, so that each upstream error is shown only once
    grouped = {}
    for dep, err, *label in failed:
        entry = grouped.setdefault(dep, [err, []])
        entry[1].extend(label)
    lines = [f'"{job_name}" did not run because {len(grouped)} job(s) it depends on failed:']
    for dep, (err, labels) in grouped.items():
        name = ', '.join(f"'{l}'" for l in labels)
        name = f'input {name}: ' if labels else ''
        lines.append(f'- {name}{op.splitext(dep)[0]}')
        lines.extend('    ' + l for l in str(err).rstrip('\n').splitlines())
    return '\n'.join(lines)


# Returns the error messages of a failed job (log_file is the `.log` name), or None if it did not fail.
def read_errors(log_file):
    error_file = replace_inner_extension(log_file, '.error')
    if not op.isfile(error_file):
        return None
    errors = JobResult(error_file)._errors
    return '\n'.join(errors) if errors else f'(see {error_file})'


# The previous run of a job, from its current file in any state (log_file is the `.log` name):
# (start time, result prefix, output mapping), or None if there is none or it cannot be read.
def previous_run(log_file):
    state_file = job_state_file(log_file, JOB_STATES)
    if state_file is None:
        return None
    try:
        with open(state_file, 'rt') as fp:
            m = re.search(r' started at (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d), saving output to$', fp.readline().rstrip('\n'))
            prefix = fp.readline().rstrip('\n')
        if not m or not prefix:
            return None
        started = datetime.strptime(m[1], '%Y-%m-%d %H:%M:%S')
        result = JobResult.__new__(JobResult)
        result._log_file = state_file
        mapping = result._load_mapping_from_log() or {}
    except (OSError, ValueError):
        return None
    return started, prefix, mapping


class JobResult():
    """The outputs of a job.

    In `run()` it is the `output` argument: `output.name` is the full path of output `name`, in the
    job's result folder, and `output(name=value)` declares an output, e.g. a number or a file name
    that is only known while the job runs.

    A rule that is connected to another rule without output name gets the other job's JobResult,
    with the same attribute access: `input.tracking.tracts`.
    """
    def __init__(self, log_file, prefix=None, create=False, output_patterns=None):
        self._log_file = log_file
        self._create = create
        self._output_patterns = output_patterns or {}
        self._compiled_patterns = []
        self._named_outputs = {}
        self._numbered_outputs = []
        self._errors = []
        self._warnings = []
        
        if self._create:
            if prefix:
                self._prefix = prefix
            else:
                self._prefix = op.join(op.dirname(log_file),'')
            if output_patterns:
                self._compile_patterns(output_patterns)
        else:
            # a job's log is called .log on success and .error on failure
            log_file = job_state_file(log_file) or log_file
            self._log_file = log_file
            if not op.exists(log_file):
                raise FileNotFoundError(f"Log file not found: {log_file}")

            with open(log_file, 'rt') as fp:
                fp.readline()
                self._prefix = fp.readline().rstrip('* \n')
            
            mapping = self._load_mapping_from_log()
            if mapping:
                self._apply_mapping(mapping)


    @classmethod
    def from_checkpoint(cls,checkpoint_job,wildcards):
        """The result of a checkpoint, in the iterator function of a loop: `checkpoint_job` is
        `checkpoints.<name>` in the Snakefile. Snakemake runs the checkpoint first if needed."""
        # trigger checkpoint execution; raises snakemake's IncompleteCheckpointException
        # while the checkpoint has not run yet, which Snakemake handles itself.
        checkpoint_job.get(**wildcards)
        from . import _get_builder, _as_dict
        rule = _get_builder().rules[checkpoint_job.rule.name]
        log_file = rule.log_path(rule._own_wildcards(_as_dict(wildcards)))
        error = read_errors(log_file)
        if error:
            raise UpstreamFailedError(f'Checkpoint `{op.splitext(log_file)[0]}` failed due to:\n{error}')
        return cls(log_file)
        
      
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        # lets JobResult be used as a field type in pydantic models, e.g. a rule's InputModel
        from pydantic_core import core_schema
        return core_schema.is_instance_schema(cls)


    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)

        value = None
        if name in self._named_outputs:
            value = self._named_outputs[name]
        elif self._create:
            if name in self._output_patterns:
                value = self._output_patterns[name]
                self._named_outputs[name] = value
            elif (matched_value := self._match_pattern(name)) is not None:
                value = matched_value
                self._named_outputs[name] = value
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        return self._resolve_value(value) if isinstance(value,str) else value


    def _compile_patterns(self, patterns):
        """Pre-compiles keys with {wildcards} into regex objects."""
        for pattern, template in patterns.items():
            if '{' in pattern and '}' in pattern:
                # Escape literal chars (like dots or hyphens)
                regex_str = re.escape(pattern)
                # Re-enable the brackets for wildcard replacement
                regex_str = regex_str.replace(r'\{', '{').replace(r'\}', '}')
                # Convert {name} to named capture groups
                regex_str = re.sub(r'\{(\w+)\}', r'(?P<\1>.+?)', regex_str)
                
                # Store the compiled regex along with the original template
                self._compiled_patterns.append((re.compile(f"^{regex_str}$"), template))


    def _match_pattern(self, name):
        """Uses the pre-compiled cache to find a match."""
        for regex, template in self._compiled_patterns:
            match = regex.match(name)
            if match:
                try:
                    return template.format(**match.groupdict())
                except KeyError:
                    continue
        return None
              
              
    def __getitem__(self, idx):
        if isinstance(idx, (int, slice)):
            value = self._numbered_outputs[idx]
            return self._resolve_value(value)
        elif isinstance(idx, str):
            return self.__getattr__(idx)
        else:
            raise TypeError(f"Invalid index type: {type(idx).__name__}")


    def __call__(self, *args, **kwargs):
        if len(args) + len(kwargs) > 1:
            raise TypeError(
                f"JobResult.call takes only one positional or keyword argument, got {len(args)} + {len(kwargs)}."
            )
        if kwargs:
            (name, value), = kwargs.items()
            self._named_outputs[name] = value
            if not isinstance(value, (str, list, tuple)):
                return value  # a plain value, not a file name
        elif args:
            value, = args
            self._numbered_outputs.append(value)
        else:
            value = ''

        return self._resolve_value(value)
    

    def _apply_mapping(self, mapping):
        self._numbered_outputs.extend(mapping.get('by_number', []))
        self._named_outputs.update(mapping.get('by_name', {}))
        self._errors.extend(mapping.get('errors', []))
        self._warnings.extend(mapping.get('warnings', []))


    def _load_mapping_from_log(self):
        try:
            with open(self._log_file, 'rb') as f:
                f.seek(0, os.SEEK_END)
                pointer = f.tell()
                buffer = []
                while pointer > 0:
                    pointer -= 1
                    f.seek(pointer)
                    char = f.read(1).decode('ascii', errors='ignore')
                    buffer.append(char)
                    if char == '{':
                        is_sol = pointer == 0
                        if not is_sol:
                            f.seek(pointer - 1)
                            if f.read(1).decode('ascii', errors='ignore') in ['\n', '\r']:
                                is_sol = True
                        if is_sol:
                            json_str = "".join(reversed(buffer)).strip()
                            if '```' in json_str:  # closing Markdown code fence, maybe followed by a note
                                json_str = json_str[:json_str.rindex('```')]
                            return json.loads(json_str)
        except (OSError, json.JSONDecodeError):
            return None
        return None


    def _resolve_value(self, value):
        if isinstance(value, (list, tuple)):
            return self._file(*value)
        return self._file(value)


    def _output_mapping(self):
        mapping = dict()
        if self._numbered_outputs:
            mapping['by_number'] = self._numbered_outputs
        if self._named_outputs:
            mapping['by_name'] = self._named_outputs        
        if self._errors:
            mapping['errors'] = self._errors        
        if self._warnings:
            mapping['warnings'] = self._warnings
        return mapping


    def _file(self, *args):
        if args:
            # Note: str(args[0]) handles if the first arg is already a path string
            result_file = op.join(self._prefix + str(args[0]), *map(str, args[1:]))
        else:
            result_file = self._prefix
            
        if self._create:
            os.makedirs(op.dirname(result_file) or '.', exist_ok=True)
        return result_file


    def _folder(self, *args):
        return op.dirname(self._file(*args) or '.')


    def _append_error(self, msg):
        self._errors.append(msg)


    def _append_warning(self, msg):
        self._warnings.append(msg)


class JobMonitor():
    """The `job` argument of `SnakeRule.run()`: runs commands and writes the job's log file.

    There is one log file per job. Its extension shows the state of the job: `.queued`, `.running`,
    then `.log` on success or `.error` if any error was reported, or `.stale` if the job was
    cancelled. The output mapping at the end of the file lists the job's outputs.

    Errors raised inside `run()` are written to the log file and do not stop Snakemake, so that
    other jobs continue. A job that uses the output of a failed job fails as well, with the original
    error in its log, unless its input type admits `JobError`.

    `job.threads` is the number of threads that Snakemake reserved for this job; pass it on to the
    tool, e.g. `job.run(['tckgen', '-nthreads', str(job.threads), ...])`.

    It can also be used without snakeplusplus rules, in a plain Snakemake rule, as
    `with JobMonitor(log[0], 'my job') as job: ...`.
    """
    def __init__(self,log_file,job_name='Job',result_folder_or_prefix=None,output_patterns=None,shell_context=None,linemaps=None,params=None,stamp_file=None,tmpdir_autodelete=True,threads=1):
        if isinstance(log_file,str):
            self.log_file = log_file
        else:
            # This makes it possible in a Snakefile to use JobMonitor(log)
            self.log_file = log_file[0]
        self.job_name = job_name
        self.markdown = self.log_file.endswith('.md')
        self.shell_context = shell_context or {}
        self.params = params      # dict, written to line 4 of the log as single-line JSON
        self.linemaps = linemaps  # Snakemake's line maps, to report correct line numbers in the Snakefile
        self.stamp_file = stamp_file  # file that Snakemake tracks as the job's output, written on completion

        # full_prefix is the combination of result_folder and result_prefix
        if not result_folder_or_prefix:
            result_folder_or_prefix = op.dirname(self.log_file)
        if result_folder_or_prefix.endswith('*'):
            prefix = result_folder_or_prefix[:-1]
        else:
            prefix = op.join(result_folder_or_prefix,'')
            
        self.result = JobResult(self.log_file,prefix=prefix,create=True,output_patterns=output_patterns)
        
        # allow JobMonitor to be run out-of-context
        self.started = datetime.now()
        self.running_log = None
        
        self._tmpdir = None  # created by tmpdir() on first use
        self.tmpdir_autodelete = tmpdir_autodelete
        self.threads = threads  # the number of threads Snakemake reserved for this job


    def tmpdir(self):
        """The temporary folder of this job, as a Path. It is created on first use, in the system's
        temporary location (the TMPDIR environment variable), and deleted when the job ends,
        unless `tmpdir_autodelete` is False; its location is then written to the log."""
        if self._tmpdir is None:
            safe = re.sub(r'[^\w.-]+', '_', self.job_name)[:60]
            self._tmpdir = Path(tempfile.mkdtemp(prefix=f'{safe}-'))
        return self._tmpdir


    def __enter__(self):
        self.started = datetime.now()
        log_folder = op.dirname(self.log_file) or '.'
        os.makedirs(log_folder,exist_ok=True)
            
        # create .running first, so that the job always has a file (see PipelineBuilder._mark_queued)
        self.running_log = replace_inner_extension(self.log_file,'.running')
        with open(self.running_log,'wt') as fp:
            # the result prefix as it is; the folder is created when the job writes there
            fp.write(f'"{self.job_name}" started at {self.started:%Y-%m-%d %H:%M:%S}, saving output to\n{self.result._prefix}\n')
            if self.params:
                fp.write(f'and using parameters\n{params_to_json(self.params)}\n')
            else:
                fp.write('\n\n')

        # one file per job: remove the files of a previous run of this job
        self._remove_other_states(keep='.running')
        return self


    def _write_mapping(self):
        with open(self.running_log,'at') as fp:
            fp.write('\nOutput mapping\n\n```json\n')
            json.dump(self.result._output_mapping(),fp,indent=2,default=str)
            fp.write('\n```\n')


    def _remove_other_states(self, keep):
        for state in JOB_STATES:
            f = replace_inner_extension(self.log_file, state)
            if state != keep and op.exists(f):
                os.remove(f)


    def __exit__(self, exc_type, exc_value, tb):
        self.stopped = datetime.now()
        elapsed = self.stopped-self.started

        if self._tmpdir:
            if self.tmpdir_autodelete:
                try:
                    shutil.rmtree(self._tmpdir)
                except Exception as e:
                    self.log(f'Warning: could not remove temporary folder: {e}')
            else:
                self.log(f'Temporary folder kept: {self._tmpdir}')
            self._tmpdir = None

        if exc_type is not None and not issubclass(exc_type, Exception):
            # KeyboardInterrupt/SystemExit: the job was cancelled. No stamp file is written,
            # so Snakemake will rerun the job next time.
            self.log(f'"{self.job_name}" was cancelled after {format_elapsed(elapsed)} (h:mm:ss, {exc_type.__name__}).')
            self._write_mapping()  # so that partial outputs can be cleaned up when the job reruns
            stale_log = replace_inner_extension(self.log_file, '.stale')
            os.rename(self.running_log, stale_log)
            self.running_log = stale_log
            return False

        if exc_type is None:
            # Process is ready.
            self.log(f'"{self.job_name}" completed in {format_elapsed(elapsed)} (h:mm:ss).')
        else:
            # An error occured. Report it: the message first, then where it happened in the
            # user's code (with line numbers of the Snakefile, via Snakemake's linemaps).
            if isinstance(exc_value, JobError) and not isinstance(exc_value, CommandFailedError):
                # no traceback needed, the message says it all
                err = str(exc_value)
            else:
                where = format_exception_remapped(exc_type, exc_value, tb, linemaps=self.linemaps)[:-1]
                if isinstance(exc_value, CommandFailedError):
                    message = str(exc_value)
                else:
                    message = ''.join(traceback.format_exception_only(exc_type, exc_value)).rstrip()
                err = message + '\n' + ''.join(where).rstrip()
            self.error(err)
            self.log(f'"{self.job_name}" failed after {format_elapsed(elapsed)} (h:mm:ss).')

        self._write_mapping()
        final_log = replace_inner_extension(self.log_file, '.error') if self.result._errors else self.log_file
        os.rename(self.running_log, final_log)
        self.running_log = final_log
        self._remove_other_states(keep=op.splitext(final_log)[1])  # e.g. a .queued created meanwhile

        if self.stamp_file:
            # Snakemake's output. New content each time, so that downstream jobs see a changed checksum.
            os.makedirs(op.dirname(self.stamp_file), exist_ok=True)
            with open(self.stamp_file, 'wt') as fp:
                fp.write(f'{final_log}\n{datetime.now().isoformat()}\n')

        # Swallow ordinary errors: they are in the .error file now,
        # and for Snakemake the job succeeded.
        return True

            
    # For JobMonitor without snakeplusplus rules (which check their inputs themselves):
    # fail this job if any of the upstream jobs with these log files has failed.
    # Only files ending with .log are checked. All failures are reported at once.
    def check_dependencies(self,dependencies):
        failed = [(dep, read_errors(dep)) for dep in dependencies if dep.endswith('.log')]
        failed = [(dep, err) for dep, err in failed if err]
        if failed:
            raise UpstreamFailedError(format_upstream_failures(self.job_name, failed))


    def log(self,msg,time_it=True):
        """Write a message to the log file, by default with the time since the job started."""
        with open(self.running_log,'at') as fp:
            if time_it:
                elapsed = datetime.now()-self.started
                fp.write(f'[{format_elapsed(elapsed)}] {msg}\n')
            else:
                fp.write(f'{msg}\n')


    def error(self, msg):
        """Report an error without stopping `run()`; the job ends as failed (`.error`).
        To stop the job right away, raise `JobError` instead."""
        if isinstance(msg, BaseException):
            tb = "".join(traceback.format_exception(type(msg), msg, msg.__traceback__))
            msg = f"{msg}\n{tb}"
        else:
            msg = str(msg)

        self.log(f"Errors occurred:\n\n```\n{escape_fences(msg.rstrip())}\n```\n")

        # recorded in the output mapping; any error turns the log into an `.error` file
        self.result._append_error(msg)


    def _periodic_log(self,p_stdout,interval,formatter):
        lines = []
        def flush():
            if formatter is None:
                formatted = '\n'.join(lines)
            else:
                formatted = formatter(lines)

            if formatted:
                self.log(escape_fences(formatted), time_it=False)

            lines.clear()        
            return formatted
                    
        last_flush = time.time()
        output = []
        for line in iter(p_stdout.readline, ''):
            lines.append(line.rstrip('\n'))

            if (time.time()-last_flush >= interval):
                output.append( flush() )
                last_flush = time.time()
                    
        if len(lines):
            output.append( flush() )
            
        return '\n'.join(output)
      

    def run(self, cmd, cwd=None, timeout=None, env=None,
            fail_on_error=True, update_interval=None, formatter=None):
        """Run a command, with its output in the log file.

        Args:
            cmd: The command as a list, e.g. `['mrconvert', input.dwi, output.mif]`; each argument
                is passed as it is, so no quoting is needed.
            cwd: Folder to run the command in.
            timeout: Maximum run time in seconds.
            env: Environment variables for the command (default: those of the job).
            fail_on_error: If True, a nonzero exit code fails the job. If False, it is reported
                as an error in the log, and `run()` continues.
            update_interval: For long commands: write their output to the log every this many
                seconds while they run, instead of when they finish.
            formatter: Function that gets a list of output lines and returns the text to log,
                e.g. to leave out progress bars. Only used with `update_interval`.

        Returns:
            The exit code and the output (stdout and stderr) of the command.
        """

        print(f'Running process `{subprocess.list2cmdline(cmd)}`,\n=> output to {self.log_file}.')
        self.log(f'Running command:\n\n```\n{format_command(cmd)}\n```\n')

        # the command's output goes into a Markdown code block, closed even if the command
        # raises (e.g. a timeout), so that whatever follows is not swallowed by the block
        start = self._open_output_block()
        try:
            p, output = self._run_process(cmd, cwd, timeout, env, update_interval, formatter)
        finally:
            self._close_output_block(start)

        return_code = p.returncode
        if return_code != 0:
            # the command's output is already in the log; the error only says what failed
            # this job will end as .error, so that is the file the reader should open
            log_name = op.basename(replace_inner_extension(self.log_file, '.error'))
            error = f'{op.basename(str(cmd[0]))} exited with code {return_code} (log-file: {log_name})'
            if fail_on_error:
                raise CommandFailedError(error)
            else:
                self.error(error)

        return return_code, output


    def _run_process(self, cmd, cwd, timeout, env, update_interval, formatter):
        if update_interval is not None:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cwd,
                env=env,
                bufsize=1
            )

            output = self._periodic_log(p.stdout, update_interval, formatter)
            p.wait()
        else:
            p = subprocess.run(
                cmd,
                cwd=cwd,
                timeout=timeout,
                env=env,
                capture_output=True,
                text=True
            )

            # Log stdout
            output = (p.stdout or "") + (p.stderr or "")
            if output:
                self.log(escape_fences(output.rstrip("\n")), time_it=False)
        return p, output
                
    
    _OUTPUT_HEADER = 'Output:\n\n```\n'

    def _open_output_block(self):
        """Start the code block for a command's output; returns where it starts in the log."""
        start = os.path.getsize(self.running_log)
        with open(self.running_log, 'at') as fp:
            fp.write(self._OUTPUT_HEADER)
        return start

    def _close_output_block(self, start):
        """Close the code block, or leave it out if the command wrote nothing."""
        if os.path.getsize(self.running_log) == start + len(self._OUTPUT_HEADER):
            with open(self.running_log, 'r+') as fp:
                fp.truncate(start)
        else:
            self.log('```\n', time_it=False)


    def shell(self,script):
        """Run a shell script, e.g. a command with pipes or redirection, with its output in the log
        file. If the script exits with a nonzero code (that of its last command), the job fails.

        Example:
            ```python
            job.shell(f"zcat {input.table} | cut -f1 > {output.ids}")
            ```
        """
        from snakemake import shell as sh
        self.log(f'Running shell script:\n\n```\n{escape_fences(script.strip())}\n```\n')
        start = self._open_output_block()
        try:
            sh(f'set +euo pipefail; exec >> {shlex.quote(self.running_log)} 2>&1; '+script,**self.shell_context)
        finally:
            self._close_output_block(start)
