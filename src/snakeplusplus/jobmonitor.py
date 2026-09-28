import os
from os import path as op
from datetime import datetime
import traceback
import subprocess
import re
import json
import time
import tempfile
import shlex
import linecache

__all__ = ['JobMonitor', 'JobResult', 'ReportedError', 'UpstreamFailedError', 'replaceInnerExtension', 'params_to_json',
           'JOB_STATES', 'jobStateFile', 'stampFile', 'logFromStamp']


class ReportedError(RuntimeError):
    """An error whose message is complete; it is logged without traceback."""


class UpstreamFailedError(ReportedError):
    """Raised inside a job when a job it depends on has failed."""


# Replaces extension, but keeps extra extension if equal to `outerExt`.
# Returns file with new extension.
def replaceInnerExtension(fname,newExt,outerExt='.md'):
    noext,ext = op.splitext(fname)
    hasOuter = (ext==outerExt)
    if hasOuter:
        noext,ext = op.splitext(noext)
    return noext+newExt+(outerExt if hasOuter else '')


# One file per job, whose extension shows the state of the job:
#   .running  job is running (or was killed without cleaning up)
#   .log      job completed successfully
#   .error    job completed with errors
#   .stale    job was cancelled (Ctrl-C), or found `.running` while no Snakemake process was active
JOB_STATES = ('.running', '.log', '.error', '.stale')


# Returns the existing state file of a job (logFile is the `.log` name), or None.
def jobStateFile(logFile, states=('.log', '.error')):
    for state in states:
        f = replaceInnerExtension(logFile, state)
        if op.isfile(f):
            return f
    return None


# Snakemake does not track the log files themselves (their name changes with the job state),
# but an invisible stamp file per job, under .snakemake/ in the working directory.
STAMP_DIR = op.join('.snakemake', 'snakeplusplus', 'stamps')

def stampFile(logFile):
    return op.join(os.getcwd(), STAMP_DIR) + op.abspath(logFile)

def logFromStamp(stamp):
    return stamp.split(op.sep + STAMP_DIR, 1)[1]


# Formats exceptions with remapping line-numbers the same way as snakemake does internally
def format_exception_remapped(exc_type, exc_value, tb, linemaps=None):
    linemaps = linemaps or {}
    lines = []
    for frame in traceback.extract_tb(tb):
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
# `failed` is a list of (logFile, errorText) tuples, optionally with an input name as 3rd item.
def formatUpstreamFailures(jobName, failed):
    # group by upstream job, so that each upstream error is shown only once
    grouped = {}
    for dep, err, *label in failed:
        entry = grouped.setdefault(dep, [err, []])
        entry[1].extend(label)
    lines = [f'"{jobName}" did not run because {len(grouped)} job(s) it depends on failed:']
    for dep, (err, labels) in grouped.items():
        name = ', '.join(f"'{l}'" for l in labels)
        name = f'input {name}: ' if labels else ''
        lines.append(f'- {name}{op.splitext(dep)[0]}')
        lines.extend('    ' + l for l in err.rstrip('\n').splitlines())
    return '\n'.join(lines)


# Returns the error messages of a failed job (logFile is the `.log` name), or None if it did not fail.
def readErrors(logFile):
    errorFile = replaceInnerExtension(logFile, '.error')
    if not op.isfile(errorFile):
        return None
    errors = JobResult(errorFile)._errors
    return '\n'.join(errors) if errors else f'(see {errorFile})'


class JobResult():
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
            log_file = jobStateFile(log_file) or log_file
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
    def fromCheckpoint(cls,checkpoint_job,wildcards):
        # trigger checkpoint execution; raises snakemake's IncompleteCheckpointException
        # while the checkpoint has not run yet, which Snakemake handles itself.
        checkpoint_job.get(**wildcards)
        logFile = logFromStamp(checkpoint_job.rule.output[0].format(**wildcards))
        error = readErrors(logFile)
        if error:
            raise UpstreamFailedError(f'Checkpoint `{op.splitext(logFile)[0]}` failed due to:\n{error}')
        return cls(logFile)
        
      
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
                            json_str = "".join(reversed(buffer))
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


# JobMonitor tracks job progress and logs messages.
# It has a `run` method to use shell commands and capture their output in the log file.
#
# `logFile` is the file that contains the job log. There is one file per job; its extension
# shows the state of the job (see JOB_STATES): `.running` while the job runs, then `.log` on
# success or `.error` if any error was reported, or `.stale` if the job was cancelled.
# Error messages are also stored in the output mapping at the end of the file.
#
# `jobName` is a short descriptive name for the job, used in error messages and in the log file.
#
# `resultFolder` is the location where results are stored. Default: folder of logFile.
# If it ends with '*' as in /my/folder/subject01* then results are stored in '/my/folder' with filenames starting with 'subject01'.
#
# `stampFile`: written when the job completes (also with errors); this is the file Snakemake tracks.
#
# Errors raised inside the `with JobMonitor(...)` block are written to the log file and then
# swallowed, so that Snakemake continues with other jobs. Downstream jobs see the `.error` file
# and decide whether to fail too (see checkDependencies).
#
class JobMonitor():
    def __init__(self,logFile,jobName='Job',resultFolderOrPrefix=None,output_patterns=None,shell_context=None,linemaps=None,params=None,stampFile=None):
        if isinstance(logFile,str):
            self.logFile = logFile
        else:
            # This makes it possible in a Snakefile to use JobMonitor(log)
            self.logFile = logFile[0]
        self.jobName = jobName
        self.markdown = self.logFile.endswith('.md')
        self.shell_context = shell_context or {}
        self.params = params      # dict, written to line 4 of the log as single-line JSON
        self.linemaps = linemaps  # Snakemake's line maps, to report correct line numbers in the Snakefile
        self.failed_inputs = {}   # input name -> list of failed upstream log files (set by SnakeRule)
        self.stampFile = stampFile  # file that Snakemake tracks as the job's output, written on completion

        # fullPrefix is the combination of resultFolder and resultPrefix
        if not resultFolderOrPrefix:
            resultFolderOrPrefix = op.dirname(self.logFile)
        if resultFolderOrPrefix.endswith('*'):
            prefix = resultFolderOrPrefix[:-1]
        else:
            prefix = op.join(resultFolderOrPrefix,'')
            
        self.result = JobResult(self.logFile,prefix=prefix,create=True,output_patterns=output_patterns)
        
        # allow JobMonitor to be run out-of-context
        self.started = datetime.now()
        self.runningLog = None
        
        # works with tmpdir function
        self._tmpdir = None
            
            
    def tmpdir(self,*args):
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory()
        
        path = self._tmpdir.name
        if len(args):
            path = op.join(path,*args)
            os.makedirs(path,exist_ok=True)
        return path


    def tmpfile(self,name):
        return op.join(self.tmpdir(),name)


    def __enter__(self):
        self.started = datetime.now()
        logFolder = op.dirname(self.logFile) or '.'
        os.makedirs(logFolder,exist_ok=True)
            
        # one file per job: remove the files of a previous run of this job
        for state in JOB_STATES:
            f = replaceInnerExtension(self.logFile, state)
            if op.exists(f):
                os.remove(f)

        self.runningLog = replaceInnerExtension(self.logFile,'.running')
        with open(self.runningLog,'wt') as fp:
            fp.write(f'"{self.jobName}" started at {self.started}, saving output to\n{self.result()}\n')
            if self.params:
                fp.write(f'and using parameters\n{params_to_json(self.params)}\n')
            else:
                fp.write('\n\n')
        return self


    def __exit__(self, exc_type, exc_value, tb):
        self.stopped = datetime.now()
        elapsed = self.stopped-self.started

        if self._tmpdir:
            try:
                self._tmpdir.cleanup()
            except Exception as e:
                self.log(f'Warning: could not remove temporary folder: {e}')
            self._tmpdir = None

        if exc_type is not None and not issubclass(exc_type, Exception):
            # KeyboardInterrupt/SystemExit: the job was cancelled. No stamp file is written,
            # so Snakemake will rerun the job next time.
            self.log(f'"{self.jobName}" was cancelled after {elapsed} hh:mm:ss ({exc_type.__name__}).')
            staleLog = replaceInnerExtension(self.logFile, '.stale')
            os.rename(self.runningLog, staleLog)
            self.runningLog = staleLog
            return False

        if exc_type is None:
            # Process is ready.
            self.log(f'"{self.jobName}" completed in {elapsed} hh:mm:ss.')
        else:
            # An error occured. Report it.
            # Use linemaps to get correct location of error in snakefile.
            if isinstance(exc_value, ReportedError):
                # no traceback needed, the message says it all
                err = str(exc_value)
            else:
                err = "".join(format_exception_remapped(exc_type, exc_value, tb, linemaps=self.linemaps))
            self.log(f'"{self.jobName}" failed after {elapsed} hh:mm:ss.')
            self.error(err)

        with open(self.runningLog,'at') as fp:
            fp.write('Output mapping\n')
            json.dump(self.result._output_mapping(),fp,indent=2)
            
        finalLog = replaceInnerExtension(self.logFile, '.error') if self.result._errors else self.logFile
        os.rename(self.runningLog, finalLog)
        self.runningLog = finalLog

        if self.stampFile:
            # Snakemake's output. New content each time, so that downstream jobs see a changed checksum.
            os.makedirs(op.dirname(self.stampFile), exist_ok=True)
            with open(self.stampFile, 'wt') as fp:
                fp.write(f'{finalLog}\n{datetime.now().isoformat()}\n')

        # Swallow ordinary errors: they are in the .error file now,
        # and for Snakemake the job succeeded.
        return True

            
    # return error message, if any
    def checkError(self,logFile=None):
        if logFile is None:
            logFile = self.logFile
        if not isinstance(logFile,str):
            logFile = logFile[0]
        return readErrors(logFile)
        

    # fail this job if it depends on another failed job
    def checkDependency(self,logFile):
        self.checkDependencies([logFile])


    # Check whether all upstream dependencies completed without error;
    # only files ending with .log are checked. All failures are reported at once.
    def checkDependencies(self,dependencies):
        failed = [(dep, self.checkError(dep)) for dep in dependencies if dep.endswith('.log')]
        failed = [(dep, err) for dep, err in failed if err]
        if failed:
            raise UpstreamFailedError(formatUpstreamFailures(self.jobName, failed))


    def log(self,msg,timeIt=True):
        with open(self.runningLog,'at') as fp:
            if timeIt:
                elapsed = datetime.now()-self.started
                fp.write(f'[{elapsed}] {msg}\n')
            else:
                fp.write(f'{msg}\n')


    def error(self, msg):
        if isinstance(msg, BaseException):
            tb = "".join(traceback.format_exception(type(msg), msg, msg.__traceback__))
            msg = f"{msg}\n{tb}"
        else:
            msg = str(msg)

        self.log(f"Error: {msg}", timeIt=False)

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
                self.log(formatted, timeIt=False)

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
            failOnError=True, updateInterval_s=None, formatter=None):

        msg = f'Running process `{subprocess.list2cmdline(cmd)}`'
        print(f'{msg},\n=> output to {self.logFile}.')
        self.log(msg)

        if self.markdown:
            self.log('```\n')

        if updateInterval_s is not None:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cwd,
                env=env,
                bufsize=1
            )

            output = self._periodic_log(p.stdout, updateInterval_s, formatter)
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
                self.log(output + ("" if output.endswith("\n") else "\n"))

        if self.markdown:
            self.log('```\n')

        returnCode = p.returncode
        if returnCode != 0:
            error = output if len(output)<=1000 else '...\n'+output[-1000:]
            if failOnError:
                raise RuntimeError(error)
            else:
                self.error(error)

        return returnCode, output
                
    
    def shell(self,script):
        from snakemake import shell as sh
        sh(f'set +euo pipefail; exec >> {shlex.quote(self.runningLog)} 2>&1; '+script,**self.shell_context)
