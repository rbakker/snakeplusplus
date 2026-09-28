import os
from os import path as op
import json
from itertools import zip_longest
import string
import re
import glob
import sys

from typing import Any, Dict, Tuple, Type, Union
from types import SimpleNamespace
from pydantic import BaseModel, Field, ConfigDict, TypeAdapter, Json
from pydantic_core import PydanticUndefined

from .jobmonitor import JobMonitor, JobResult, ReportedError, UpstreamFailedError, formatUpstreamFailures, format_exception_remapped, replaceInnerExtension, stampFile, logFromStamp, jobStateFile, JOB_STATES, STAMP_DIR

import inspect
import typing
import functools
from beartype.door import TypeHint


# Public API: what `from snakeplusplus import *` exports and what the reference docs show.
__all__ = [
    # building blocks for rule classes
    'SnakeRule', 'SnakeCheckpoint', 'TargetRule',
    'Fixed', 'Tolerant', 'Extensible', 'Field', 'Json',
    # runtime objects passed to SnakeRule.run()
    'JobMonitor', 'JobResult',
    # Snakefile functions
    'configure', 'build', 'target', 'get_builder', 'SNAKEFILE',
    # errors and helpers
    'ParserAnnotationError', 'UpstreamFailedError', 'LoopError', 'get_parser_types',
]


_builder = None

# Snakemake part of snakeplusplus; end a Snakefile with `include: snakeplusplus.SNAKEFILE` and `snakeplusplus.build(locals())`
SNAKEFILE = op.join(op.dirname(op.abspath(__file__)), 'snakeplusplus.smk')
_read_only = False  # set by snakeplusplus.doc: parse the Snakefile without touching any files

def get_builder():
    if _builder is None:
        raise RuntimeError("PipelineBuilder has not been configured. Call snakeplusplus.configure(...) first.")
    return _builder


class Fixed(BaseModel):
    model_config = ConfigDict(extra="forbid",validate_default=True)


class Tolerant(BaseModel):
    model_config = ConfigDict(extra="ignore",validate_default=True)


class Extensible(BaseModel):
    model_config = ConfigDict(extra="allow",validate_default=True)


class PartialFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            if key in kwargs:
                return kwargs[key]
            # leave unknown fields untouched
            return "{" + key + "}"
        return string.Formatter.get_value(self, key, args, kwargs)

partial_formatter = PartialFormatter()


def is_assignable(actual: type, expected: type) -> bool:
    """True if a value of type `actual` can satisfy a field of type `expected`."""
    return TypeHint(actual) <= TypeHint(expected)


def _validate_keys(model, data):
    required = {name for name, f in model.model_fields.items() if f.is_required()}
    allowed = set(model.model_fields)

    missing = required - data.keys()
    extra_keys = data.keys() - allowed

    extra_mode = model.model_config.get("extra", "ignore")

    errors = []
    if missing:
        errors.append(f"missing={missing}")
    if extra_keys and extra_mode == "forbid":
        errors.append(f"extra={extra_keys}")

    if errors:
        raise ValueError("Model keys mismatch, "+' '.join(errors))


def _validate_key(model,key):
    #"""Check whether `key` is a valid field name for `model`."""
    if key not in model.model_fields:
        raise KeyError( f"Key mismatch, {key!r} is not a defined for {model.__name__} " )


class ParserAnnotationError(TypeError):
    """Raised when a parser passed to get_output lacks required type hints."""


class LoopError(ReportedError):
    """Raised when the wildcard iterator of a `foreach` loop fails."""


def _positional_params(parser):
    try:
        sig = inspect.signature(parser)
    except (ValueError, TypeError) as e:
        raise ParserAnnotationError(
            f"Could not inspect signature of parser function {parser!r}: {e}"
        ) from e
    return [
        p for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL)
    ]


def get_parser_types(parser: typing.Callable) -> tuple[type, type]:
    """Returns (input_type, output_type) for a parser function.

    Raises ParserAnnotationError if the parser is missing annotations,
    since this check must happen before any promises are resolved.
    """
    target = parser.func if isinstance(parser, functools.partial) else parser

    params = [p for p in _positional_params(parser) if p.kind != p.VAR_POSITIONAL]
    if not params:
        raise ParserAnnotationError(
            f"Parser {parser!r} must accept at least one positional argument"
        )

    hints = typing.get_type_hints(target)

    first_param = params[0]
    if first_param.name not in hints:
        raise ParserAnnotationError(
            f"Parser {parser!r} must use type hints to enable code validation. Missing for input '{first_param.name}'."
        )
    if "return" not in hints:
        raise ParserAnnotationError(
            f"Parser {parser!r} must use type hints to enable code validation. Missing for returned variable."
        )

    return hints[first_param.name], hints["return"]


def _parser_wants_wildcards(parser) -> bool:
    """A parser gets the wildcards as 2nd argument only if it has a 2nd positional argument
    without default value, or one that is named `wildcards`."""
    params = _positional_params(parser)
    if any(p.kind == p.VAR_POSITIONAL for p in params):
        return True
    if len(params) < 2:
        return False
    second = params[1]
    return second.default is inspect.Parameter.empty or second.name == 'wildcards'


def _apply_parser(parser, value, wildcards):
    if parser is None:
        return value
    return parser(value, wildcards) if _parser_wants_wildcards(parser) else parser(value)


def _output_value(result, key):
    """Get output `key` from a JobResult, or the JobResult itself if key is None.
    Returns None if the output is not listed in the log file (e.g. because the job failed)."""
    if key is None:
        return result
    try:
        return getattr(result, key)
    except AttributeError:
        return None


def _is_incomplete_checkpoint(exc):
    # Snakemake uses this exception for control flow: it must never be caught.
    return type(exc).__name__ == 'IncompleteCheckpointException'


def _is_input_function(val):
    return callable(val) and not isinstance(val, (SnakeRule, JobLoop, OutputPromise, type))


class SnakeRule:
    name = "[no name]" # name of this rule, may vary per instance
    description = "[no description]" # description of what this rule does

    class WildcardModel(Extensible): # describes wildcards and how they appear in file names
        pass

    class InputModel(Extensible): # describes expected inputs
        pass

    class OutputModel(Extensible): # describes outputs + default file names
        pass

    class ParamModel(Extensible): # describes parameters + defaults
        pass


    threads = 8
    conda_env = None
    default_target = False
    is_checkpoint = False

    # What to do when a job that provides input to this rule has failed (has a .error file):
    # - False: this job fails too, with an error that lists the failed upstream jobs (default).
    # - True: run anyway, for all inputs.
    # - a set of input names, e.g. {'tracts'}: run anyway if only those inputs have failures.
    # Tolerated failures are listed in `job.failed_inputs`, and their outputs resolve to None.
    allow_failed_inputs = False


    def __init__(self, params=None):
        self.params = params or {} # params are validated in the configure step.
        self.inputs = {}


    def set_input(self, **inputs):
        _validate_keys(self.InputModel,inputs)

        # type-check the inputs in sofar possible at build time
        model_fields = self.InputModel.model_fields
        for key, field in model_fields.items():
            if key not in inputs:
                continue

            expected_type = field.annotation
            value = inputs[key]

            if isinstance(value, (SnakeRule, JobLoop)):
                value = value.get_output()
                inputs[key] = value

            if isinstance(value, OutputPromise):
                actual_type = value.annotation # validates rule.get_output() and foreach().get_output()
                if not is_assignable(actual_type, expected_type):
                    raise TypeError(
                        f"Type mismatch: {type(value.rule_or_loop).__name__}.{value.key} "
                        f"produces {actual_type!r}, but {type(self).__name__}.{key} "
                        f"expects {expected_type!r}"
                    )
            elif _is_input_function(value):
                # input function, its result is only known at run time
                pass
            else:
                # concrete value passed directly — validate it for real, right now
                TypeAdapter(expected_type).validate_python(value)

        # promises that are not covered by model_fields (Extensible InputModel)
        for key, value in inputs.items():
            if isinstance(value, (SnakeRule, JobLoop)):
                inputs[key] = value.get_output()

        self.inputs = inputs
        return self


    # Only to be used as argument to set_input() of another SnakeRule, see _resolved_inputs().
    def get_output(self, key=None, parser=None):
        if key is not None:
            _validate_key(self.OutputModel, key)

        # return specific output
        return OutputPromise(self, key, parser)


    def foreach(self, *wildcard_list_of_dicts, **wildcard_dict_of_lists):
        return JobLoop(self, *wildcard_list_of_dicts, **wildcard_dict_of_lists)


    def run(self, job, input, output, params, wildcards):
        raise NotImplementedError


    def log_path(self, wildcards=None):
        fmt = self._log_template()
        return fmt if wildcards is None else partial_formatter.format(fmt, **wildcards)


    def result_path(self, wildcards=None):
        fmt = self._result_template()
        return fmt if wildcards is None else fmt.format(**dict(wildcards))


    @classmethod
    def describe(cls):
        print(f"{cls.__name__}: {cls.description}")
        for title, model in [('wildcards', cls.WildcardModel), ('inputs', cls.InputModel),
                              ('outputs', cls.OutputModel), ('params', cls.ParamModel)]:
            print(f"{cls.__name__} {title}:")
            for key, field in model.model_fields.items():
                default = '' if field.default is PydanticUndefined else f" (default={field.default!r})"
                print(f"  {key}: {field.description}{default}")


    # ---- internals, called by PipelineBuilder and Snakemake ---- #

    def _configure(self,name,config_params):
        self.name = name

        # check for unused params
        unused = set(config_params) - set(self.ParamModel.model_fields)
        if unused:
            raise ValueError(f"Rule {name} has unused parameters: {unused}")

        # merge parameters (constructor params have priority over config params)
        params = {**config_params, **self.params}
        _validate_keys(self.ParamModel,params)
        self.params = params

        # Complete parameter set including defaults. Snakemake stores it with the job's
        # log file and reruns the job when it changes (its `params` rerun trigger).
        self._snake_params = {
            key: field.get_default(call_default_factory=True)
            for key, field in self.ParamModel.model_fields.items()
            if not field.is_required()
        }
        self._snake_params.update(params)

        allowed = self.allow_failed_inputs
        if isinstance(allowed, str):
            allowed = self.allow_failed_inputs = {allowed}
        if allowed and allowed is not True:
            unknown = set(allowed) - set(self.inputs)
            if unknown:
                raise ValueError(f"Rule {name}: allow_failed_inputs contains unknown inputs {unknown}")


    # Snakemake will ensure that _run_job is called after all input log files have been created.
    # Everything that can go wrong here ends up in the .log and .error files, not in Snakemake.
    def _run_job(self, rule_context):
        wildcards = rule_context['wildcards']
        raw_params = rule_context['params'] # params passed as functions are resolved in rule_context
        stamp = rule_context['output'][0]  # what Snakemake tracks; the log file is derived from it
        log_path = logFromStamp(stamp)

        setup_error = None
        try:
            params = self.ParamModel(**raw_params)
            params_dict = params.model_dump(mode='json')
        except Exception as e:
            # parameter discrepancies are reported inside the JobMonitor context below
            params, params_dict, setup_error = None, dict(raw_params.items()), e

        try:
            descr = self._descr(wildcards)
            result_path = self.result_path(wildcards)
            output_patterns = {
                key: partial_formatter.format(field.default, params=raw_params, **wildcards)
                for key, field in self.OutputModel.model_fields.items()
                if isinstance(field.default, str)
            }
        except Exception as e:
            descr, result_path, output_patterns = self.name, None, None
            setup_error = setup_error or e

        with JobMonitor(log_path, descr, result_path, output_patterns=output_patterns,
                        shell_context=rule_context, linemaps=get_builder().linemaps,
                        params=params_dict, stampFile=stamp) as job:
            if setup_error:
                raise setup_error
            self._check_failed_inputs(job, wildcards)
            inputs = self._resolved_inputs(wildcards)
            # TO DO: use InputModel(inputs)
            self.run(job, inputs, job.result, params, wildcards)


    def _check_failed_inputs(self, job, wildcards):
        """Find upstream jobs that failed, and fail this job unless allowed by `allow_failed_inputs`."""
        failed = {}
        for key, val in self.inputs.items():
            if isinstance(val, OutputPromise):
                logs = val.rule_or_loop.log_path(wildcards)  # a failed loop iterator raises here
                logs = logs if isinstance(logs, list) else [logs]
                bad = [(log, job.checkError(log)) for log in logs]
                bad = [(log, err) for log, err in bad if err]
                if bad:
                    failed[key] = bad

        job.failed_inputs = {key: [jobStateFile(log) for log, _ in bad] for key, bad in failed.items()}

        allowed = self.allow_failed_inputs
        blocking = [
            (log, err, key) for key, bad in failed.items() for log, err in bad
            if not (allowed is True or (allowed and key in allowed))
        ]
        if blocking:
            raise UpstreamFailedError(formatUpstreamFailures(job.jobName, blocking))

        for key, logs in job.failed_inputs.items():
            job.log(f"Warning: input '{key}' has {len(logs)} failed job(s), continuing because of allow_failed_inputs.")


    def _resolve(self, wildcards, key, parser):
        val = _output_value(JobResult(self.log_path(wildcards)), key)
        return _apply_parser(parser, val, wildcards)


    def _resolved_inputs(self, wildcards):
        inp = {}
        for key, val in self.inputs.items():
            if isinstance(val, str):
                # simple filename input
                inp[key] = partial_formatter.format(val, **wildcards)
            elif isinstance(val, (list, tuple)):
                inp[key] = [partial_formatter.format(v, **wildcards) for v in val]
            elif isinstance(val, OutputPromise):
                inp[key] = val.rule_or_loop._resolve(wildcards, val.key, val.parser)
            elif _is_input_function(val):
                inp[key] = val(wildcards)
            else:
                inp[key] = val

        return SimpleNamespace(**inp)


    def _descr(self, wildcards):
        if len(wildcards):
            d = dict(wildcards)
            kvpairs = ','.join(f"{k}={v}" for k, v in d.items())
            return f"{self.name}({type(self).__name__})<{kvpairs}>"
        return self.name


    def _input_paths(self, snake_checkpoint_magic):
        # snake_checkpoint_magic makes the checkpoints variable available

        paths = []
        for key, val in self.inputs.items():
            if isinstance(val, str):
                # simple filename input
                paths.append(val)
            elif isinstance(val, (list, tuple)):
                # multiple filename input
                paths.append(val)
            elif isinstance(val, OutputPromise):
                # input via a SnakeRule or a loop over a SnakeRule; account for checkpoints here
                rule_or_loop = val.rule_or_loop
                if isinstance(rule_or_loop, JobLoop):
                    paths.append(snake_checkpoint_magic(rule_or_loop._input_stamps))
                else:
                    paths.append(snake_checkpoint_magic(
                        lambda wildcards, rule=rule_or_loop: stampFile(rule.log_path(wildcards))))
            elif _is_input_function(val):
                # input function that returns paths, account for checkpoints here
                paths.append(snake_checkpoint_magic(val))

        return paths


    def _wildcard_parts(self):
        parts = []
        for key, field in self.WildcardModel.model_fields.items():
            pattern = field.default if isinstance(field.default, str) else '{}'
            parts.append(pattern.format('{' + key + '}'))
        parts.append(self.name)
        return parts


    def _log_template(self):
        return op.join(get_builder().pathvars.get('logs'), '_'.join(self._wildcard_parts()) + ".log")


    def _existing_jobs(self, wildcards=None):
        """Wildcards of this rule's jobs that have a job file, and that agree with `wildcards`.
        Only files matching this rule's own log file pattern are looked at."""
        template = self._log_template()
        names = re.findall(r'\{(\w+)\}', template)
        regex, pos = '', 0
        for m in re.finditer(r'\{(\w+)\}', template):
            regex += re.escape(template[pos:m.start()]) + f'(?P<{m.group(1)}>[^/]+?)'
            pos = m.end()
        regex = re.compile(regex + re.escape(template[pos:]) + '$')
        pattern = re.sub(r'\{\w+\}', '*', template)
        found = []
        for state in JOB_STATES:
            for f in glob.glob(replaceInnerExtension(pattern, state)):
                m = regex.match(replaceInnerExtension(f, '.log'))
                if m and all(m[k] == wildcards[k] for k in names if wildcards and k in wildcards):
                    if m.groupdict() not in found:
                        found.append(m.groupdict())
        return found


    def _result_template(self):
        return op.join(get_builder().pathvars.get('results'), *self._wildcard_parts())


    def _as_snake(self, snake_checkpoint_magic):
        # The rule's only output is the job's stamp file; its inputs are the stamps of upstream jobs.
        return SimpleNamespace(
            name=self.name,
            input=self._input_paths(snake_checkpoint_magic),
            params=self._snake_params,
            output=stampFile(self.log_path()),
            threads=self.threads,
            default_target=self.default_target,
            conda=self.conda_env,
            run_job=self._run_job,
            checkpoint=self.is_checkpoint,
        )


class OutputPromise:
    def __init__(self, rule_or_loop, key=None, parser=None):
        self.rule_or_loop = rule_or_loop
        self.key = key
        self.parser = parser
        self._annotation = self._compute_annotation()  # computes the expected output type of this promise

    def _compute_annotation(self):
        is_loop = isinstance(self.rule_or_loop, JobLoop)

        if self.key is None:
            # No specific field: the promise resolves to the whole JobResult,
            # not to a value described by rule.OutputModel.
            base = JobResult
        else:
            rule = self.rule_or_loop.rule if is_loop else self.rule_or_loop
            base = rule.OutputModel.model_fields[self.key].annotation

        if is_loop and not self.rule_or_loop.scalar:
            base = typing.List[base]

        if self.parser is None:
            return base

        parser_in, parser_out = get_parser_types(self.parser)

        if not is_assignable(base, parser_in):
            raise TypeError(
                f"Parser {self.parser!r} expects input {parser_in!r}, but "
                f"{type(self.rule_or_loop).__name__}.{self.key} produces {base!r}"
            )
        return parser_out

    @property
    def annotation(self):
        return self._annotation


class JobLoop:
    def __init__(self,rule,*wildcard_list_of_dicts,**wildcard_dict_of_lists):
        """
        Use wildcard_list_of_dicts to define a group of wildcards.
        - Each element is a list of dicts, or a function(parent_wildcards) that yields dicts,
          containing wildcard name/value pairs.

        Use wildcard_dict_of_lists as an alternative or supplemental method.
        - Each item contains value(s) for a single wildcard name.
        - The value can be a single string or array of strings; all arrays must have the same length.

        The loop is 'scalar' (resolves to a single value instead of a list) if it only
        uses wildcard_dict_of_lists with single values.
        """
        self.rule = rule
        self.wildcard_iterables = list(wildcard_list_of_dicts)

        lengths = {len(v) for v in wildcard_dict_of_lists.values() if isinstance(v, (list, tuple))}
        if len(lengths) > 1:
            raise ValueError(f"foreach: all lists must have the same length, got lengths {sorted(lengths)}")
        self.scalar = not wildcard_list_of_dicts and not lengths

        if wildcard_dict_of_lists:
            count = lengths.pop() if lengths else 1
            def dict_of_lists_iterator(parent_wildcards):
                for i in range(count):
                    yield {k: (v[i] if isinstance(v, (list, tuple)) else v)
                           for k, v in wildcard_dict_of_lists.items()}
            self.wildcard_iterables.append(dict_of_lists_iterator)


    def get_output(self, key=None, parser=None):
        if key is not None:
            _validate_key(self.rule.OutputModel, key)
        return OutputPromise(self, key, parser)


    def wildcard_iterator(self, parent_wildcards):
        parent_wildcards = dict(parent_wildcards) if parent_wildcards else {}
        iterables = [
            it(parent_wildcards) if callable(it) else iter(it)
            for it in self.wildcard_iterables
        ]

        first = None

        for dicts in zip_longest(*iterables, fillvalue=None):
            if first is None:
                first = dicts

            merged = dict(parent_wildcards)
            for i, d in enumerate(dicts):
                merged.update(d or first[i] or {})

            yield merged


    def members(self, parent_wildcards=None):
        """List of wildcard dicts, one per loop member. Raises LoopError if the iterator fails."""
        try:
            return list(self.wildcard_iterator(parent_wildcards))
        except Exception as e:
            if _is_incomplete_checkpoint(e):
                raise
            if isinstance(e, ReportedError):
                detail = str(e)
            else:
                linemaps = _builder.linemaps if _builder else None
                detail = "".join(format_exception_remapped(type(e), e, e.__traceback__, linemaps)).rstrip()
            raise LoopError(
                f"Wildcard iterator of loop over rule '{self.rule.name}' failed:\n{detail}"
            ) from e


    def _resolve(self,wildcards,key,parser):
        val = [ _output_value(JobResult(self.rule.log_path(wc)), key) for wc in self.members(wildcards) ]
        val = val[0] if self.scalar else val
        return _apply_parser(parser, val, wildcards)


    # The log_path of a loop contains a list with all log_paths of its members.
    # wildcard_iterator may depend on a checkpoint.
    def log_path(self,parent_wildcards=None):
        val = [ self.rule.log_path(wc) for wc in self.members(parent_wildcards) ]
        return val[0] if self.scalar else val


    # Input function for Snakemake. If the iterator fails (typically after a failed checkpoint),
    # the loop contributes no inputs, and the error is reported by the downstream job instead
    # of crashing Snakemake.
    def _input_stamps(self, parent_wildcards):
        try:
            logs = self.log_path(parent_wildcards)
            return stampFile(logs) if isinstance(logs, str) else [stampFile(log) for log in logs]
        except LoopError as e:
            if str(e) not in _reported_loop_errors and get_builder().is_main_process:
                _reported_loop_errors.add(str(e))
                print(f"Warning: {e}\n=> the error will be reported in the log of the job that uses this loop.",
                      file=sys.stderr)
            return []


_reported_loop_errors = set()  # print each deferred loop error only once


class SnakeCheckpoint(SnakeRule):
    is_checkpoint = True


class TargetRule(SnakeRule):
    class InputModel(Extensible):
        pass

    default_target = True
    def run(self,job,input,output,params,wildcards):
        print('All jobs done.')


class PipelineBuilder:
    def __init__(self,pathvars,config_params,env={},retry_failed=True):
        self.pathvars = pathvars
        self.retry_failed = retry_failed
        self.config_params = config_params
        self.env = env
        self.rules = {}           # name -> SnakeRule, filled by build()
        self.target_rule = None   # set by target()
        self.linemaps = None      # Snakemake line maps, for correct line numbers in error messages
        self.is_main_process = True  # False when Snakemake re-parses the Snakefile to run a job


    # Collect all rules in the scope of namespace.
    # And give the rules a unique name.
    def _collect_rules(self,namespace):
        passed = {}
        for name, obj in namespace.items():
            if isinstance(obj, SnakeRule):
                obj._configure(name,self.config_params.get(obj.__class__.__name__,{}))
                passed[name] = obj

        return passed


    def target(self, rule):
        if not isinstance(rule, SnakeRule):
            raise TypeError(f"target() expects a SnakeRule, got {type(rule).__name__}")
        self.target_rule = rule


    def _sync_job_states(self):
        """Derive Snakemake's stamp files from the state of this pipeline's job files.

        Walks the pipeline from every rule without wildcards (the potential targets) down through
        set_input()/get_output(), job by job with the right wildcards, and checks each job's file:

        - outdated: no job file (never run, or deleted by the user); .stale; .error (if
          retry_failed); or any upstream job is outdated
        - up to date: .log, and .error if not retry_failed

        Outdated jobs lose their stamp, and so does everything downstream of them up to the target,
        so Snakemake reruns exactly those jobs. Up-to-date jobs get their stamp back if it is
        missing (e.g. after .snakemake/ was deleted), with the log file's modification time.
        A .running file is renamed to .stale, since no job can be running at this point.

        Only files of this pipeline's jobs are looked at, so the log folder may contain other
        files. The result only depends on the job files, so a dry run followed by a real run gives
        the same plan. Code changes deliberately do not trigger reruns: after fixing a bug, only
        the failed jobs (and what depends on them) run again.

        Only done in the main Snakemake process, before the DAG is built, and not while another
        Snakemake process is working in this directory.
        """
        if not self.is_main_process or _read_only:
            return
        locks = op.join('.snakemake', 'locks')
        if op.isdir(locks) and os.listdir(locks):
            return  # another Snakemake process is active: leave everything as it is

        outdated = {}  # log path -> bool, for every job visited

        def visit(rule, wildcards):
            log = rule.log_path(wildcards)
            if log in outdated:
                return outdated[log]
            outdated[log] = False  # guards against cycles

            state_file = jobStateFile(log, JOB_STATES)
            if state_file and replaceInnerExtension(state_file, '.running') == state_file:
                with open(state_file, 'at') as fp:
                    fp.write('Job was interrupted (found .running while Snakemake was not active).\n')
                stale = replaceInnerExtension(log, '.stale')
                os.replace(state_file, stale)
                state_file = stale
            state = next((st for st in JOB_STATES
                          if state_file and replaceInnerExtension(state_file, st) == state_file), None)

            is_outdated = state is None or state == '.stale' or (state == '.error' and self.retry_failed)
            for up_rule, up_wildcards in self._upstream_jobs(rule, wildcards):
                if visit(up_rule, up_wildcards):
                    is_outdated = True
            outdated[log] = is_outdated

            stamp = stampFile(log)
            if is_outdated:
                if op.exists(stamp):
                    os.remove(stamp)
            elif not op.exists(stamp):
                os.makedirs(op.dirname(stamp), exist_ok=True)
                with open(stamp, 'wt') as fp:
                    fp.write(f'{state_file}\n(recreated)\n')
                t = os.stat(state_file).st_mtime
                os.utime(stamp, (t, t))
            return is_outdated

        for rule in self.rules.values():
            if not rule.WildcardModel.model_fields:
                visit(rule, {})


    def _upstream_jobs(self, rule, wildcards):
        """(rule, wildcards) of the jobs that the job (rule, wildcards) takes input from.

        A loop whose items cannot be determined while the Snakefile is loaded (it depends on a
        checkpoint, which Snakemake only resolves later) yields the existing jobs of the looped
        rule, found by their file names, whose wildcards agree with this job's wildcards."""
        for val in rule.inputs.values():
            if not isinstance(val, OutputPromise):
                continue
            source = val.rule_or_loop
            if isinstance(source, JobLoop):
                try:
                    members = source.members(wildcards)
                except Exception:
                    members = source.rule._existing_jobs(wildcards)
                for wc in members:
                    yield source.rule, self._wildcards_for(source.rule, wc)
            else:
                yield source, self._wildcards_for(source, wildcards)


    @staticmethod
    def _wildcards_for(rule, wildcards):
        return {k: wildcards[k] for k in rule.WildcardModel.model_fields if k in wildcards}


    def build(self,namespace,inject_rule,checkpoint_magic,verbose=False):
        """
        Build the snakemake pipeline from all rules present in namespace (typically locals() in snakefile)
        """
        self.rules = self._collect_rules(namespace)

        workflow = namespace.get('workflow')
        self.linemaps = getattr(workflow, 'linemaps', None)
        # Snakemake parses the Snakefile again for every job; only be verbose in the main process.
        self.is_main_process = getattr(workflow, 'is_main_process', True)
        verbose = verbose and self.is_main_process

        self._sync_job_states()

        # find target rule: explicit target(), else default_target, else first rule without wildcards
        first_target = None
        default_target = None
        for c in self.rules.values():
            if first_target is None and len(c.WildcardModel.model_fields) == 0:
                first_target = c
            if c.default_target:
                default_target = c
        target = self.target_rule or default_target or first_target
        if not target:
            raise RuntimeError(f'Could not figure out which of the {len(self.rules)} rules to run. Specify default_target.')

        for c in self.rules.values():
            c.default_target = c is target

        if verbose:
            print('Target rule:',target.name)

        # inject all available rules
        for rule in self.rules.values():
            r = rule._as_snake(checkpoint_magic)
            if verbose:
                print('Injecting rule\n',json.dumps(vars(r),indent=2,default=str))
            inject_rule(r)


# To be called from within the snakefile before build.
# retry_failed=True: jobs that failed in a previous run (have an .error file) are run again.
def configure(pathvars,config_params={},env={},retry_failed=True):
    global _builder
    _builder = PipelineBuilder(pathvars,config_params,env,retry_failed)


# Set the target rule to be resolved by Snakemake.
def target(rule):
    get_builder().target(rule)


# Make the snakefile ready for execution by Snakemake.
# inject_rule and checkpoint_magic are taken from the namespace if not given; they are defined
# in snakeplusplus.smk, see SNAKEFILE.
def build(namespace,inject_rule=None,checkpoint_magic=None,verbose=False):
    inject_rule = inject_rule or namespace.get('inject_rule')
    checkpoint_magic = checkpoint_magic or namespace.get('checkpoint_magic')
    if inject_rule is None or checkpoint_magic is None:
        raise RuntimeError("inject_rule/checkpoint_magic not found: add `include: snakeplusplus.SNAKEFILE` before build()")
    builder = get_builder()
    builder.build(namespace,inject_rule,checkpoint_magic,verbose)
