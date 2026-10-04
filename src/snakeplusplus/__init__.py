import os
from os import path as op
import json
from itertools import zip_longest
import string
import re
import glob
import sys
import shutil
from datetime import datetime

from typing import Any, Dict, Tuple, Type, Union
from types import SimpleNamespace
from pydantic import BaseModel, Field, ConfigDict, TypeAdapter, Json
from pydantic_core import PydanticUndefined

from .jobmonitor import JobMonitor, JobResult, JobError, UpstreamFailedError, previous_run, format_upstream_failures, format_exception_remapped, replace_inner_extension, job_state_file, JOB_STATES, STAMP_DIR

import inspect
import pathlib
import typing
import functools
from beartype.door import TypeHint


# Public API: what `from snakeplusplus import *` exports and what the reference docs show.
__all__ = [
    # building blocks for rule classes
    'SnakeRule', 'SnakeCheckpoint', 'Fixed', 'Extensible', 'Field', 'Json', 'PathPrefix',
    # what SnakeRule.run() receives
    'JobMonitor', 'JobResult', 'JobError',
    # Snakefile functions
    'configure', 'target', 'build',
]


_builder = None

# Snakemake part of snakeplusplus, included by build(). (Snakefiles that include it themselves still work.)
SNAKEFILE = op.join(op.dirname(op.abspath(__file__)), 'snakeplusplus.smk')
_read_only = False  # set by snakeplusplus.doc: parse the Snakefile without touching any files

def _get_builder():
    if _builder is None:
        raise RuntimeError("PipelineBuilder has not been configured. Call snakeplusplus.configure(...) first.")
    return _builder


# Base classes for the models of a rule. Any class can be used as a field type (checked with isinstance).
class Fixed(BaseModel):
    """Base class for a model that accepts only the fields it declares. This is the default: a model
    declared without base class, e.g. `class InputModel:`, becomes a Fixed model."""
    model_config = ConfigDict(extra="forbid",validate_default=True,arbitrary_types_allowed=True)


class Extensible(BaseModel):
    """Base class for a model that also accepts fields it does not declare, e.g. a rule that takes
    any number of inputs."""
    model_config = ConfigDict(extra="allow",validate_default=True,arbitrary_types_allowed=True)


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


def _validate_key(model, key, owner):
    """Check whether `key` is a field name of `model`, or a pattern like 'DRT_{hemi}' that matches one."""
    if '{' in key:
        if not _key_matches(model, key):
            raise KeyError(f"{owner} has no output that matches {key!r}")
    elif key not in model.model_fields:
        raise KeyError(f"{owner} has no output {key!r}")


def _key_matches(model, pattern):
    """Field names of `model` that match a key pattern like 'DRT_{hemi}'."""
    regex = ''.join(re.escape(part) if i % 2 == 0 else '.+'
                    for i, part in enumerate(re.split(r'\{(\w+)\}', pattern)))
    return [name for name in model.model_fields if re.fullmatch(regex, name)]


class ParserAnnotationError(TypeError):
    """Raised when a parser passed to get_output lacks required type hints."""


class LoopError(JobError):
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


def _get_parser_types(parser: typing.Callable) -> tuple[type, type]:
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


class PathPrefix(type(pathlib.Path())):
    """An output that is the start of file names, for tools that write several files with a common
    prefix, e.g. `prefix: PathPrefix = Field('{tract}({hemi})_')`. Like a Path output, it gets the
    job's result path in front. A job must create at least one file whose name starts with it, and
    on a rerun those files are removed like other outputs. Use it as `f'{output.prefix}tracts.tck'`.

    With an empty name, `Field('')`, it is the job's own prefix (or folder): all files of the job,
    e.g. for a tool with an `-output_prefix` option and a `result_template` that ends with `*`.
    """
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        from pydantic_core import core_schema
        def validate(value):
            if isinstance(value, cls):
                return value
            if isinstance(value, (str, os.PathLike)):
                return cls(value)
            raise ValueError(f'expected a path prefix (str or path), got {type(value).__name__}')
        return core_schema.no_info_plain_validator_function(
            validate, serialization=core_schema.to_string_ser_schema())

    def matches(self):
        """Existing files and folders whose name starts with this prefix."""
        return _prefix_matches(str(self))


def _prefix_matches(prefix):
    # a trailing separator would be removed by Path, so prefixes are handled as strings
    return sorted(glob.glob(glob.escape(prefix) + '*'))


def _is_prefix_annotation(annotation):
    """True for PathPrefix, also as `PathPrefix | None`."""
    if annotation is PathPrefix:
        return True
    return any(a is PathPrefix for a in typing.get_args(annotation))


def _path_annotation(annotation):
    """(is_path, optional) for an OutputModel field: Path or a Path subclass, optionally `| None`."""
    import pathlib
    import types
    args = typing.get_args(annotation)
    optional = False
    if typing.get_origin(annotation) in (typing.Union, types.UnionType) and type(None) in args:
        optional = True
        rest = [a for a in args if a is not type(None)]
        if len(rest) != 1:
            return False, optional
        annotation = rest[0]
    is_path = isinstance(annotation, type) and issubclass(annotation, (pathlib.PurePath, os.PathLike))
    return is_path, optional


def _created_since(path, since, recursive=False):
    """True if `path` was modified at or after `since` (a datetime or timestamp); with `recursive`,
    a folder and everything in it. Symbolic links are not followed. A margin of 2 seconds covers
    file systems with a coarse time resolution."""
    since = since.timestamp() if isinstance(since, datetime) else since
    newer = lambda p: os.lstat(p).st_mtime >= since - 2
    if not newer(path):
        return False
    if recursive:
        for root, dirs, files in os.walk(path):
            if not all(newer(op.join(root, f)) for f in dirs + files):
                return False
    return True


def _admits_job_error(annotation):
    """True if JobError appears anywhere in a type annotation, e.g. `list[Path | JobError]`."""
    if annotation is JobError:
        return True
    return any(_admits_job_error(a) for a in typing.get_args(annotation))


def _is_incomplete_checkpoint(exc):
    # Snakemake uses this exception for control flow: it must never be caught.
    return type(exc).__name__ == 'IncompleteCheckpointException'


def _threads_for(budget, cores, owner='rule'):
    """The number of threads for a threads_budget: a whole number, or an expression of `cores`
    such as 'cores/2' or 'cores-2' (rounded down), kept between 1 and `cores`."""
    import ast
    if isinstance(budget, bool) or not isinstance(budget, (int, str)):
        raise TypeError(f"{owner}.threads_budget must be a whole number or an expression such as "
                        f"'cores/2', got {budget!r}")
    if isinstance(budget, int):
        value = budget
    else:
        def evaluate(node):
            if isinstance(node, ast.Expression):
                return evaluate(node.body)
            if isinstance(node, ast.Name) and node.id in ('cores', 'all'):  # 'all' as in `snakemake -c all`
                return cores
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
                return node.value
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
                a, b = evaluate(node.left), evaluate(node.right)
                return {ast.Add: a + b, ast.Sub: a - b, ast.Mult: a * b,
                        ast.Div: a / b if b else 0}[type(node.op)]
            raise ValueError
        try:
            value = evaluate(ast.parse(budget.strip(), mode='eval'))
        except (SyntaxError, ValueError):
            raise ValueError(f"{owner}.threads_budget = {budget!r} is not a valid expression; use "
                             f"'cores' (or 'all') with + - * / and numbers, e.g. 'cores/2' or 'cores-2'") from None
    return max(1, min(int(value), cores))


def _template_wildcards(template):
    """Names of the wildcards in a template, in order of appearance, without `{rule}`."""
    names = re.findall(r'\{(\w+)\}', template or '')
    return list(dict.fromkeys(n for n in names if n != 'rule'))


def _check_template(cls_name, attr, template):
    if not isinstance(template, str):
        raise TypeError(f"{cls_name}.{attr} must be a string, got {type(template).__name__}")
    if re.search(r'\{\w+\}\{\w+\}', template):
        raise TypeError(f"{cls_name}.{attr} = {template!r}: two wildcards without anything between them "
                        f"would make names ambiguous; separate them, e.g. with '_' or '/'")
    if '*' in template[:-1]:
        raise TypeError(f"{cls_name}.{attr} = {template!r}: '*' can only be at the end (a file name prefix)")
    for name in re.findall(r'\{([^}]*)\}', template):
        if not re.fullmatch(r'\w+', name):
            raise TypeError(f"{cls_name}.{attr} = {template!r}: {{{name}}} is not a valid wildcard name")


def _with_rule(template):
    """A result template with the rule name in it: at `{rule}`, or else as the last part."""
    if '{rule}' in template:
        return template
    if template.endswith('*'):
        prefix = template[:-1]
        sep = '' if not prefix or prefix[-1] in '_-./' else '_'
        return prefix + sep + '{rule}_*'
    folder = template.rstrip('/')
    return (folder + '/' if folder else '') + '{rule}/'


def _log_name_template(result_template, log_template=None):
    """The log file name pattern (without folder and extension), with `{rule}`."""
    if log_template is None:
        # The parts of the path, joined with '_'. A part whose wildcards all appear again further on
        # adds nothing and is left out, so that a BIDS-style template such as
        # 'sub-{subject}/ses-{session}/sub-{subject}_ses-{session}_*' gives 'sub-01_ses-1_<rule>'.
        parts = _with_rule(result_template).rstrip('*').rstrip('/').split('/')
        kept = []
        for i, part in enumerate(parts):
            later = set().union(*(_template_wildcards(p) for p in parts[i + 1:]))
            if i < len(parts) - 1 and '{rule}' not in part and set(_template_wildcards(part)) <= later:
                continue
            kept.append(part)
        return '_'.join(kept).rstrip('_-.')
    name = log_template[:-4] if log_template.endswith('.log') else log_template
    if '{rule}' not in name:
        name += ('' if not name or name[-1] in '_-./' else '_') + '{rule}'
    return name


def _template_from_wildcard_model(model):
    """The result_template that gives the same names as an old-style WildcardModel."""
    if isinstance(model, type) and issubclass(model, BaseModel):
        # a pydantic model keeps its fields (and their defaults) in model_fields
        defaults = {name: field.default for name, field in model.model_fields.items()}
    else:
        defaults = {}
        for name in getattr(model, '__annotations__', {}):
            default = getattr(model, name, None)
            defaults[name] = getattr(default, 'default', default)
    return '/'.join((d if isinstance(d, str) else '{}').format('{' + name + '}') for name, d in defaults.items())


def _snakemake_names():
    """Names that Snakemake's own module `snakemake.workflow` defines. A Snakefile runs in that
    module's namespace, so a variable with such a name replaces what Snakemake itself uses."""
    global _SNAKEMAKE_NAMES
    if _SNAKEMAKE_NAMES is None:
        import ast
        names = set()
        try:
            import importlib.util
            module = sys.modules.get('snakemake.workflow')
            path = module.__file__ if module else importlib.util.find_spec('snakemake.workflow').origin
            tree = ast.parse(open(path).read())
            for node in tree.body:
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names.update((a.asname or a.name).split('.')[0] for a in node.names)
                elif isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)):
                    names.add(node.name)
                elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    names.update(t.id for t in targets if isinstance(t, ast.Name))
        except Exception:
            pass
        _SNAKEMAKE_NAMES = names
    return _SNAKEMAKE_NAMES

_SNAKEMAKE_NAMES = None


def _check_rule_names(names):
    """Rule names are variables in the Snakefile, which runs in Snakemake's own namespace: a name
    that hides a Python built-in or a name of Snakemake can make Snakemake itself fail."""
    import builtins
    problems = []
    for name in names:
        if hasattr(builtins, name):
            problems.append(f"'{name}' would hide Python's built-in {name}()")
        elif name in _snakemake_names():
            problems.append(f"'{name}' would replace Snakemake's own '{name}'")
    if problems:
        raise NameError("These rule names cannot be used, because the Snakefile runs in Snakemake's "
                        "own namespace:\n  " + '\n  '.join(problems) +
                        "\nChoose other names, e.g. with a suffix: 'filter_tracts' instead of 'filter'.")


def _as_dict(wildcards):
    if isinstance(wildcards, dict):
        return wildcards
    return dict(wildcards.items()) if hasattr(wildcards, 'items') else dict(wildcards or {})


def _dynamic(val, wildcards):
    """An input as it is for one job: for an input function, its result, where a rule or loop
    becomes its output promise. An input function may thus choose a connection per job, e.g.
    `tck=lambda wildcards: tracking.get_output(f'DRT_{wildcards.hemi}')`."""
    if _is_input_function(val):
        val = val(wildcards)
        if isinstance(val, (SnakeRule, JobLoop)):
            val = val.get_output()
    return val


class _Wildcards(dict):
    """Wildcards as a dict that also allows attribute access (`wildcards.subject`), like Snakemake's."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


def _promise_stamps(promise, wildcards):
    """The stamp file(s) Snakemake waits for, for an output promise of a rule or loop."""
    source = promise.rule_or_loop
    if isinstance(source, JobLoop):
        return source._input_stamps(wildcards)
    return source._stamp_path(source._own_wildcards(wildcards))


def _is_input_function(val):
    return callable(val) and not isinstance(val, (SnakeRule, JobLoop, OutputPromise, type))


class SnakeRule:
    """A step of the pipeline. It runs once for every combination of wildcard values: a job.

    `result_template` gives the wildcards, and where each job writes its results, relative to the
    results folder: `'sub-{subject}/ses-{session}'` is a folder per job; a template that ends with
    `*`, such as `'sub-{subject}/{hemi}_*'`, is a prefix for the job's file names. The rule name is
    added as the last part, unless the template has `{rule}`. The log file name is the same
    template with `/` replaced by `_`, in the logs folder, unless `log_template` says otherwise.

    Describe the step further with three nested models, each optional:

    - `InputModel`: the inputs and their types. They are connected with `set_input()`.
    - `OutputModel`: the outputs and their default file names, relative to the job's result folder,
      e.g. `mask: Path = Field('mask.nii.gz')`.
    - `ParamModel`: the parameters, with their defaults.

    A model declared without base class becomes a `Fixed` model. Then implement `run()`.

    Example:
        ```python
        class Denoise(SnakeRule):
            result_template = 'sub-{subject}'

            class InputModel:
                dwi: Path
            class OutputModel:
                denoised: Path = Field('dwi_denoised.mif')
            class ParamModel:
                extent: int = 5

            def run(self, job, input, output, params, wildcards):
                job.run(['dwidenoise', '-extent', str(params.extent), input.dwi, output.denoised])
        ```

    Attributes:
        result_template: Where each job writes, relative to the results folder; its wildcards
            are the wildcards of the rule. Default: `''`, a rule without wildcards.
        log_template: Name of each job's log file, relative to the logs folder, with the same
            wildcards. Default: `result_template` with `/` replaced by `_`.
        name: Name of the rule, set by `build()` to the variable name in the Snakefile.
        description: Shown in the generated pipeline documentation; by default the class docstring.
        threads_budget: At most how many threads a job of this rule uses (default 1): a number, or
            an expression of the cores that Snakemake was given (`-c`), such as `'cores'` (or
            `'all'`), `'cores/2'` or `'cores-2'`. A job gets at most `-c` threads; the number it got is
            `job.threads`, to pass on to the tool. Snakemake runs jobs side by side as long as
            their threads fit in `-c`.
        conda_env: Conda environment for the jobs (with Snakemake's `--use-conda`).
        tmpdir_autodelete: If False, `job.tmpdir()` is kept when the job ends, and its location
            is written to the log. Can be set per instance: `myrule.tmpdir_autodelete = False`.
    """
    name = "[no name]" # name of this rule, may vary per instance
    description = "[no description]" # description of what this rule does

    class InputModel(Fixed): # describes expected inputs
        pass

    class OutputModel(Fixed): # describes outputs + default file names
        pass

    class ParamModel(Fixed): # describes parameters + defaults
        pass


    result_template = ''  # e.g. 'sub-{subject}/ses-{session}' (folders) or 'sub-{subject}/{hemi}_*' (prefix)
    log_template = None   # default: result_template with '/' replaced by '_'
    threads_budget = 1  # a number, or e.g. 'cores' (or 'all'), 'cores/2', 'cores-2'
    conda_env = None
    tmpdir_autodelete = True  # False: keep the job's temporary folder (job.tmpdir()), e.g. for debugging
    default_target = False
    is_checkpoint = False


    def __init_subclass__(cls, **kwargs):
        # A model declared without base class, e.g. `class InputModel:`, becomes a Fixed model.
        super().__init_subclass__(**kwargs)
        if 'WildcardModel' in cls.__dict__:
            hint = _template_from_wildcard_model(cls.__dict__['WildcardModel'])
            raise TypeError(f"{cls.__name__}: WildcardModel is no longer supported. Use instead:\n"
                            f"    result_template = {hint!r}\n"
                            f"which gives the same names for log files and result folders as before.")
        if 'threads' in cls.__dict__:
            raise TypeError(f"{cls.__name__}: 'threads' is now called 'threads_budget' (at most this many "
                            f"threads per job); the number a job got is job.threads. Use instead:\n"
                            f"    threads_budget = {cls.__dict__['threads']!r}")
        _threads_for(cls.threads_budget, 8, cls.__name__)  # checks the expression
        _check_template(cls.__name__, 'result_template', cls.result_template)
        if cls.log_template is not None:
            _check_template(cls.__name__, 'log_template', cls.log_template)
            if set(_template_wildcards(cls.log_template)) != set(_template_wildcards(cls.result_template)):
                raise TypeError(f"{cls.__name__}: log_template {cls.log_template!r} must have the same "
                                f"wildcards as result_template {cls.result_template!r}")
        for name in ('InputModel', 'OutputModel', 'ParamModel'):
            model = cls.__dict__.get(name)
            if model is None or (isinstance(model, type) and issubclass(model, BaseModel)):
                continue
            if not isinstance(model, type) or model.__bases__ != (object,):
                raise TypeError(f"{cls.__name__}.{name} must be a class without base class, or derive "
                                f"from Fixed or Extensible")
            attrs = {k: v for k, v in vars(model).items() if k not in ('__dict__', '__weakref__')}
            setattr(cls, name, type(name, (Fixed,), attrs))


    @classmethod
    def _wildcard_names(cls):
        return _template_wildcards(cls.result_template)


    def _own_wildcards(self, wildcards):
        """This rule's wildcards out of a larger set, e.g. those of a downstream job."""
        wildcards = _as_dict(wildcards)
        return {k: wildcards[k] for k in self._wildcard_names() if k in wildcards}


    def __init__(self, params=None, **kwargs):
        """Create a rule; its parameters are given as keyword arguments, e.g. `N4(modality='T1w')`.

        Alternatively, they can be given as a dict: `N4(params=dict(modality='T1w'))`. The two forms
        cannot be mixed. Parameter names and values are checked against `ParamModel` here, so that
        a mistake is reported at this line of the Snakefile. Parameters not given here get their
        value from the config (see `configure()`), or the default in `ParamModel`.
        """
        if kwargs and params is not None:
            raise TypeError(f"{type(self).__name__}: give parameters either as keyword arguments "
                            f"or as params=dict(...), not both")
        if params is not None and not isinstance(params, dict):
            raise TypeError(f"{type(self).__name__}: params must be a dict, got {type(params).__name__}")
        self.params = dict(kwargs) if kwargs else dict(params or {})
        self._check_params()
        self.inputs = {}


    def _check_params(self):
        from pydantic import ValidationError
        fields = self.ParamModel.model_fields
        extra = self.ParamModel.model_config.get('extra', 'ignore')
        unknown = set(self.params) - set(fields)
        if unknown and extra == 'forbid':
            raise TypeError(f"{type(self).__name__}: unknown parameter(s) {sorted(unknown)}; "
                            f"{type(self).__name__}.ParamModel has {sorted(fields) or 'no parameters'}")
        for key, value in self.params.items():
            if key in fields and not callable(value):
                try:
                    TypeAdapter(fields[key].annotation).validate_python(value)
                except ValidationError as e:
                    raise ValueError(f"{type(self).__name__}: invalid value for parameter {key!r}.\n{e}") from None


    def set_input(self, **inputs):
        """Connect the inputs of this rule, one keyword per field of `InputModel`. Returns the rule
        itself, so that it can directly follow the constructor.

        A value can be:

        - an output of another rule: `other.get_output('mask')`, or a rule itself for its whole
          `JobResult`;
        - the outputs of a loop over another rule: `other.foreach(...).get_output('mask')`, a list;
        - a file name, which may contain this rule's wildcards: `'data/sub-{subject}/T1w.nii.gz'`;
        - an input function `f(wildcards)` that returns file names, or a connection as above.

        Types are checked here, so a mismatch is reported when the Snakefile is loaded. Inputs from
        an input function can only be checked when the job runs.

        Example:
            ```python
            denoise = Denoise().set_input(dwi='data/sub-{subject}/dwi.mif')
            fit = FitTensor().set_input(dwi=denoise.get_output('denoised'))
            ```
        """
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
                unknown = value.key_wildcards - set(self._wildcard_names())
                if unknown:
                    raise TypeError(f"{type(self).__name__}.{key}: get_output({value.key!r}) uses "
                                    f"{', '.join('{' + w + '}' for w in sorted(unknown))}, which is not a "
                                    f"wildcard of {type(self).__name__}")
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


    def get_output(self, key=None, parser=None):
        """A connection to an output of this rule, to pass to `set_input()` of another rule.

        Args:
            key: Name of a field of `OutputModel`. It can also be a pattern like `'DRT_{hemi}'`,
                which picks the output per job, using the wildcards of the receiving rule. Without
                key, the receiving rule gets the whole `JobResult`.
            parser: Function that converts the output before it is passed on, e.g. reads a
                number from the file. Its type hints are used for type checking. If it has a
                second argument, it gets the wildcards.
        """
        if key is not None:
            _validate_key(self.OutputModel, key, type(self).__name__)

        # return specific output
        return OutputPromise(self, key, parser)


    def foreach(self, *wildcard_list_of_dicts, **wildcard_dict_of_lists):
        """A loop over jobs of this rule, to connect all their outputs at once.

        Give the wildcard values as lists, `rule.foreach(subject=['01', '02'])`, or as a function
        that yields one dict per job, `rule.foreach(all_subjects)`; such a function gets the
        wildcards of the receiving job, and may read the result of a checkpoint. Wildcards of the
        receiving job that the loop does not set are passed on. Use `.get_output()` on the loop.

        Example:
            ```python
            group = GroupStats().set_input(fa=fit.foreach(all_subjects).get_output('fa'))
            ```
        """
        return JobLoop(self, *wildcard_list_of_dicts, **wildcard_dict_of_lists)


    def run(self, job, input, output, params, wildcards):
        """Do the work of one job; implement this in a subclass.

        Everything that goes wrong in here is reported in the job's log file, which then gets the
        extension `.error`; Snakemake itself continues with the other jobs.

        Args:
            job (JobMonitor): Runs commands with their output in the log (`job.run`, `job.shell`),
                writes to the log (`job.log`), and gives a temporary folder (`job.tmpdir()`).
                `job.threads` is the number of threads this job got (see `threads_budget`).
            input: The inputs, as an instance of `InputModel`.
            output (JobResult): `output.name` is the full path of output file `name`, in the
                job's result folder; `output(name=value)` declares an output value.
            params: The parameters, as an instance of `ParamModel`.
            wildcards: The wildcards of this job, e.g. `wildcards.subject`.
        """
        raise NotImplementedError


    def log_path(self, wildcards=None):
        """The `.log` name of the log file of a job, or its pattern if no wildcards are given."""
        fmt = self._log_template()
        return fmt if wildcards is None else partial_formatter.format(fmt, **wildcards)


    def result_path(self, wildcards=None):
        """The result folder of a job, or its pattern if no wildcards are given."""
        fmt = self._result_template()
        return fmt if wildcards is None else fmt.format(**dict(wildcards))


    @classmethod
    def describe(cls):
        """Print the wildcards, inputs, outputs and parameters of this rule."""
        print(f"{cls.__name__}: {cls.description}")
        print(f"{cls.__name__} wildcards: {', '.join(cls._wildcard_names()) or '(none)'} "
              f"(result_template {cls.result_template!r})")
        for title, model in [('inputs', cls.InputModel),
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
        self._validate_param_values()

        # Complete parameter set including defaults. Snakemake stores it with the job's
        # log file and reruns the job when it changes (its `params` rerun trigger).
        self._snake_params = {
            key: field.get_default(call_default_factory=True)
            for key, field in self.ParamModel.model_fields.items()
            if not field.is_required()
        }
        self._snake_params.update(params)


    def _validate_param_values(self):
        """Check parameter values when the Snakefile is loaded, so that mistakes are reported in the
        console instead of in every job's log. Parameters given as functions (evaluated by Snakemake
        per job) can only be checked when the job runs."""
        from pydantic import ValidationError
        fields = self.ParamModel.model_fields
        if not any(callable(v) for v in self.params.values()):
            try:
                self.ParamModel(**self.params)
            except ValidationError as e:
                raise ValueError(f"Rule {self.name} ({type(self).__name__}): invalid parameters.\n{e}") from None
            return
        for key, value in self.params.items():
            if key in fields and not callable(value):
                try:
                    TypeAdapter(fields[key].annotation).validate_python(value)
                except ValidationError as e:
                    raise ValueError(f"Rule {self.name} ({type(self).__name__}): invalid parameter {key!r}.\n{e}") from None


    # Snakemake will ensure that _run_job is called after all input log files have been created.
    # Everything that can go wrong here ends up in the .log and .error files, not in Snakemake.
    def _run_job(self, rule_context):
        wildcards = rule_context['wildcards']
        raw_params = rule_context['params'] # params passed as functions are resolved in rule_context
        stamp = rule_context['output'][0]  # what Snakemake tracks
        log_path = self.log_path(_as_dict(wildcards))

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
                if isinstance(field.default, str) and _path_annotation(field.annotation)[0]
            }
        except Exception as e:
            descr, result_path, output_patterns = self.name, None, None
            setup_error = setup_error or e

        previous = previous_run(log_path)  # read before JobMonitor replaces the job's file

        with JobMonitor(log_path, descr, result_path, output_patterns=output_patterns,
                        shell_context=rule_context, linemaps=_get_builder().linemaps,
                        params=params_dict, stamp_file=stamp, tmpdir_autodelete=self.tmpdir_autodelete,
                        threads=rule_context.get('threads', 1)) as job:
            if setup_error:
                raise setup_error
            job_inputs = {key: _dynamic(val, wildcards) for key, val in self.inputs.items()}
            if previous:
                self._remove_previous_outputs(job, previous, wildcards, job_inputs)
            failed = self._check_failed_inputs(job, wildcards, job_inputs)
            inputs = self._validated_inputs(self._resolved_inputs(wildcards, failed, job_inputs))
            self.run(job, inputs, job.result, params, wildcards)
            if not job.result._errors:
                self._check_outputs(job)


    def _check_outputs(self, job):
        """After run(): every path-typed field of OutputModel must exist, and be listed in the log.

        - a required output that exists but was not referenced in run() is added to the log;
        - a missing required output makes the job fail;
        - an optional output (annotated `Path | None`) that is missing is left out of the log,
          so that it resolves to None downstream.
        """
        from pydantic import ValidationError
        result = job.result
        missing, invalid = [], []
        for key, field in self.OutputModel.model_fields.items():
            is_path, optional = _path_annotation(field.annotation)
            if not is_path:
                # other outputs are declared with output(name=value), unless they have a default
                if key in result._named_outputs:
                    try:
                        TypeAdapter(field.annotation).validate_python(result._named_outputs[key])
                    except ValidationError as e:
                        invalid.append(f"  {key}: {e.errors()[0]['msg']} (got {result._named_outputs[key]!r})")
                elif field.is_required():
                    missing.append(f"  {key}: not declared (use output({key}=...) in run)")
                continue
            referenced = key in result._named_outputs  # run() used or declared this output
            value = result._named_outputs.get(key, result._output_patterns.get(key))
            if value is None or isinstance(value, (list, tuple)):
                continue  # no file name known, or a value that is not a single path
            path = result._resolve_value(value)
            if _is_prefix_annotation(field.annotation):
                # the prefix is not a file itself: files starting with it must exist
                found = [m for m in _prefix_matches(path) if referenced or _created_since(m, job.started)]
                if found:
                    result._named_outputs[key] = value
                    names = [op.relpath(m, op.dirname(path) or '.') for m in found]
                    shown = ', '.join(names[:10]) + (f', ... ({len(names)} in total)' if len(names) > 10 else '')
                    job.log(f"Output '{key}': {shown}")
                elif optional:
                    result._named_outputs.pop(key, None)
                else:
                    missing.append(f"  {key}: {path}* (no files with this prefix were created)")
                continue
            if op.exists(path) and (referenced or _created_since(path, job.started)):
                result._named_outputs[key] = value
            elif optional:
                if result._named_outputs.pop(key, None) is not None:
                    job.log(f"Optional output '{key}' was not created: {path}")
            elif op.exists(path):
                missing.append(f"  {key}: {path} (exists, but was not created by this run)")
            else:
                missing.append(f"  {key}: {path}")
        problems = []
        if missing:
            problems.append(f'these required outputs of {type(self).__name__} are missing:\n'
                            + '\n'.join(missing) +
                            '\n(a file output is optional if annotated `Path | None`; '
                            'another output is optional if it has a default value)')
        if invalid:
            problems.append(f'these outputs of {type(self).__name__} have a wrong type:\n' + '\n'.join(invalid))
        if problems:
            raise JobError('\n'.join(problems))


    def _remove_previous_outputs(self, job, previous, wildcards, job_inputs):
        """Remove the outputs of the previous run of this job, as listed in its log, so that the
        new run starts clean. To protect files that this job did not make, a path is removed only if
        - the previous run used the same result folder, and the path is strictly inside it;
        - it was modified after the previous run started (a folder: everything in it);
        - it is not an input of this job, and does not contain an input or the log folder.
        Symbolic links are removed as links, never followed."""
        started, prefix, mapping = previous
        if prefix != job.result._prefix:
            return
        folder = op.realpath(prefix if prefix.endswith(op.sep) else op.dirname(prefix) or '.')
        since = started.timestamp()

        def full(value, empty_ok=False):
            if isinstance(value, (list, tuple)):
                value = op.join(*map(str, value))
            return prefix + value if isinstance(value, str) and (value or empty_ok) else None
        paths = [full(v) for v in mapping.get('by_number', [])]
        for key, value in mapping.get('by_name', {}).items():
            field = self.OutputModel.model_fields.get(key)
            if field is not None and _is_prefix_annotation(field.annotation):
                # an empty prefix name stands for all files of the job
                paths += _prefix_matches(full(value, empty_ok=True)) if full(value, empty_ok=True) else []
            elif field is None or _path_annotation(field.annotation)[0]:
                paths.append(full(value))
        protected = [op.realpath(p) for p in self._input_files(wildcards, job_inputs)]
        protected += [op.realpath(op.dirname(job.log_file) or '.'), op.realpath(os.getcwd())]

        removed, kept = [], []
        for path in paths:
            if path is None:
                continue
            # the real location of the path itself (a symbolic link is not followed)
            real = op.normpath(op.join(op.realpath(op.dirname(path) or '.'), op.basename(path.rstrip(op.sep))))
            try:
                if not op.lexists(real) or real == folder or op.commonpath([folder, real]) != folder:
                    continue
                if any(op.commonpath([real, p]) == real for p in protected):
                    kept.append(real)
                elif op.isdir(real) and not op.islink(real):
                    if _created_since(real, since, recursive=True):
                        shutil.rmtree(real)
                        removed.append(real + op.sep)
                    else:
                        kept.append(real + op.sep)
                elif _created_since(real, since):
                    os.unlink(real)
                    removed.append(real)
                else:
                    kept.append(real)
            except ValueError:
                continue  # paths on different drives

        # short names: relative to the result folder
        rel = lambda p: op.relpath(p, folder) + (op.sep if p.endswith(op.sep) else '')
        if removed:
            job.log(f"Removed outputs of the previous run: {', '.join(rel(p) for p in removed)}")
        if kept:
            job.log(f"Kept outputs of the previous run that it may not have created: {', '.join(rel(p) for p in kept)}")


    def _input_files(self, wildcards, job_inputs):
        """Paths that inputs of this job may refer to, for protection against removal. Best effort:
        file names given directly, and all outputs listed in the logs of upstream jobs."""
        paths = []
        def add(v):
            if isinstance(v, (str, os.PathLike)):
                paths.append(str(v))
            elif isinstance(v, (list, tuple)):
                for x in v:
                    add(x)
        for key, val in job_inputs.items():
            try:
                if isinstance(val, str):
                    add(partial_formatter.format(val, **wildcards))
                elif isinstance(val, (list, tuple)):
                    add([partial_formatter.format(v, **wildcards) for v in val])
                elif isinstance(val, OutputPromise):
                    logs = val.rule_or_loop.log_path(wildcards)
                    for log in (logs if isinstance(logs, list) else [logs]):
                        result = JobResult(log)
                        for v in list(result._named_outputs.values()) + result._numbered_outputs:
                            if isinstance(v, (str, list, tuple)):
                                add(result._resolve_value(v))
                else:
                    add(val)
            except Exception as e:
                if _is_incomplete_checkpoint(e):
                    raise
        return paths


    def _check_failed_inputs(self, job, wildcards, job_inputs):
        """Find upstream jobs that failed. Fail this job, unless the InputModel type of each affected
        input admits JobError, e.g. `list[Path | JobError]`. Returns {log path: JobError}."""
        failed, blocking = {}, []
        fields = self.InputModel.model_fields
        for key, val in job_inputs.items():
            if not isinstance(val, OutputPromise):
                continue
            logs = val.rule_or_loop.log_path(wildcards)  # a failed loop iterator raises here
            logs = logs if isinstance(logs, list) else [logs]
            errors = [(log, JobError._from_log(log)) for log in logs]
            errors = [(log, err) for log, err in errors if err is not None]
            if not errors:
                continue
            if key in fields and _admits_job_error(fields[key].annotation):
                failed.update(errors)
                job.log(f"Input '{key}': {len(errors)} upstream job(s) failed, passed on as JobError.")
            else:
                blocking.extend((log, err, key) for log, err in errors)
        if blocking:
            raise UpstreamFailedError(format_upstream_failures(job.job_name, blocking))
        return failed


    def _resolve(self, wildcards, key, parser, failed={}):
        log = self.log_path(wildcards)
        if log in failed:
            return failed[log]  # parsers are not applied to a failed job's output
        val = self._read_output(JobResult(log), key, wildcards)
        return _apply_parser(parser, val, wildcards)


    def _read_output(self, result, key, wildcards):
        """Output `key` of a finished job: a full path for path-typed fields, the plain value for
        other fields (or the OutputModel default if the job did not declare it).

        A required file output that is not listed in the job's log (e.g. a log written by an older
        version of snakeplusplus) is found by its default file name; if that does not exist either,
        the job that wants it fails with a clear message. An optional one resolves to None."""
        if key is None:
            return result
        field = self.OutputModel.model_fields.get(key)
        if field is None:
            return _output_value(result, key)
        is_path, optional = _path_annotation(field.annotation)
        if is_path:
            value = _output_value(result, key)
            if value is not None or optional:
                return value
            expected = None
            if isinstance(field.default, str):
                try:
                    name = partial_formatter.format(field.default, params=SimpleNamespace(**self._snake_params),
                                                    **dict(wildcards))
                    expected = result._resolve_value(name)
                except Exception:
                    pass
            if expected and (_prefix_matches(expected) if _is_prefix_annotation(field.annotation) else op.exists(expected)):
                return expected
            raise JobError(f"{type(self).__name__} did not produce its output {key!r}"
                           + (f" (expected {expected}" if expected else " (") + f"; log-file: {result._log_file})")
        if key in result._named_outputs:
            return result._named_outputs[key]
        return None if field.is_required() else field.get_default(call_default_factory=True)


    def _resolved_inputs(self, wildcards, failed, job_inputs):
        inp = {}
        for key, val in job_inputs.items():
            if _is_input_function(self.inputs.get(key)) and not isinstance(val, OutputPromise):
                inp[key] = val  # result of an input function, used as it is
            elif isinstance(val, str):
                # simple filename input
                inp[key] = partial_formatter.format(val, **wildcards)
            elif isinstance(val, (list, tuple)):
                inp[key] = [partial_formatter.format(v, **wildcards) for v in val]
            elif isinstance(val, OutputPromise):
                inp[key] = val.rule_or_loop._resolve(wildcards, val._key_for(wildcards), val.parser, failed)
            else:
                inp[key] = val
        return inp


    def _validated_inputs(self, inputs):
        """The inputs of a job as an InputModel instance; a type mismatch fails the job."""
        from pydantic import ValidationError
        try:
            return self.InputModel(**inputs)
        except ValidationError as e:
            lines = []
            for err in e.errors():
                loc = '.'.join(str(l) for l in err['loc'])
                got = repr(err.get('input'))
                got = got if len(got) <= 100 else got[:97] + '...'
                lines.append(f"  {loc}: {err['msg']} (got {got})")
            raise JobError(f'these inputs of {type(self).__name__} have a wrong type:\n' + '\n'.join(lines)) from None


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
                paths.append(snake_checkpoint_magic(lambda wildcards, p=val: _promise_stamps(p, wildcards)))
            elif _is_input_function(val):
                # input function that returns paths, or a connection to another rule
                def paths_of(wildcards, fn=val):
                    v = _dynamic(fn, wildcards)
                    return _promise_stamps(v, wildcards) if isinstance(v, OutputPromise) else v
                paths.append(snake_checkpoint_magic(paths_of))

        return paths


    def _log_template(self):
        name = _log_name_template(self.result_template, self.log_template).replace('{rule}', self.name)
        return op.join(_get_builder().pathvars.get('logs'), name + '.log')


    def _stamp_path(self, wildcards=None):
        """The file that Snakemake tracks for a job (or its pattern): one folder level per wildcard
        under the rule name, so that Snakemake can never confuse rules or split values wrongly."""
        parts = [self.name]
        for w in self._wildcard_names():
            if wildcards is None:
                parts.append('{' + w + '}')
            else:
                value = str(wildcards[w])
                if not value or '/' in value:
                    raise ValueError(f"rule '{self.name}': wildcard {w}={value!r} is not allowed; "
                                     f"a wildcard value must not be empty or contain '/'")
                parts.append(value)
        return op.join(os.getcwd(), STAMP_DIR, *parts, 'job')


    def _existing_jobs(self, wildcards=None):
        """Wildcards of this rule's jobs that have a job file, and that agree with `wildcards`.
        Only files matching this rule's own log file pattern are looked at."""
        template = self._log_template()
        names = re.findall(r'\{(\w+)\}', template)
        regex, pos, seen = '', 0, set()
        for m in re.finditer(r'\{(\w+)\}', template):
            name = m.group(1)
            regex += re.escape(template[pos:m.start()]) + (f'(?P={name})' if name in seen else f'(?P<{name}>[^/]+?)')
            seen.add(name)
            pos = m.end()
        regex = re.compile(regex + re.escape(template[pos:]) + '$')
        pattern = re.sub(r'\{\w+\}', '*', template)
        found = []
        for state in JOB_STATES:
            for f in glob.glob(replace_inner_extension(pattern, state)):
                m = regex.match(replace_inner_extension(f, '.log'))
                if m and all(m[k] == wildcards[k] for k in names if wildcards and k in wildcards):
                    if m.groupdict() not in found:
                        found.append(m.groupdict())
        return found


    def _result_template(self):
        return op.join(_get_builder().pathvars.get('results'), _with_rule(self.result_template).replace('{rule}', self.name))


    def _as_snake(self, snake_checkpoint_magic):
        # The rule's only output is the job's stamp file; its inputs are the stamps of upstream jobs.
        return SimpleNamespace(
            name=self.name,
            input=self._input_paths(snake_checkpoint_magic),
            params=self._snake_params,
            output=self._stamp_path(),
            wildcard_constraints={w: '[^/]+' for w in self._wildcard_names()},
            threads=_threads_for(self.threads_budget, _get_builder().cores, type(self).__name__),
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
        # a key pattern like 'DRT_{hemi}' picks the output per job, from the wildcards of the receiving job
        self.key_wildcards = set(re.findall(r'\{(\w+)\}', key)) if key else set()
        self._annotation = self._compute_annotation()  # computes the expected output type of this promise

    def _compute_annotation(self):
        is_loop = isinstance(self.rule_or_loop, JobLoop)

        if self.key is None:
            # No specific field: the promise resolves to the whole JobResult,
            # not to a value described by rule.OutputModel.
            base = JobResult
        else:
            fields = self._rule().OutputModel.model_fields
            if self.key_wildcards:
                # any of the matching outputs, e.g. DRT_L or DRT_R
                types = list(dict.fromkeys(fields[k].annotation for k in _key_matches(self._rule().OutputModel, self.key)))
                base = types[0] if len(types) == 1 else typing.Union[tuple(types)]
            else:
                base = fields[self.key].annotation

        if is_loop and not self.rule_or_loop.scalar:
            base = typing.List[base]

        if self.parser is None:
            return base

        parser_in, parser_out = _get_parser_types(self.parser)

        if not is_assignable(base, parser_in):
            raise TypeError(
                f"Parser {self.parser!r} expects input {parser_in!r}, but "
                f"{type(self.rule_or_loop).__name__}.{self.key} produces {base!r}"
            )
        return parser_out

    @property
    def annotation(self):
        return self._annotation

    def _rule(self):
        return self.rule_or_loop.rule if isinstance(self.rule_or_loop, JobLoop) else self.rule_or_loop

    def _key_for(self, wildcards):
        """The output name for one job: a key pattern filled in with the wildcards of that job."""
        if not self.key_wildcards:
            return self.key
        key = self.key.format(**{w: wildcards[w] for w in self.key_wildcards})
        if key not in self._rule().OutputModel.model_fields:
            raise JobError(f"{type(self._rule()).__name__} has no output {key!r} "
                           f"(from get_output({self.key!r}) with these wildcards)")
        return key


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
            _validate_key(self.rule.OutputModel, key, type(self.rule).__name__)
        return OutputPromise(self, key, parser)


    def _known_wildcards(self):
        """Names of the wildcards this loop sets, or None if not known before running (an iterator function)."""
        names = set()
        for it in self.wildcard_iterables:
            if callable(it) and getattr(it, '__name__', '') != 'dict_of_lists_iterator':
                return None
            first = next(iter(it({}) if callable(it) else it), None)
            names |= set(first or {})
        return names


    def _wildcard_iterator(self, parent_wildcards):
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
            members = list(self._wildcard_iterator(parent_wildcards))
            for wc in members:
                for k in self.rule._wildcard_names():
                    if k in wc and (not str(wc[k]) or '/' in str(wc[k])):
                        raise JobError(f"wildcard {k}={wc[k]!r} is not allowed: a wildcard value must "
                                       f"not be empty or contain '/'")
            return members
        except Exception as e:
            if _is_incomplete_checkpoint(e):
                raise
            if isinstance(e, JobError):
                detail = str(e)
            else:
                linemaps = _builder.linemaps if _builder else None
                detail = "".join(format_exception_remapped(type(e), e, e.__traceback__, linemaps)).rstrip()
            raise LoopError(
                f"Wildcard iterator of loop over rule '{self.rule.name}' failed:\n{detail}"
            ) from e


    def _resolve(self, wildcards, key, parser, failed={}):
        # a failed member's output is its JobError; a parser of the loop gets those as well
        val = []
        for wc in self.members(wildcards):
            log = self.rule.log_path(wc)
            val.append(failed[log] if log in failed else self.rule._read_output(JobResult(log), key, wc))
        val = val[0] if self.scalar else val
        return _apply_parser(parser, val, wildcards)


    # The log_path of a loop contains a list with all log_paths of its members.
    # _wildcard_iterator may depend on a checkpoint.
    def log_path(self,parent_wildcards=None):
        val = [ self.rule.log_path(wc) for wc in self.members(parent_wildcards) ]
        return val[0] if self.scalar else val


    # Input function for Snakemake. If the iterator fails before any job has started (while
    # Snakemake builds its job list, or in a dry run), the error stops Snakemake with a message
    # in the console. If it fails while jobs are running (typically after a checkpoint produced
    # a bad result), the loop contributes no inputs and the error is reported in the .error file
    # of the job that uses the loop, so that the other jobs continue.
    def _input_stamps(self, parent_wildcards):
        try:
            members = self.members(parent_wildcards)
            stamps = [self.rule._stamp_path(self.rule._own_wildcards(wc)) for wc in members]
            return stamps[0] if self.scalar else stamps
        except LoopError as e:
            builder = _get_builder()
            # a job's own process (not the main one) only exists once execution has started
            if builder.is_main_process and not builder.execution_started:
                raise
            if str(e) not in _reported_loop_errors and builder.is_main_process:
                _reported_loop_errors.add(str(e))
                print(f"Warning: {e}\n=> jobs are already running, so this error is reported in the "
                      f".error file of the job that uses this loop.", file=sys.stderr)
            return []


_reported_loop_errors = set()  # print each deferred loop error only once


class SnakeCheckpoint(SnakeRule):
    """A rule whose result determines which jobs come next, e.g. the list of subjects to process.

    A loop reads it in its iterator function with `JobResult.from_checkpoint()`:

    Example:
        ```python
        def all_subjects(wildcards):
            result = JobResult.from_checkpoint(checkpoints.find_subjects, wildcards)
            for subject in json.load(open(result.subjects)):
                yield dict(subject=subject)
        ```
    """
    is_checkpoint = True


class TargetRule(SnakeRule):
    class InputModel(Extensible):
        pass

    default_target = True
    def run(self,job,input,output,params,wildcards):
        print('All jobs done.')


def _undefined_names(func):
    """(name, line) of global names used in `func` (and functions nested in it) that are neither
    defined in its module nor builtins, e.g. a function that was never imported."""
    import builtins, dis, types
    available = set(func.__globals__) | set(dir(builtins))
    found = []

    def scan(code):
        assigned = {i.argval for i in dis.get_instructions(code) if i.opname == 'STORE_GLOBAL'}
        line = code.co_firstlineno
        for i in dis.get_instructions(code):
            if i.positions and i.positions.lineno:
                line = i.positions.lineno
            if i.opname in ('LOAD_GLOBAL', 'LOAD_NAME') and i.argval not in available | assigned:
                found.append((i.argval, line, code.co_filename))
        for const in code.co_consts:
            if isinstance(const, types.CodeType):
                scan(const)

    scan(func.__code__)
    return found


def _check_undefined_names(rules, linemaps=None):
    """Raise NameError, listing every undefined name in the methods of the rules' classes."""
    import types
    problems, seen = [], set()
    for rule in rules:
        for cls in type(rule).__mro__:
            if cls in (SnakeRule, SnakeCheckpoint, TargetRule, object) or cls in seen:
                continue
            seen.add(cls)
            for attr, value in vars(cls).items():
                func = value.__func__ if isinstance(value, (staticmethod, classmethod)) else value
                if not isinstance(func, types.FunctionType):
                    continue
                for name, line, filename in _undefined_names(func):
                    line = (linemaps or {}).get(filename, {}).get(line, line)
                    problems.append(f'  {cls.__name__}.{attr}, {filename} line {line}: '
                                    f'name {name!r} is not defined (missing import?)')
    if problems:
        raise NameError('Undefined names in rule classes:\n' + '\n'.join(problems) +
                        '\n(if this is a false alarm, use snakeplusplus.configure(..., check_names=False))')


class PipelineBuilder:
    def __init__(self,pathvars,config_params,env={},retry_failed=True,check_names=True):
        self.pathvars = pathvars
        self.retry_failed = retry_failed
        self.check_names = check_names
        self.config_params = config_params
        self.env = env
        self.rules = {}           # name -> SnakeRule, filled by build()
        self.linemaps = None      # Snakemake line maps, for correct line numbers in error messages
        self.is_main_process = True  # False when Snakemake re-parses the Snakefile to run a job
        self.execution_started = False  # True once Snakemake starts running jobs (not in a dry run)


    # Collect all rules in the scope of namespace.
    # And give the rules a unique name.
    def _collect_rules(self,namespace):
        passed = {}
        for name, obj in namespace.items():
            if isinstance(obj, SnakeRule):
                obj._configure(name,self.config_params.get(obj.__class__.__name__,{}))
                passed[name] = obj

        return passed


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

        owners = {}  # log file or result folder -> the job that uses it

        def check_unique(path, what, rule, wildcards):
            job = (rule.name, tuple(sorted(wildcards.items())))
            other = owners.setdefault(path, job)
            if other != job:
                describe = lambda j: f"rule '{j[0]}'" + (f" with {dict(j[1])}" if j[1] else '')
                raise ValueError(f"Two jobs would use the same {what} {path}: {describe(other)} and "
                                 f"{describe(job)}. Change result_template or log_template so that "
                                 f"their names differ.")

        def visit(rule, wildcards):
            log = rule.log_path(wildcards)
            check_unique(log, 'log file', rule, wildcards)
            check_unique(rule.result_path(wildcards), 'result folder or prefix', rule, wildcards)
            if log in outdated:
                return outdated[log]
            outdated[log] = False  # guards against cycles

            state_file = job_state_file(log, JOB_STATES)
            if state_file and replace_inner_extension(state_file, '.queued') == state_file:
                # left over from a run that was stopped before this job started
                if os.path.getsize(state_file) == 0:
                    os.remove(state_file)  # never ran
                    state_file = None
                else:
                    with open(state_file, 'at') as fp:
                        fp.write('The run was stopped before this job started again.\n')
                    stale = replace_inner_extension(log, '.stale')
                    os.replace(state_file, stale)
                    state_file = stale
            if state_file and replace_inner_extension(state_file, '.running') == state_file:
                with open(state_file, 'at') as fp:
                    fp.write('Job was interrupted (found .running while Snakemake was not active).\n')
                stale = replace_inner_extension(log, '.stale')
                os.replace(state_file, stale)
                state_file = stale
            state = next((st for st in JOB_STATES
                          if state_file and replace_inner_extension(state_file, st) == state_file), None)

            is_outdated = state is None or state == '.stale' or (state == '.error' and self.retry_failed)
            for up_rule, up_wildcards in self._upstream_jobs(rule, wildcards):
                if visit(up_rule, up_wildcards):
                    is_outdated = True
            outdated[log] = is_outdated

            stamp = rule._stamp_path(wildcards)
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
            if not rule._wildcard_names():
                visit(rule, {})


    def _upstream_jobs(self, rule, wildcards):
        """(rule, wildcards) of the jobs that the job (rule, wildcards) takes input from.

        A loop whose items cannot be determined while the Snakefile is loaded (it depends on a
        checkpoint, which Snakemake only resolves later) yields the existing jobs of the looped
        rule, found by their file names, whose wildcards agree with this job's wildcards."""
        for val in rule.inputs.values():
            if _is_input_function(val):
                try:
                    # an input function may choose a connection per job; it expects wildcards
                    # with attribute access, like Snakemake gives them
                    val = _dynamic(val, _Wildcards(wildcards))
                except Exception:
                    continue  # e.g. depends on a checkpoint that has not run yet
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
        return rule._own_wildcards(wildcards)


    def _register_onstart(self, workflow):
        """Mark the jobs of this run as .queued when Snakemake starts executing, keeping any
        onstart handler defined in the Snakefile. Not called for dry runs."""
        previous = getattr(workflow, '_onstart', None)

        def onstart(log):
            # from here on, errors go to the jobs' log files instead of stopping Snakemake
            self.execution_started = True
            try:
                dag = workflow.dag
                self._mark_queued(dag.needrun_jobs())
                # jobs that become known after a checkpoint has run are added in
                # dag.update_checkpoint_dependencies(); mark those too
                original = dag.update_checkpoint_dependencies

                async def update_checkpoint_dependencies(jobs=None):
                    updated = await original(jobs)
                    if updated:
                        try:
                            self._mark_queued(dag.needrun_jobs(), only_new=True)
                        except Exception as e:
                            print(f'Warning: could not mark queued jobs: {e}', file=sys.stderr)
                    return updated

                dag.update_checkpoint_dependencies = update_checkpoint_dependencies
            except Exception as e:  # the dashboard must never stop the run
                print(f'Warning: could not mark queued jobs: {e}', file=sys.stderr)
            if previous is not None:
                previous(log)

        workflow.onstart(onstart)


    def _mark_queued(self, jobs, only_new=False):
        """One .queued file per job that Snakemake is about to run: empty for a job that never ran,
        otherwise the job's current file (.log, .error, .stale) renamed, with a note appended.
        With only_new (while jobs are already running), only jobs without any file are marked."""
        now = datetime.now()
        for job in jobs:
            rule = self.rules.get(job.rule.name)
            if rule is None:
                continue  # not a snakeplusplus rule
            log = rule.log_path(rule._own_wildcards(job.wildcards_dict))
            queued = replace_inner_extension(log, '.queued')
            if only_new:
                if job_state_file(log, JOB_STATES) is None:
                    os.makedirs(op.dirname(queued) or '.', exist_ok=True)
                    open(queued, 'w').close()
                continue
            if job_state_file(log, ('.running',)):
                continue
            current = job_state_file(log, ('.log', '.error', '.stale'))
            os.makedirs(op.dirname(queued) or '.', exist_ok=True)
            if current:
                with open(current, 'at') as fp:
                    fp.write(f'\nQueued to run again at {now}.\n')
                os.replace(current, queued)
            elif not op.exists(queued):
                open(queued, 'w').close()


    def build(self,namespace,inject_rule,checkpoint_magic,verbose=False):
        """
        Build the snakemake pipeline from all rules present in namespace (typically locals() in snakefile)
        """
        self.rules = self._collect_rules(namespace)
        _check_rule_names(self.rules)

        workflow = namespace.get('workflow')
        self.linemaps = getattr(workflow, 'linemaps', None)
        # the cores Snakemake was given (-c); without -c (e.g. a dry run), all cores of this machine
        try:
            self.cores = workflow.cores
        except Exception:
            self.cores = None
        if not self.cores:
            from snakemake.utils import available_cpu_count
            self.cores = available_cpu_count()
        # Snakemake parses the Snakefile again for every job; only be verbose in the main process.
        self.is_main_process = getattr(workflow, 'is_main_process', True)
        verbose = verbose and self.is_main_process

        if self.check_names and self.is_main_process:
            _check_undefined_names(self.rules.values(), self.linemaps)

        self._sync_job_states()

        # the default target (for `snakemake` without target name): like in Snakemake, the first
        # target in the Snakefile; without target(), the first rule without wildcards
        targets = [c for c in self.rules.values() if isinstance(c, TargetRule)]
        targets += [c for c in self.rules.values() if not c._wildcard_names()]
        if not targets:
            raise RuntimeError(f'None of the {len(self.rules)} rules can be a target, because they all have '
                               f'wildcards. Define one with snakeplusplus.target(), e.g. '
                               f'everything = target(myrule.foreach(...))')
        target = targets[0]

        for c in self.rules.values():
            c.default_target = c is target

        if verbose:
            print('Target rule:',target.name)

        if workflow is not None and self.is_main_process:
            self._register_onstart(workflow)

        # inject all available rules
        for rule in self.rules.values():
            r = rule._as_snake(checkpoint_magic)
            if verbose:
                print('Injecting rule\n',json.dumps(vars(r),indent=2,default=str))
            inject_rule(r)


def configure(pathvars,config_params=None,env=None,retry_failed=True,check_names=True):
    """Set up snakeplusplus; call it in the Snakefile before the rules are created.

    Args:
        pathvars: Snakemake's `workflow.pathvars`, with at least `logs` (the folder for the log
            files) and `results` (under which each job gets its result folder).
        config_params: Parameters per rule class, e.g. from Snakemake's config:
            `{'Denoise': {'extent': 7}}`. Parameters given to a rule's constructor take priority.
        retry_failed: Run jobs again that failed in a previous run (that have an `.error` file).
        check_names: Report names that the rules' methods use but that are not defined, e.g. a
            missing import, when the Snakefile is loaded instead of when the job runs.

    Example:
        ```python
        pathvars:
            logs = 'logs',
            results = 'results'

        snakeplusplus.configure(workflow.pathvars, config.get('params'))
        ```
    """
    global _builder
    _builder = PipelineBuilder(pathvars,config_params or {},env or {},retry_failed,check_names)


def target(*rules_or_loops):
    """A named target that runs all given rules and loops, e.g.

        preprocessing = target(preproc.foreach(all_subjects))
        everything = target(tckgen.foreach(all_subjects), report)

    and then `snakemake preprocessing`. The name of the variable is the name of the target. The
    first target in the Snakefile is the default target, used when Snakemake gets no target name.
    A rule without wildcards is a target by itself; a rule with wildcards needs foreach().
    """
    if not rules_or_loops:
        raise TypeError("target() needs at least one rule or loop")
    inputs = {}
    for i, item in enumerate(rules_or_loops):
        rule = item.rule_or_loop if isinstance(item, OutputPromise) else item
        if isinstance(rule, JobLoop):
            rule, given = rule.rule, rule._known_wildcards()
        elif isinstance(rule, SnakeRule):
            given = set()
        else:
            raise TypeError(f"target() expects rules or loops (rule.foreach(...)), got {type(item).__name__}")
        missing = set(rule._wildcard_names()) - given if given is not None else set()
        if missing:
            missing = ', '.join('{' + w + '}' for w in sorted(missing))
            hint = 'foreach(...)' if isinstance(item, SnakeRule) else 'foreach(...) with these wildcards'
            raise ValueError(f"target(): rule {type(rule).__name__} has wildcards {missing}; use {hint}")
        inputs[f'_{i}'] = item
    return TargetRule().set_input(**inputs)


def build(namespace,verbose=False):
    """Turn all rules and targets of the Snakefile into Snakemake rules; the last line of a Snakefile.

    Each rule gets the name of its variable. Call it from the Snakefile itself, not from an
    included file, as `snakeplusplus.build(locals())`.

    Args:
        namespace: The Snakefile's `locals()`.
        verbose: Print the target and the Snakemake rules that are created.
    """
    if 'inject_rule' not in namespace:
        workflow = namespace.get('workflow')
        if workflow is None:
            raise RuntimeError("build() must be called from a Snakefile, as snakeplusplus.build(locals())")
        workflow.include(SNAKEFILE)  # defines inject_rule() and checkpoint_magic() in the namespace
    inject_rule = namespace['inject_rule']
    checkpoint_magic = namespace['checkpoint_magic']
    builder = _get_builder()
    builder.build(namespace,inject_rule,checkpoint_magic,verbose)
