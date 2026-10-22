"""Draw a diagram of a snakeplusplus pipeline: an HTML page with the graph of its rules, and a
reference of each rule's wildcards, inputs, outputs and parameters.

    snakeplusplus-diagram                       # finds the Snakefile, like Snakemake
    snakeplusplus-diagram workflow/drt.smk      # writes drt-diagram.html
    snakeplusplus-diagram -o pipeline.html --config tracking_root=/data

The default output is <Snakefile name>-diagram.html in the pipeline folder: the folder of the
Snakefile, or the folder above it if that is called `workflow`. The Snakefile is only parsed with
the Snakemake API; nothing is run.
"""
import argparse
import html
import os
from os import path as op
from pathlib import Path

from pydantic_core import PydanticUndefined

import snakeplusplus
from snakeplusplus import SnakeRule, TargetRule, LoopPromise, OutputPromise, _is_incomplete_checkpoint, _is_input_function, _short_type_name


def load_pipeline(snakefile, config=None, configfiles=None, workdir=None):
    """Parse a Snakefile with the Snakemake API and return the configured PipelineBuilder."""
    from snakemake.api import SnakemakeApi
    from snakemake.settings.types import ConfigSettings, OutputSettings, ResourceSettings

    config_settings = ConfigSettings(
        config=config or {},
        configfiles=[Path(f).absolute() for f in (configfiles or [])],
    )
    with SnakemakeApi(OutputSettings()) as api:
        workflow_api = api.workflow(
            resource_settings=ResourceSettings(),
            config_settings=config_settings,
            snakefile=Path(snakefile).absolute(),
            workdir=Path(workdir).absolute() if workdir else None,
        )
        snakeplusplus._read_only = True
        workflow_api._workflow  # parses the Snakefile, which calls snakeplusplus.build()
    return snakeplusplus._get_builder()


def _esc(text):
    return html.escape(str(text), quote=True)


_type_name = _short_type_name


def _loop_label(loop):
    """(loop levels, count text, name of the checkpoint rule it depends on or None), without
    triggering checkpoints. Levels are shown outer first, e.g. 'subject × session'."""
    levels = []
    for level in loop.levels:
        if level[0] == 'values':
            levels.append(level[1])
        elif callable(level[1]):
            levels.append(getattr(level[1], '__name__', 'function') + '()')
        else:
            keys = list(dict.fromkeys(k for d in level[1] for k in d))
            levels.append('(' + ', '.join(keys) + ')')
    wc = ' × '.join(levels)
    if loop.fixed:
        wc = ', '.join(f'{k}={v}' for k, v in loop.fixed.items()) + ('; ' + wc if wc else '')
    wc = wc or 'loop'
    try:
        members = loop.members({})
    except Exception as e:
        # IncompleteCheckpointException: depends on a checkpoint; other errors: depends on
        # the wildcards of the receiving job, or on files that do not exist yet.
        if _is_incomplete_checkpoint(e):
            return wc, 'from checkpoint', getattr(getattr(e, 'rule', None), 'name', None)
        return wc, 'per job', None
    # a level given by a function: show the wildcards it sets rather than the function's name
    known = set(loop.fixed) | {level[1] for level in loop.levels if level[0] == 'values'}
    found = [k for k in (members[0] if members else {}) if k not in known and k in loop.rule._wildcard_names()]
    for level in loop.levels:
        if level[0] == 'dicts' and callable(level[1]) and found:
            wc = wc.replace(getattr(level[1], '__name__', 'function') + '()', '/'.join(found))
    return wc, f'{len(members)} items', None


def mermaid_graph(rules):
    lines = [
        "---",
        "config:",
        "  theme: 'base'",
        "  themeVariables:",
        "    primaryColor: '#eee'",
        "    primaryTextColor: '#000'",
        "    primaryBorderColor: '#bbb'",
        "    lineColor: '#000'",
        "    secondaryColor: '#ffe'",
        "    tertiaryColor: '#fff'",
        "---",
        "graph LR",
        "    classDef checkpointStyle fill:#fff4e0,stroke:#e0a040,stroke-width:2px",
        "    classDef targetStyle fill:#e8f4e8,stroke:#60a060,stroke-width:2px",
        "    classDef loopStyle fill:#fff,stroke:#8080ff",
    ]
    ids = {id(rule): f'node{i}' for i, rule in enumerate(rules.values())}
    ids_by_name = {name: ids[id(rule)] for name, rule in rules.items()}

    for name, rule in rules.items():
        cls = type(rule)
        node_id = ids[id(rule)]
        w_keys = rule._wildcard_names()
        w_text = f"<br/><small><font color='blue'>{_esc('/'.join(w_keys))}</font></small>" if w_keys else ''
        kind = '<br/><small><i>checkpoint</i></small>' if rule.is_checkpoint else ''
        label = (f"<div style='border-bottom: 1px solid black'><b>{_esc(name)}</b></div>"
                 f"{'<i>target</i>' if isinstance(rule, TargetRule) else _esc(cls.__name__)}{kind}{w_text}")
        if rule.is_checkpoint:
            lines.append(f'    {node_id}{{{{"{label}"}}}}:::checkpointStyle')
        elif isinstance(rule, TargetRule):
            lines.append(f'    {node_id}(["{label}"]):::targetStyle')
        else:
            lines.append(f'    {node_id}["{label}"]')

        for key, val in rule.inputs.items():
            if not isinstance(val, OutputPromise):
                continue
            source = val.rule_or_loop
            loop = source if isinstance(source, LoopPromise) else None
            source = loop.rule if loop else source
            if id(source) not in ids:
                continue  # rule not assigned to a variable in the Snakefile
            source_id = ids[id(source)]
            edge_label = val.key or (key if not key.startswith('_') and not loop else '')
            if val.parser is not None:
                edge_label += f' ⟶ {getattr(val.parser, "__name__", "parser")}()'
            if val.condition:
                edge_label += ' if ' + ', '.join(f'{k}={v!r}' for k, v in val.condition.items())
            edge = f'|"{_esc(edge_label)}"|' if edge_label else ''
            arrow = '-.->' if val.condition else '-->'
            if loop:
                agg_id = f'agg_{node_id}_{key}'
                wc, count, checkpoint = _loop_label(loop)
                agg_label = (f"<span style='font-size: 2em; line-height: 0.8'>&#8635;</span><br/>"
                             f"<font color='blue'><small>{_esc(wc)}<br/>{_esc(count)}</small></font>")
                lines.append(f'    {agg_id}(("{agg_label}")):::loopStyle')
                lines.append(f'    {source_id} {arrow}{edge} {agg_id}')
                into = f'|"{_esc(key)}"|' if not key.startswith('_') else ''
                lines.append(f'    {agg_id} ==>{into} {node_id}')
                if checkpoint in ids_by_name:
                    lines.append(f'    {ids_by_name[checkpoint]} -.->|"defines items"| {agg_id}')
            else:
                lines.append(f'    {source_id} {arrow}{edge} {node_id}')
    return '\n'.join(lines)


def _field_rows(title, model, show_default=True):
    rows = []
    for fname, field in model.model_fields.items():
        default = '' if field.default is PydanticUndefined or not show_default else _esc(repr(field.default))
        rows.append(f"<tr><td>{title}</td><td><code>{_esc(fname)}</code></td>"
                    f"<td><code>{_esc(_type_name(field.annotation))}</code></td>"
                    f"<td>{default}</td><td>{_esc(field.description or '')}</td></tr>")
    return rows


def _connection(val):
    if isinstance(val, OutputPromise):
        src = val.rule_or_loop
        text = f"{src.rule.name}.foreach(…)" if isinstance(src, LoopPromise) else src.name
        if val.key:
            text += f".{val.key}"
        if val.parser is not None:
            text += f" → {getattr(val.parser, '__name__', 'parser')}()"
        if val.condition:
            text += ' if ' + ', '.join(f'{k}={v!r}' for k, v in val.condition.items())
        return text
    if _is_input_function(val):
        return f"function {getattr(val, '__name__', '')}(wildcards)"
    return str(val)


def rule_card(name, rule):
    cls = type(rule)
    kind = 'checkpoint' if rule.is_checkpoint else 'target' if isinstance(rule, TargetRule) else 'rule'
    doc = (cls.__doc__ or '').strip()
    description = rule.description if rule.description != SnakeRule.description else doc
    wildcard_rows = [f"<tr><td>wildcard</td><td><code>{_esc(w)}</code></td><td><code>str</code></td>"
                     f"<td></td><td></td></tr>" for w in rule._wildcard_names()]
    rows = (wildcard_rows + _field_rows('input', rule.InputModel, False)
            + _field_rows('output', rule.OutputModel) + _field_rows('param', rule.ParamModel))
    inputs = ''.join(f"<li><code>{_esc(k)}</code> ← {_esc(_connection(v))}</li>" for k, v in rule.inputs.items())
    params = ''.join(f"<li><code>{_esc(k)}</code> = {_esc(repr(v))}</li>" for k, v in rule.params.items())
    return f"""
<div class="rule-card" id="{_esc(name)}">
<h3>{_esc(name)} <small>({_esc(cls.__name__)}, {kind})</small></h3>
<p>{_esc(description)}</p>
<p class="meta">log: <code>{_esc(rule.log_path())}</code><br/>results: <code>{_esc(rule.result_path())}</code></p>
{f'<h4>Connected inputs</h4><ul>{inputs}</ul>' if inputs else ''}
{f'<h4>Parameters set in this pipeline</h4><ul>{params}</ul>' if params else ''}
<table>
<tr><th>Kind</th><th>Name</th><th>Type</th><th>Default</th><th>Description</th></tr>
{''.join(rows)}
</table>
</div>"""


def create_diagram(builder, output_path, title='Pipeline diagram'):
    rules = builder.rules
    cards = ''.join(rule_card(name, rule) for name, rule in rules.items())
    page = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>{_esc(title)}</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
<script>
mermaid.initialize({{startOnLoad: false, securityLevel: 'loose'}});
window.addEventListener('load', async () => {{
    await mermaid.run();
    // Mermaid draws an (empty) label background for edges without a label; hide those
    document.querySelectorAll('.edgeLabel').forEach(e => {{ if (!e.textContent.trim()) e.style.display = 'none'; }});
}});
</script>
<style>
body {{ font-family: sans-serif; margin: 40px; line-height: 1.6; color: #333; }}
.rule-card {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 8px; }}
.meta {{ color: #666; font-size: 0.9em; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
th, td {{ text-align: left; padding: 6px 8px; border-bottom: 1px solid #eee; vertical-align: top; }}
th {{ background-color: #f4f4f4; }}
h3 {{ margin-top: 0; color: #2c3e50; }}
.edgeLabel div {{ background-color: #ffe !important; border: 1px solid #bbb !important;
    border-radius: 0.5ex; padding: 2px 0.5ex !important; font-size: 0.9em; }}
</style>
</head>
<body>
<h1>{_esc(title)}</h1>
<pre class="mermaid">
{mermaid_graph(rules)}
</pre>
<h2>Rule details</h2>
{cards}
</body>
</html>
"""
    os.makedirs(op.dirname(op.abspath(output_path)), exist_ok=True)
    with open(output_path, 'w') as fp:
        fp.write(page)
    return output_path


def _parse_config(items):
    import yaml
    config = {}
    for item in items or []:
        key, _, value = item.partition('=')
        config[key] = yaml.safe_load(value)
    return config


SNAKEFILE_LOCATIONS = ['Snakefile', 'snakefile', 'workflow/Snakefile', 'workflow/snakefile']


def default_output(snakefile):
    """<Snakefile name>-diagram.html in the pipeline folder: the folder of the Snakefile, or the
    folder above it if that is called `workflow` (Snakemake's recommended layout)."""
    folder = op.dirname(op.abspath(snakefile))
    if op.basename(folder) == 'workflow':
        folder = op.dirname(folder)
    name = op.splitext(op.basename(snakefile))[0]
    return op.join(folder, f'{name}-diagram.html')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('snakefile', nargs='?', help='default: Snakefile or workflow/Snakefile, like Snakemake')
    parser.add_argument('-o', '--output', help='default: <Snakefile name>-diagram.html in the pipeline folder')
    parser.add_argument('--config', nargs='*', metavar='KEY=VALUE', help='same as snakemake --config')
    parser.add_argument('--configfile', nargs='*', help='same as snakemake --configfile')
    parser.add_argument('--directory', help='working directory, same as snakemake --directory')
    parser.add_argument('--title', default='Pipeline diagram')
    args = parser.parse_args(argv)

    snakefile = args.snakefile or next((f for f in SNAKEFILE_LOCATIONS if op.isfile(f)), None)
    if snakefile is None:
        parser.error(f"no Snakefile found here ({', '.join(SNAKEFILE_LOCATIONS)}); give its path")
    output = op.abspath(args.output or default_output(snakefile))  # before the Snakemake API changes the working directory
    builder = load_pipeline(snakefile, _parse_config(args.config), args.configfile, args.directory)
    create_diagram(builder, output, args.title)
    print(f"Diagram written to: {output}")


if __name__ == '__main__':
    main()
