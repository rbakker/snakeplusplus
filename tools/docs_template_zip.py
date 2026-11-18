"""MkDocs hook (see mkdocs.yml): zip the folder template/ into docs/snakeplusplus-template.zip before each build,
so that the website offers the current template for download."""
import os
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / 'template'
TARGET = ROOT / 'docs' / 'snakeplusplus-template.zip'
TOP = 'my-pipeline'
SKIP = {'__pycache__', '.snakemake', 'results', 'logs'}


def make_zip(target=TARGET):
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as z:
        for folder, dirs, files in os.walk(TEMPLATE):
            dirs[:] = sorted(d for d in dirs if d not in SKIP)
            rel = Path(folder).relative_to(TEMPLATE)
            if not files and not dirs:
                z.writestr(f'{TOP}/{rel.as_posix()}/', '')  # keep empty folders
            for f in sorted(files):
                if not f.endswith('-diagram.html'):
                    z.write(Path(folder) / f, f'{TOP}/{(rel / f).as_posix()}')
        # folders that the pipeline fills, empty in the template
        for name in ['results', 'logs']:
            z.writestr(f'{TOP}/{name}/', '')
    return target


def on_pre_build(config, **kwargs):
    make_zip()
