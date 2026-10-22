# A small diffusion MRI pipeline, used for the code fragments on the concept pages.
# The commands are MRtrix3 tools; the pipeline is tested by loading it (snakeplusplus-diagram),
# which checks all names, types and connections without running anything.
import json
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, JobLoop, JobError

pathvars:
    logs = 'logs',
    results = 'results'

snakeplusplus.configure(workflow.pathvars, config.get('params'))

SUBJECTS = ['01', '02', '03']


# --8<-- [start:denoise]
class Denoise(SnakeRule):
    """Remove noise from a diffusion-weighted image."""
    result_template = 'sub-{subject}'
    threads_budget = 'cores'

    class InputModel:
        dwi: Path = Field(description='diffusion-weighted image')

    class OutputModel:
        denoised: Path = Field('dwi_denoised.mif', description='denoised image')
        noise: Path = Field('noise.mif', description='estimated noise level')

    class ParamModel:
        extent: int = Field(5, description='size of the sliding window, in voxels')

    def run(self, job, input, output, params, wildcards):
        job.run(['dwidenoise', input.dwi, output.denoised, '-noise', output.noise,
                 '-extent', str(params.extent), '-nthreads', str(job.threads)])
# --8<-- [end:denoise]


# --8<-- [start:base]
class SubjectRule(SnakeRule):
    """Base class for rules that run once per subject."""
    result_template = 'sub-{subject}'


class FitTensor(SubjectRule):
    class InputModel:
        dwi: Path
        mask: Path | None = None

    class OutputModel:
        fa: Path = Field('fa.mif')
        md: Path = Field('md.mif')

    def run(self, job, input, output, params, wildcards):
        job.run(['dwi2tensor', input.dwi, job.tmpdir() / 'tensor.mif'] + (['-mask', input.mask] if input.mask else []))
        job.run(['tensor2metric', job.tmpdir() / 'tensor.mif', '-fa', output.fa, '-adc', output.md])
# --8<-- [end:base]


# --8<-- [start:value_output]
class MeanFA(SubjectRule):
    """The mean FA of a subject."""
    class InputModel:
        fa: Path

    class OutputModel:
        mean_fa: float

    def run(self, job, input, output, params, wildcards):
        code, text = job.run(['mrstats', input.fa, '-output', 'mean'])
        output(mean_fa=float(text.split()[0]))
# --8<-- [end:value_output]


# --8<-- [start:tracking]
class Track(SnakeRule):
    """Streamlines of one tract, in both hemispheres, written with a common prefix."""
    result_template = 'sub-{subject}/tract-{tract}_*'
    threads_budget = 'cores'

    class InputModel:
        fod: Path

    class OutputModel:
        DRT_L: Path = Field('L.tck')
        DRT_R: Path = Field('R.tck')

    def run(self, job, input, output, params, wildcards):
        for out in (output.DRT_L, output.DRT_R):
            job.run(['tckgen', input.fod, out, '-nthreads', str(job.threads)])
# --8<-- [end:tracking]


class Filter(SnakeRule):
    """Filter the streamlines of one hemisphere."""
    result_template = 'sub-{subject}/tract-{tract}/{hemi}_*'

    class InputModel:
        tck: Path

    class OutputModel:
        filtered: Path = Field('filtered.tck')

    def run(self, job, input, output, params, wildcards):
        job.run(['tckedit', input.tck, output.filtered, '-minlength', '40'])


class FOD(SubjectRule):
    class InputModel:
        dwi: Path

    class OutputModel:
        fod: Path = Field('wm_fod.mif')

    def run(self, job, input, output, params, wildcards):
        job.run(['dwi2fod', 'csd', input.dwi, 'response.txt', output.fod])


def read_float(path: Path) -> float:
    return float(open(path).read())


class GroupFA(SnakeRule):
    """Collect the mean FA of all subjects."""
    class InputModel:
        mean_fa: JobLoop[float | JobError]

    class OutputModel:
        table: Path = Field('mean_fa.json')

    def run(self, job, input, output, params, wildcards):
        ok = {wc.subject: v for wc, v in input.mean_fa.items() if not isinstance(v, JobError)}
        json.dump(ok, open(output.table, 'w'), indent=2)


# --8<-- [start:graph]
denoise = Denoise().set_input(dwi='data/sub-{subject}/dwi.mif')
denoise_wide = Denoise(extent=7).set_input(dwi='data/sub-{subject}/dwi.mif')

fit = FitTensor().set_input(dwi=denoise.get_output('denoised'))
mean_fa = MeanFA().set_input(fa=fit.get_output('fa'))
group = GroupFA().set_input(
    mean_fa=mean_fa.foreach(subject=SUBJECTS).get_output('mean_fa'),
)
# --8<-- [end:graph]

# --8<-- [start:pattern]
fod = FOD().set_input(dwi=denoise.get_output('denoised'))
track = Track().set_input(fod=fod.get_output('fod'))
filter_tracts = Filter().set_input(tck=track.get_output('DRT_{hemi}'))
# --8<-- [end:pattern]

everything = target(group, filter_tracts.foreach(subject=SUBJECTS).foreach(tract=['AF']).foreach(hemi=['L', 'R']))

snakeplusplus.build(locals())
