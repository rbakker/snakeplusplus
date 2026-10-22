# Word counts of a few texts, used for the code fragments on the pages "Log files", "When jobs
# rerun" and "Where errors go". It runs without external tools other than `wc`; a test runs it.
import json
from pathlib import Path

import snakeplusplus
from snakeplusplus import SnakeRule, target, Field, JobLoop, JobError

snakeplusplus.configure(dict(logs='logs', results='results'), config.get('params'))

TEXTS = ['fox', 'hamlet', 'short']


# --8<-- [start:count]
class CountWords(SnakeRule):
    """Count the words of one text."""
    result_template = 'text-{name}'

    class InputModel:
        text: Path

    class ParamModel:
        min_words: int = Field(1, description='fewer words than this is an error')

    class OutputModel:
        words: int
        upper: Path = Field('upper.txt')

    def run(self, job, input, output, params, wildcards):
        code, out = job.run(['wc', '-w', input.text])
        words = int(out.split()[0])
        if words < params.min_words:
            raise JobError(f'{input.text} has {words} words, fewer than min_words={params.min_words}')
        output.upper.write_text(input.text.read_text().upper())
        output(words=words)
# --8<-- [end:count]


class Report(SnakeRule):
    """The word counts of all texts."""
    class InputModel:
        words: JobLoop[int]

    class OutputModel:
        table: Path = Field('word_counts.json')

    def run(self, job, input, output, params, wildcards):
        json.dump({wc.name: n for wc, n in input.words.items()}, open(output.table, 'w'), indent=2)


# --8<-- [start:graph]
count_words = CountWords().set_input(text='texts/{name}.txt')
word_report = Report().set_input(words=count_words.foreach(name=TEXTS).get_output('words'))
everything = target(word_report)
# --8<-- [end:graph]

snakeplusplus.build(locals())
