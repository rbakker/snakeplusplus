"""The Hello pipeline in Pydra 1.0.

Run from this folder: python hello.py
"""
import csv
from pathlib import Path

from fileformats.generic import File
from pydra.compose import python, workflow


@python.define
def SayHello(greeting: str) -> File:
    out = Path(f"{greeting}-output.txt").absolute()
    out.write_text(greeting + "\n")
    return out


@python.define
def ConvertToUpper(input_file: File) -> File:
    out = Path(f"UPPER-{Path(input_file).name}").absolute()
    out.write_text(Path(input_file).read_text().upper())
    return out


@python.define(outputs=["collected", "report"])
def CollectGreetings(input_files: list[File]) -> tuple[File, File]:
    collected, report = Path("COLLECTED-output.txt").absolute(), Path("report.txt").absolute()
    collected.write_text("".join(Path(f).read_text() for f in input_files))
    report.write_text(f"There were {len(input_files)} greetings in this batch.\n")
    return collected, report


@workflow.define(outputs=["collected", "report"])
def Hello(greetings: list[str]):
    say_hello = workflow.add(SayHello().split(greeting=greetings), name="say_hello")
    upper = workflow.add(ConvertToUpper(input_file=say_hello.out).combine("say_hello.greeting"))
    collect = workflow.add(CollectGreetings(input_files=upper.out))
    return collect.collected, collect.report


if __name__ == "__main__":
    with open("../greetings.csv") as fp:
        greetings = [row[0] for row in csv.reader(fp)]
    outputs = Hello(greetings=greetings)(cache_root=Path("cache").absolute())
    print(outputs.collected, outputs.report, sep="\n")
