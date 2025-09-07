from pathlib import Path

from rich import print
from rich.syntax import Syntax


def print_validation_errors(errors: list, file: Path):
    print(f"===[red] {file} [/red]===")
    for error in errors:
        print(
            f"{error['description']}.",
            "On Line:",
            f"{error['line']},",
            "Column:",
            error["col"],
        )
        print(
            (
                f"[white]{error['start_context']}[/white]"
                f"[red]{error['highlight']}[/red]"
                f"[white]{error['end_context']}"
            )
        )


def print_sql(sql: str):
    syntax = Syntax(sql, "sql", theme="monokai", word_wrap=True)
    print(syntax)


def print_total_format_files(formatted: int, errors: int):
    if formatted == 1:
        print(f"=== [green]{formatted}[/green] file was formatted ===")
    else:
        print(f"=== [green]{formatted}[/green] files were formatted ===")
    if errors == 1:
        print(f"=== [red]{errors}[/red] file has error ===")
    elif formatted > 1:
        print(f"=== [red]{errors}[/red] files have error ===")
