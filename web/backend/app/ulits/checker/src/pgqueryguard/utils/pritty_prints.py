from pathlib import Path

from rich.console import Console

console = Console(force_terminal=True)


def print_validation_errors(errors: list, file: Path):
    console.print(f"===[red] {file} [/red]===")
    for error in errors:
        console.print(
            f"{error['description']}.",
            "On Line:",
            f"{error['line']},",
            "Column:",
            error["col"],
        )
        console.print(
            (
                f"[white]{error['start_context']}[/white]"
                f"[red]{error['highlight']}[/red]"
                f"[white]{error['end_context']}"
            )
        )


def print_total_format_files(formatted: int, errors: int):
    if formatted == 1:
        console.print(f"=== [green]{formatted}[/green] file was formatted ===")
    else:
        console.print(f"=== [green]{formatted}[/green] files were formatted ===")
    if errors == 1:
        console.print(f"=== [red]{errors}[/red] file has error ===")
    elif formatted > 1:
        console.print(f"=== [red]{errors}[/red] files have error ===")
