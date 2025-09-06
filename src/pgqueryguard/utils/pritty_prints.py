from rich import print
from rich.syntax import Syntax


def print_validation_errors(errors: list):
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
