from pathlib import Path
from pgqueryguard.utils.pritty_prints import print_validation_errors, print_sql
from pgqueryguard.validator import validate_query
from pgqueryguard.formatters import format_with_pg_formatter, format_with_sqlglot

import logging
import typer
from enum import StrEnum

from pgqueryguard.query_read.from_file import read_file
from pgqueryguard.utils.async_run import async_command

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

app = typer.Typer()


class FormatterParameter(StrEnum):
    DEFAULT = "default"
    PG_FORMAT = "pg_format"


@app.command()
@async_command
async def check_from_file(
    filename: Path = typer.Argument(..., exists=True, readable=True),
    connection_url: str | None = typer.Option(
        None, "--connection-url", "-c", help="Database conection url"
    ),
    pg_format_file: Path | None = typer.Option(
        None,
        "--pg-format-file",
        "-f",
        exists=True,
        readable=True,
        help="Path to pg_format executable file",
    ),
    pg_format_cofig: Path | None = typer.Option(
        None,
        "--pg-format-cofig",
        exists=True,
        readable=True,
        help="Path to pg_format cofiguration file",
    ),
):
    query = await read_file(filename)
    errors = validate_query(query)
    if errors:
        print_validation_errors(errors)
        return
    # optimized_query = optimize_query(query)
    if pg_format_file:
        opts = []
        if pg_format_cofig:
            opts = ["--no-rcfile", "-c", pg_format_cofig]
        formatted_query = await format_with_pg_formatter(
            query,
            pg_format_file,
            opts,
        )
    else:
        formatted_query = format_with_sqlglot(query)

    print_sql(formatted_query)


def main():
    app()
