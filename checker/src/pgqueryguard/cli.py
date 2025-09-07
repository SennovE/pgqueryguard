from pgqueryguard.checkers.optimizer import optimize_query
from pgqueryguard.outer_database.inspect import get_column_types_from_sql
from pgqueryguard.query_files.files import get_sql_files, write_file
from pgqueryguard.utils.parse_config import parse_opts_for_sqlglot
from pgqueryguard.utils.pritty_prints import (
    print_total_format_files,
    print_validation_errors,
)
from pgqueryguard.checkers.validator import validate_query
from pgqueryguard.checkers.formatters import (
    format_with_pg_formatter,
    format_with_sqlglot,
)

import logging
import typer
from enum import StrEnum

from pgqueryguard.query_files.files import read_file
from pgqueryguard.utils.async_run import async_command
from pgqueryguard.utils.annotaions import (
    DBUrlOption,
    PathArgument,
    RecursiveOption,
    FixOption,
    PgFormatFileOption,
    FormatConfigOption,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

app = typer.Typer()


class FormatterParameter(StrEnum):
    DEFAULT = "default"
    PG_FORMAT = "pg_format"


@app.command()
@async_command
async def check(
    directory: PathArgument,
    db_url: DBUrlOption = None,
    recursive: RecursiveOption = True,
    fix: FixOption = False,
    pg_format_file: PgFormatFileOption = None,
    config: FormatConfigOption = None,
):
    files = get_sql_files(directory, recursive)
    error_files = 0
    formatted_files = 0

    opts = None
    if config:
        if pg_format_file:
            opts = ["--no-rcfile", "-c", str(config)]
        else:
            opts = await parse_opts_for_sqlglot(config)

    for file in files:
        base_query = await read_file(file)
        query = base_query
        errors = validate_query(query)
        if errors:
            print_validation_errors(errors, file)
            error_files += 1
            continue
        if db_url:
            scheme = get_column_types_from_sql(query, f"postgresql://{db_url}")
            query = optimize_query(query, scheme)
        if pg_format_file:
            query = await format_with_pg_formatter(
                query, pg_format_file, opts or []
            )
        else:
            query = format_with_sqlglot(query, opts or {})

        if fix:
            if base_query != query:
                await write_file(file, query)
                formatted_files += 1

    print_total_format_files(formatted_files, error_files)
    if error_files:
        raise typer.Exit(code=1)


def main():
    app()
