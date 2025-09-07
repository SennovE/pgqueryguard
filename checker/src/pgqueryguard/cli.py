import logging
import os
from enum import StrEnum
from pathlib import Path

import sqlparse
import typer
from sqlalchemy import create_engine

from pgqueryguard.checkers.formatters import (
    format_with_pg_formatter,
    format_with_sqlglot,
)
from pgqueryguard.checkers.optimizer import optimize_query
from pgqueryguard.checkers.validator import validate_query
from pgqueryguard.outer_database.advice import advise_from_plan
from pgqueryguard.outer_database.count_resourses import estimate_profile
from pgqueryguard.outer_database.inspect import (
    get_column_types_from_sql,
    read_table_stats,
    run_explain,
)
from pgqueryguard.query_files.files import get_sql_files, read_file, write_file
from pgqueryguard.query_files.report import write_html_report
from pgqueryguard.query_files.report_index import IndexItem, write_index_page
from pgqueryguard.utils.annotaions import (
    DBUrlOption,
    FixOption,
    FormatConfigOption,
    PathArgument,
    PgFormatFileOption,
    RecursiveOption,
)
from pgqueryguard.utils.async_run import async_command
from pgqueryguard.utils.parse_config import parse_opts_for_sqlglot
from pgqueryguard.utils.pritty_prints import (
    print_total_format_files,
    print_validation_errors,
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
        engine = create_engine(f"postgresql://{db_url}")
        if db_url:
            scheme = get_column_types_from_sql(engine, query)
            query = optimize_query(query, scheme)
        if pg_format_file:
            query = await format_with_pg_formatter(query, pg_format_file, opts or [])
        else:
            query = format_with_sqlglot(query, opts or {})

        if fix:
            if base_query != query:
                await write_file(file, query)
                formatted_files += 1

    print_total_format_files(formatted_files, error_files)
    if error_files:
        raise typer.Exit(code=1)


@app.command()
@async_command
async def report(
    directory: PathArgument,
    db_url: DBUrlOption = None,
    recursive: RecursiveOption = True,
):
    files = get_sql_files(directory, recursive)
    error_files = 0
    items_for_index = []
    output_dir = "pgqueryguard_reports"
    reports_subdir = os.path.join(output_dir, "reports")
    os.makedirs(reports_subdir, exist_ok=True)

    engine = create_engine(f"postgresql://{db_url}")

    for file in files:
        base_query = await read_file(file)
        errors = validate_query(base_query)
        if errors:
            print_validation_errors(errors, file)
            error_files += 1
            continue

        for i, query in enumerate(
            s.strip() for s in sqlparse.split(base_query) if s.strip()
        ):
            basename = Path(file).name
            out_html = os.path.join(reports_subdir, f"{basename}_{i}.html")
            os.makedirs(os.path.dirname(out_html), exist_ok=True)
            rel_path = os.path.relpath(out_html, output_dir).replace("\\", "/")

            plan = run_explain(engine, query)
            profile = estimate_profile(plan)
            adv = advise_from_plan(plan, read_table_stats(engine))
            write_html_report(out_html, plan, profile, adv)

            sql_text_for_excerpt = query.strip().replace("\n", " ")
            items_for_index.append(
                IndexItem(
                    title=Path(file).name,
                    file=str(Path(file)),
                    report_rel=rel_path,
                    risk=(
                        "HIGH"
                        if getattr(profile, "est_pages", 0) >= 500_000
                        or getattr(profile, "est_memory_bytes", 0) >= 1_000_000_000
                        else "MED"
                        if getattr(profile, "est_pages", 0) >= 100_000
                        or getattr(profile, "est_memory_bytes", 0) >= 256_000_000
                        else "LOW"
                    ),
                    total_cost=float(getattr(profile, "total_cost", 0.0)),
                    est_pages=float(getattr(profile, "est_pages", 0.0)),
                    est_bytes=float(getattr(profile, "est_bytes", 0.0)),
                    warnings=len(getattr(profile, "warnings", []) or []),
                    excerpt=sql_text_for_excerpt[:180],
                )
            )

    write_index_page("pgqueryguard_reports", items_for_index)
    print(f"=== Report: ./pgqueryguard_reports/index.html ===")


def main():
    app()
