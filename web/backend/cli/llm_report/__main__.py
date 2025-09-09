import os
from pathlib import Path
from typing_extensions import Annotated

import sqlparse
from sqlalchemy import create_engine

from pgqueryguard.checkers.validator import validate_query
from pgqueryguard.outer_database.advice import advise_from_plan
from pgqueryguard.outer_database.count_resourses import estimate_profile
from pgqueryguard.outer_database.inspect import (
    read_table_stats,
    run_explain,
)
from pgqueryguard.utils.async_run import async_command
from pgqueryguard.query_files.files import get_sql_files, read_file
from app.utils.llm.report import write_html_report
from pgqueryguard.query_files.report_index import IndexItem, write_index_page
from pgqueryguard.utils.annotaions import (
    DBUrlOption,
    PathArgument,
)
from pgqueryguard.utils.pritty_prints import (
    print_validation_errors,
)
from app.utils.llm.query_improve import improve_and_filter_sql
import typer

app = typer.Typer()

@app.command()
@async_command
async def report(
    directory: PathArgument,
    db_url: Annotated[
        str,
        typer.Option(
            "--db-url",
            help="Url for database connection",
        ),
    ],
):
    files = get_sql_files(directory, True)
    error_files = 0
    items_for_index = []
    output_dir = "pgqueryguard_reports"
    reports_subdir = os.path.join(output_dir, "reports")
    os.makedirs(reports_subdir, exist_ok=True)

    engine = create_engine(str(db_url))

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
            ai_adv = await improve_and_filter_sql(engine, query, profile=profile, n_variants=5)
            write_html_report(out_html, plan, profile, adv, ai_adv, query)

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
    print("=== Report: ./pgqueryguard_reports/index.html ===")


if __name__ == "__main__":
    app()
