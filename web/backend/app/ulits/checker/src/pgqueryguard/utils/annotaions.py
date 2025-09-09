from pathlib import Path

import typer
from typing_extensions import Annotated

PathArgument = Annotated[Path, typer.Argument(exists=True, readable=True)]
RecursiveOption = Annotated[
    bool,
    typer.Option(
        "--recursive",
        help="Search *.sql/*.psql/*.pgsql in folders with recursion",
    ),
]
PgFormatFileOption = Annotated[
    Path | None,
    typer.Option(
        "--pg-format-file",
        "-f",
        exists=True,
        readable=True,
        help="Path to pg_format executable file",
    ),
]
FormatConfigOption = Annotated[
    Path | None,
    typer.Option(
        "--config",
        "-c",
        exists=True,
        readable=True,
        help="Path to format configuration file",
    ),
]
FixOption = Annotated[
    bool,
    typer.Option(
        "--fix",
        help="If set to true, will format files",
    ),
]
DBUrlOption = Annotated[
    str | None,
    typer.Option(
        "--db-url",
        help="Url for database connection",
    ),
]
