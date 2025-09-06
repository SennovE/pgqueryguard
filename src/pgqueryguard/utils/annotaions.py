from typing_extensions import Annotated

from pathlib import Path

import typer


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
PgFormatConfigOption = Annotated[
    Path | None,
    typer.Option(
        "--pg-format-config",
        exists=True,
        readable=True,
        help="Path to pg_format configuration file",
    ),
]
FixOption = Annotated[
    bool,
    typer.Option(
        "--fix",
        help="If set to true, will format files",
    ),
]
