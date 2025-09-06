import sqlglot
from sqlglot.optimizer import optimize


def optimize_query(sql: str, schema: dict[str, dict[str, str]]) -> str:
    return optimize(
        sqlglot.parse_one(sql),
        schema=schema,
        validate_qualify_columns=False,
        dialect="postgres",
    ).sql(dialect="postgres", pretty=True)
