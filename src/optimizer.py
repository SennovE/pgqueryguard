import sqlglot
from sqlglot.optimizer import optimize


def optimize_query(sql: str, schema: dict[str, dict[str, str]]) -> str:
    return optimize(sqlglot.parse_one(sql), schema=schema).sql(pretty=True)
