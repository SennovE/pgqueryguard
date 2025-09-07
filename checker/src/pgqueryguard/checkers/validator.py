from logging import getLogger
from typing import Any

import sqlglot

logger = getLogger(__name__)


def validate_query(sql: str) -> list[dict[str, Any]]:
    try:
        sqlglot.transpile(sql, write="postgres", pretty=True)[0]
    except sqlglot.ParseError as exc:
        return exc.errors
    return []
