import sqlglot

from logging import getLogger

logger = getLogger(__name__)


def check_query(sql: str):
    try:
        formatted_sql = sqlglot.transpile(sql, write="postgres", pretty=True)[0]
    except sqlglot.ParseError:
        logger.exception("Parse error")
    return formatted_sql
