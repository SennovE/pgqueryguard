from src.formatter import check_query
from src.optimizer import optimize_query

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


if __name__ == "__main__":
    sql = """
        SELECT A OR (B OR (C AND D))
        FROM x
        WHERE Z = date '2021-01-01' + INTERVAL '1' month OR 1 = 0
    """
    sql = optimize_query(
        sql,
        schema={"x": {"A": "INT", "B": "INT", "C": "INT", "D": "INT", "Z": "STRING"}},
    )
    sql = check_query(sql)
    print(sql)
