import sqlglot
from sqlglot import exp
from sqlalchemy import create_engine, text


def get_column_types_from_sql(sql_query: str, db_url: str) -> dict[str, dict[str, str]]:
    needed_by_table = _extract_needed_tables_and_columns(sql_query)
    if not needed_by_table:
        return {}

    engine = create_engine(db_url, future=True)
    schema_map: dict[str, dict[str, str]] = {}

    meta_query = text("""
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
        FROM pg_catalog.pg_attribute AS a
        JOIN pg_catalog.pg_class     AS c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND c.relkind IN ('r','p','v','m','f')
          AND c.relname = :table_name
          AND (
               (:schema_name IS NULL AND n.nspname = ANY (current_schemas(TRUE)))
            OR (:schema_name IS NOT NULL AND n.nspname = :schema_name)
          )
    """)

    with engine.connect() as conn:
        for table_ref, wanted_cols in needed_by_table.items():
            if "." in table_ref:
                schema_name, table_name = table_ref.split(".", 1)
            else:
                schema_name, table_name = None, table_ref

            rows = (
                conn.execute(
                    meta_query, {"schema_name": schema_name, "table_name": table_name}
                )
                .mappings()
                .all()
            )

            if not rows:
                continue

            actual_schema = rows[0]["schema_name"]
            qualified_key = f"{actual_schema}.{table_name}"
            table_columns: dict[str, str] = {}

            for row in rows:
                col = row["column_name"]
                if not wanted_cols or col in wanted_cols:
                    table_columns[col] = _normalize_pg_type(row["data_type"])

            if not table_columns:
                for row in rows:
                    table_columns[row["column_name"]] = _normalize_pg_type(
                        row["data_type"]
                    )

            schema_map[qualified_key] = table_columns
            schema_map.setdefault(table_name, table_columns.copy())

    return schema_map


def _extract_needed_tables_and_columns(sql_query: str) -> dict[str, set[str]]:
    expr = sqlglot.parse_one(sql_query, read="postgres")

    alias_to_table: dict[str, str] = {}
    tables: set[str] = set()

    def ident(i: exp.Identifier | None) -> str | None:
        return i.name if isinstance(i, exp.Identifier) else None

    for t in expr.find_all(exp.Table):
        fq = ".".join([p for p in (ident(t.db), ident(t.this)) if p]) or ident(t.this)
        tables.add(fq)
        if isinstance(t.parent, exp.Alias):
            a = ident(t.parent.alias)
            if a:
                alias_to_table[a] = fq

    needed: dict[str, set[str]] = {t: set() for t in tables}

    for c in expr.find_all(exp.Column):
        col = ident(c.this)
        if not col:
            continue
        ref = ident(c.table) if isinstance(c.table, exp.Identifier) else None
        targets = [alias_to_table.get(ref, ref)] if ref else list(tables)
        for tname in targets:
            if tname:
                needed.setdefault(tname, set()).add(col)

    return needed


def _normalize_pg_type(type_text: str) -> str:
    s = type_text.lower()
    if "bigint" in s:
        return "BIGINT"
    if "smallint" in s:
        return "SMALLINT"
    if "integer" in s or " int" in s or s == "int":
        return "INT"
    if "numeric" in s or "decimal" in s:
        return "DECIMAL"
    if "double" in s or "real" in s:
        return "DOUBLE"
    if "boolean" in s:
        return "BOOLEAN"
    if "timestamp" in s:
        return "TIMESTAMPTZ" if "with time zone" in s else "TIMESTAMP"
    if s.startswith("date") and "time" not in s:
        return "DATE"
    if s.startswith("time"):
        return "TIME"
    if "json" in s:
        return "JSON"
    if "uuid" in s:
        return "UUID"
    if "bytea" in s:
        return "BLOB"
    if "char" in s or "text" in s:
        return "VARCHAR"
    return "TEXT"
