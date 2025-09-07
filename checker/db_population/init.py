import os, glob, time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


DB = {
    "host": os.getenv("PGHOST", "postgres"),
    "port": os.getenv("PGPORT", "5432"),
    "database": os.getenv("PGDATABASE", "appdb"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "secret"),
}

SQL_DIR = os.path.dirname(__file__)
files = sorted(glob.glob(os.path.join(SQL_DIR, "*.sql")))

for attempt in range(10):
    try:
        conn = psycopg2.connect(**DB)
        break
    except Exception as e:
        print(f"[wait-db] попытка {attempt+1}: {e}")
        time.sleep(3)
else:
    raise RuntimeError("Postgres не отвечает")

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

cur.execute("""
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
    CREATE ROLE postgres WITH LOGIN SUPERUSER;
  END IF;
END
$$;
""")

import io, re

COPY_RE = re.compile(r'COPY\s.+?\sFROM\s+stdin;?', re.IGNORECASE | re.DOTALL)

LINE_COMMENT_RE = re.compile(r'--.*?$', re.MULTILINE)
BLOCK_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)

def _strip_comments(sql: str) -> str:
    sql = LINE_COMMENT_RE.sub('', sql)
    sql = BLOCK_COMMENT_RE.sub('', sql)
    return sql

def _is_executable(sql: str) -> bool:
    # убрать комменты и пробелы
    s = _strip_comments(sql).strip()
    # выбросить одиночные ';'
    s = s.strip(';').strip()
    return bool(s)

def run_sql_file(path):
    with open(path, "r", encoding="utf-8-sig") as f:  # utf-8-sig на случай BOM
        buf = f.read()

    pos = 0
    while True:
        m = COPY_RE.search(buf, pos)
        if not m:
            tail = buf[pos:]
            if _is_executable(tail):
                cur.execute(_strip_comments(tail))
            break

        # выполнить SQL до COPY (если есть)
        pre = buf[pos:m.start()]
        if _is_executable(pre):
            cur.execute(_strip_comments(pre))

        # --- обработка COPY ... FROM stdin ---
        data_start = m.end()

        # найти строку-терминатор "\." (учитываем LF/CRLF и возможные пробелы)
        term = re.search(r'(?m)^[ \t]*\\\.[ \t]*\r?\n?', buf[data_start:])
        if not term:
            raise RuntimeError("COPY terminator (\\.) not found")
        term_abs_start = data_start + term.start()

        # тело данных без завершающей строки "\."
        data = buf[data_start:term_abs_start]

        # убрать возможный одиночный перевод строки сразу после "FROM stdin;"
        if data.startswith('\r\n'):
            data = data[2:]
        elif data.startswith('\n'):
            data = data[1:]

        copy_cmd = m.group(0).replace("FROM stdin", "FROM STDIN").strip()
        cur.copy_expert(copy_cmd, io.StringIO(data))

        # перейти за terminator
        pos = term_abs_start + len(term.group(0))

files = sorted(glob.glob(os.path.join(SQL_DIR, "*.sql")))
print("[init] files:", [os.path.basename(f) for f in files])
for f in files:
    print(f"[init] run", os.path.basename(f))
    run_sql_file(f)

cur.close(); conn.close()
print("[init] done")