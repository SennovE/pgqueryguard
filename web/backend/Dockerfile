FROM python:3.12-slim

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# ➜ добавили libpq5 (даёт libpq.so), можно без компилятора
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv tomli-w
WORKDIR /app

COPY web/backend/pyproject.toml ./pyproject.toml
COPY web/backend/uv.lock ./uv.lock
COPY checker ./vendor/pgqueryguard
COPY web/backend/app ./app

RUN python - <<'PY'
import tomllib, tomli_w, pathlib
p = pathlib.Path("pyproject.toml")
data = tomllib.loads(p.read_text(encoding="utf-8"))
tool = data.setdefault("tool", {})
uv = tool.setdefault("uv", {})
sources = uv.setdefault("sources", {})
sources["pgqueryguard"] = {"path": "vendor/pgqueryguard", "editable": True}
p.write_text(tomli_w.dumps(data), encoding="utf-8")
PY

# lock можно оставить как есть; если нужно — пересчитать под локальный источник
RUN uv lock --upgrade-package pgqueryguard

RUN uv sync --no-dev --frozen
RUN uv pip install --no-deps -e .

# (опционально) лёгкая проверка без импорта psycopg_binary:
# RUN python -c "import psycopg, psycopg.pq as pq; print('psycopg', psycopg.__version__)"
CMD ["uv", "run", "python", "-m", "app.main"]
