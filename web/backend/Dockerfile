FROM python:3.12-slim

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

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

RUN rm -f uv.lock && uv lock && uv sync --no-dev
RUN uv pip install --no-deps -e .
