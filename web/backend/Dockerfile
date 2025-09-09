FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY . .
RUN uv pip install --no-deps -e .

CMD ["uv", "run", "python", "-m", "app.main"]
