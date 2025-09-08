FROM python:3.12-alpine

RUN apk add --no-cache curl ca-certificates \
 && update-ca-certificates

WORKDIR /app

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock* ./

RUN uv sync --frozen --no-dev --no-install-project

COPY ./app ./app

CMD ["uv", "run", "python", "-m", "app.main"]
