format:
	uv run ruff format .
	uv run ruff check --fix .

check:
	uv run ruff check
	uv run pyrefly check

venv:
	uv sync --frozen --no-install-project --group=dev