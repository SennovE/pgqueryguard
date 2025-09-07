format:
	uv run ruff format .
	uv run ruff check --fix .

check:
	uv run ruff check
	uv run pyrefly check

venv:
	uv sync --frozen --no-install-project --group=dev
.SILENT: db
db: 
	docker compose exec backend bash -lc "cd /db_population && uv run python init.py"