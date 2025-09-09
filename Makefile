test_db:
	cd .\db_population\ && docker compose up -d

.SILENT: run
COMPOSE := docker compose --project-directory web -f web/Docker-compose.yml
run:
	$(COMPOSE) down -v
	$(COMPOSE) build --no-cache backend
	$(COMPOSE) up -d --remove-orphans
	@echo "Docs: http://localhost:8080/api/v1/swagger"