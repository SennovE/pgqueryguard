.SILENT: run
run:
	docker compose down -v
	docker compose build 
	docker compose up -d --remove-orphans 
	echo "Docs: http://localhost:8000/api/v1/swagger"
