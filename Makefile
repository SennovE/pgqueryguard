.ONESHELL: run 
.SILENT: run
run:
	cd sql_files_examples
	docker compose down -v
	docker compose build
	docker compose up -d --remove-orphans

.SILENT: data
data:
	cd sql_files_examples && \
	docker compose up -d db backend && \
	docker compose exec backend bash -lc '\
	  pip install --no-cache-dir -q psycopg2-binary && \
	  cd /checker/db_population && \
	  python init.py \
	'
.ONESHELL: drop
.SILENT: drop
drop:
	cd sql_files_examples
	docker compose exec -T db bash -lc 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'


.ONESHELL:
.SILENT: check
check:
	cd sql_files_examples && \
	docker compose up -d db && \
	docker compose exec -T db bash -lc '\
	  echo "[whoami]"; psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT current_user, current_database();" && \
	  echo "[schemas]"; psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dn" && \
	  echo "[tables]";  psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dt" && \
	  echo "[actor sample]"; psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT * FROM actor ORDER BY actor_id LIMIT 5;"'


