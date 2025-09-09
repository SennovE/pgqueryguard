.SILENT: run
run:
	docker compose down -v
	docker compose build --no-cache backend
	docker compose up -d --remove-orphans 
	echo "Docs: http://localhost:8080/api/v1/swagger"


.SILENT: docker_clear
docker_clear:  ##@Clear all docker files
	- docker stop $$(docker ps -qa)
	docker rm $$(docker ps -a -qa)
	docker rmi $$(docker images -a -qa)
	docker volume rm $$(docker volume ls -qa)
	docker network prune -f