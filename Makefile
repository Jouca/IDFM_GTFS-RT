restart:
	docker compose up -d

down:
	docker compose down -v

clean:
	docker images --filter "label=project=gtfs-rt" --filter "dangling=true" -q | xargs --no-run-if-empty docker rmi

deploy:
	docker compose up -d --build
	$(MAKE) clean
