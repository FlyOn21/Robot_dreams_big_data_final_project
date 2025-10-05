.DEFAULT_GOAL := help

.PHONY: help fp-demon-up fp-up fp-down fp-clean kafka-create-topics kafka-delete-topics kafka-list-topics kafka-logs kafka-status kafka-shell tools-check execute-crime-listener execute-arrest-listener execute-all-listeners stop-listeners execute-producer execute-producer-large ruff-check ruff-fix


help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

fp-demon-up: ## Start Final Project cluster in daemon mode (background)
	docker compose -f ${PWD}/docker-compose.yaml up --build -d

fp-up: ## Start Final Project cluster in foreground
	docker compose -f ${PWD}/docker-compose.yaml up --build

fp-down: ## Stop Final Project cluster
	docker compose -f ${PWD}/docker-compose.yaml down

fp-clean: ## Stop Final Project cluster and remove volumes
	docker compose -f ${PWD}/docker-compose.yaml down -v

kafka-create-topics: ## Create sample topics in Kafka cluster
	docker exec -it kafka1 kafka-topics --create --topic crimes --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1
	docker exec -it kafka1 kafka-topics --create --topic arrests --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1

kafka-delete-topics: ## Delete sample topics in Kafka cluster
	docker exec -it kafka1 kafka-topics --delete --topic crimes --bootstrap-server 127.0.0.1:9092
	docker exec -it kafka1 kafka-topics --delete --topic arrests --bootstrap-server 127.0.0.1:9092

kafka-list-topics: ## List topics in Kafka cluster
	docker exec -it kafka1 kafka-topics --list --bootstrap-server 127.0.0.1:9092

kafka-logs: ## Show Kafka broker logs
	docker logs -f kafka1

kafka-status: ## Show status of all Kafka services
	docker compose -f ${PWD}/docker-compose-kafka.yaml ps

kafka-shell: ## Access Kafka container shell
	docker exec -it kafka1 bash

tools-check: ## Verify host has curl and jq installed
	@bash -c 'command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found"; exit 1; }'
	@bash -c 'command -v jq   >/dev/null 2>&1 || { echo "ERROR: jq not found"; exit 1; }'


######### Starter tasks ##########
execute-crime-listener: ## Run the crime stream listener
	spark-submit \
		--master local[*] \
		--driver-memory 4g \
		--executor-memory 4g \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
		--jars $(POSTGRES_JAR) \
		--conf spark.sql.shuffle.partitions=200 \
		--conf spark.streaming.stopGracefullyOnShutdown=true \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.sql.streaming.stateStore.compression.codec=lz4 \
		${PWD}/spark/silver/spark_stream_crime_processing.py

execute-arrest-listener: ## Run the arrest stream listener
	spark-submit \
		--master local[*] \
		--driver-memory 4g \
		--executor-memory 4g \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
		--jars $(POSTGRES_JAR) \
		--conf spark.sql.shuffle.partitions=200 \
		--conf spark.streaming.stopGracefullyOnShutdown=true \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.sql.streaming.stateStore.compression.codec=lz4 \
		${PWD}/spark/silver/spark_stream_arrests_processing.py
execute-all-listeners: ## Run all stream listeners in background
	@echo "Starting crime listener..."
	@nohup make execute-crime-listener > logs/crime_listener.log 2>&1 & echo $$! > logs/crime_listener.pid
	@sleep 3
	@echo "Starting arrest listener..."
	@nohup make execute-arrest-listener > logs/arrest_listener.log 2>&1 & echo $$! > logs/arrest_listener.pid
	@echo "All listeners started. Check logs/ directory for output."
	@echo "To stop: make stop-listeners"

stop-listeners: ## Stop all running listeners
	@echo "Stopping all listeners..."
	@if [ -f logs/crime_listener.pid ]; then kill $$(cat logs/crime_listener.pid) 2>/dev/null || true; rm logs/crime_listener.pid; fi
	@if [ -f logs/arrest_listener.pid ]; then kill $$(cat logs/arrest_listener.pid) 2>/dev/null || true; rm logs/arrest_listener.pid; fi
	@pkill -f "spark_stream_crime_processing.py" 2>/dev/null || true
	@pkill -f "spark_stream_arrests_processing.py" 2>/dev/null || true
	@echo "All listeners stopped."

execute-producer: ## Run the data producer
	python3 ${PWD}/faker_generator/generate_messages.py --stream kafka --size-mb 10 \
		--iucr-csv /home/flyon21/PycharmProjects/BD_robot_dreams_final_project/source_data/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting_\(IUCR\)_Codes_20250928.csv

execute-producer-large: ## Run the data producer with 50MB
	python3 ${PWD}/faker_generator/generate_messages.py --stream kafka --size-mb 50 \
		--iucr-csv /home/flyon21/PycharmProjects/BD_robot_dreams_final_project/source_data/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting_\(IUCR\)_Codes_20250928.csv

#################################
#RUFF

ruff-check: ## Run ruff linter
	ruff check .

ruff-fix: ## Run ruff linter with auto-fix
	ruff check . --fix