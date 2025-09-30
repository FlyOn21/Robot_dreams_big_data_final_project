.DEFAULT_GOAL := help

.PHONY: help fp-demon-up fp-up fp-down fp-clean kafka-create-topics kafka-list-topics \
		kafka-produce-transactions kafka-consume-transactions \
		kafka-produce-user_activity kafka-consume-user_activity \
		kafka-logs kafka-status kafka-shell \
		tools-check \
		schema-wait schema-register schema-list schema-get-latest \
		schema-compat-backward schema-compat-full schema-delete-subject \
		execute-listener execute-producer

SR_URL            ?= http://127.0.0.1:8081
TX_SUBJECT        ?= transaction-value
UA_SUBJECT        ?= user-activity-value
SCHEMAS_DIR       ?= schemas

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
	docker exec -it kafka1 kafka-topics --create --topic transactions --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1
	docker exec -it kafka1 kafka-topics --create --topic user_activity --bootstrap-server 127.0.0.1:9092 --partitions 3 --replication-factor 1

kafka-list-topics: ## List topics in Kafka cluster
	docker exec -it kafka1 kafka-topics --list --bootstrap-server 127.0.0.1:9092

kafka-produce-transactions: ## Produce messages to transactions
	docker exec -it kafka1 kafka-console-producer --topic transactions --bootstrap-server 127.0.0.1:9092

kafka-consume-transactions: ## Consume messages from transactions
	docker exec -it kafka1 kafka-console-consumer --topic transactions --from-beginning --bootstrap-server 127.0.0.1:9092

kafka-produce-user_activity: ## Produce messages to user_activity
	docker exec -it kafka1 kafka-console-producer --topic user_activity --bootstrap-server 127.0.0.1:9092

kafka-consume-user_activity: ## Consume messages from user_activity
	docker exec -it kafka1 kafka-console-consumer --topic user_activity --from-beginning --bootstrap-server 127.0.0.1:9092

kafka-logs: ## Show Kafka broker logs
	docker logs -f kafka1

kafka-status: ## Show status of all Kafka services
	docker compose -f ${PWD}/docker-compose-kafka.yaml ps

kafka-shell: ## Access Kafka container shell
	docker exec -it kafka1 bash

tools-check: ## Verify host has curl and jq installed
	@bash -c 'command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found"; exit 1; }'
	@bash -c 'command -v jq   >/dev/null 2>&1 || { echo "ERROR: jq not found"; exit 1; }'


########## Schema Registry tasks ##########
schema-wait: ## Wait until Schema Registry is reachable
	@bash -c 'echo "Waiting for Schema Registry at $(SR_URL) ..."; \
	until curl -sf "$(SR_URL)/subjects" >/dev/null; do sleep 2; done; \
	echo "Schema Registry is up."'

schema-register: tools-check schema-wait ## Register JSON Schemas (transaction & user_activity)
	@bash -c 'jq -e . "${PWD}/$(SCHEMAS_DIR)/transaction.json" >/dev/null || { echo "Invalid JSON: $(SCHEMAS_DIR)/transaction.json"; exit 1; }'
	@bash -c 'jq -e . "${PWD}/$(SCHEMAS_DIR)/user_activity.json" >/dev/null || { echo "Invalid JSON: $(SCHEMAS_DIR)/user_activity.json"; exit 1; }'
	@echo "Registering: $(TX_SUBJECT)"
	@curl -s -X POST "$(SR_URL)/subjects/$(TX_SUBJECT)/versions" \
	  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	  -d "$$(jq -nc --arg schema "$$(jq -c . ${PWD}/$(SCHEMAS_DIR)/transaction.json)" --arg type JSON \
	        '{schema: $$schema, schemaType: $$type}')" | jq .
	@echo "Registering: $(UA_SUBJECT)"
	@curl -s -X POST "$(SR_URL)/subjects/$(UA_SUBJECT)/versions" \
	  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	  -d "$$(jq -nc --arg schema "$$(jq -c . ${PWD}/$(SCHEMAS_DIR)/user_activity.json)" --arg type JSON \
	        '{schema: $$schema, schemaType: $$type}')" | jq .
	@echo "Done."

schema-list: tools-check schema-wait ## List subjects in Schema Registry
	@curl -s "$(SR_URL)/subjects" | jq .

schema-get-latest: tools-check schema-wait ## Show latest versions of both subjects
	@echo "Latest $(TX_SUBJECT):"; \
	curl -s "$(SR_URL)/subjects/$(TX_SUBJECT)/versions/latest" | jq .
	@echo "Latest $(UA_SUBJECT):"; \
	curl -s "$(SR_URL)/subjects/$(UA_SUBJECT)/versions/latest" | jq .

schema-compat-backward: tools-check schema-wait ## Set BACKWARD compatibility on both subjects
	@for s in "$(TX_SUBJECT)" "$(UA_SUBJECT)"; do \
	  echo "Setting BACKWARD on $$s"; \
	  curl -s -X PUT "$(SR_URL)/config/$$s" \
	    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	    -d '{"compatibility":"BACKWARD"}' | jq .; \
	done

schema-compat-full: tools-check schema-wait ## Set FULL compatibility on both subjects
	@for s in "$(TX_SUBJECT)" "$(UA_SUBJECT)"; do \
	  echo "Setting FULL on $$s"; \
	  curl -s -X PUT "$(SR_URL)/config/$$s" \
	    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	    -d '{"compatibility":"FULL"}' | jq .; \
	done

schema-delete-subject: tools-check schema-wait ## Delete subjects (USE WITH CARE): make schema-delete-subject S=transaction-value
	@[ -n "$$S" ] || { echo "Usage: make schema-delete-subject S=<subject>"; exit 1; }
	@curl -s -X DELETE "$(SR_URL)/subjects/$$S?permanent=true" | jq .


######### Starter tasks ##########
execute-listener: ## Run the application
	spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
	${PWD}/spark_stream/spark_structure_strim.py

execute-producer: ## Run the data producer
	python3 ${PWD}/faker_generator/generate_messages.py