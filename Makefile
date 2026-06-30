DOCKER_COMPOSE = docker compose -f infra/docker-compose.yml --env-file .env
EXEC_SCHEDULER = $(DOCKER_COMPOSE) exec -w /opt/airflow/dbt airflow-scheduler

.PHONY: up down restart logs build dbt-seed dbt-run dbt-test dbt-all dbt-docs dag-trigger

# --- DOCKER COMMANDS ---

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

restart:
	$(DOCKER_COMPOSE) restart

logs:
	$(DOCKER_COMPOSE) logs -f

build:
	$(DOCKER_COMPOSE) up -d --build

# --- DBT COMMANDS ---

dbt-seed:
	$(EXEC_SCHEDULER) dbt seed

dbt-run:
	$(EXEC_SCHEDULER) dbt run

dbt-test:
	$(EXEC_SCHEDULER) dbt test

dbt-all:
	$(EXEC_SCHEDULER) /bin/bash -c "dbt seed && dbt run && dbt test"

dbt-docs:
	$(EXEC_SCHEDULER) dbt docs generate

dbt-docs-server:
	python -m http.server 8081 --directory dbt/target

# --- AIRFLOW COMMANDS ---

dag-trigger:
	$(DOCKER_COMPOSE) exec airflow-scheduler airflow dags trigger nyc_taxi_elt_pipeline