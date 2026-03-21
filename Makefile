.PHONY: help install test lint format \
        deploy-dags terraform-init terraform-plan terraform-apply \
        setup-snowflake setup-iceberg run-compaction expire-snapshots \
        docker-up docker-down

PYTHON         := python3.11
VENV           := .venv
ENV            ?= dev
AIRFLOW_HOME   := ./airflow
AIRFLOW_BUCKET ?= restaurant-airflow-$(ENV)

## ─── Help ────────────────────────────────────────────────────────────────────

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\n\033[1mUsage:\033[0m\n  make \033[36m<target>\033[0m [ENV=dev|staging|prod]\n\n\033[1mTargets:\033[0m\n"} \
	/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

## ─── Setup ───────────────────────────────────────────────────────────────────

install: ## Create venv and install all dependencies (including dev)
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"
	$(VENV)/bin/pre-commit install
	@echo "✅  Environment ready. Activate with: source $(VENV)/bin/activate"

## ─── Testing ─────────────────────────────────────────────────────────────────

test: ## Run all tests with coverage
	$(VENV)/bin/pytest airflow/tests/ tests/ -v \
		--cov=airflow --cov-report=html --cov-report=term-missing

test-dags: ## Test DAG integrity only
	$(VENV)/bin/pytest airflow/tests/dags/ -v -k "integrity"

test-integration: ## Run integration tests (requires ENV credentials)
	$(VENV)/bin/pytest tests/integration/ -v --env=$(ENV)

test-security: ## Run IAM policy tests
	$(VENV)/bin/pytest tests/security/ -v

## ─── Code Quality ────────────────────────────────────────────────────────────

lint: ## Run all linters
	$(VENV)/bin/ruff check airflow/ tests/
	$(VENV)/bin/black --check airflow/ tests/
	$(VENV)/bin/mypy airflow/

format: ## Auto-format code
	$(VENV)/bin/black airflow/ tests/
	$(VENV)/bin/ruff check --fix airflow/ tests/

## ─── Airflow ─────────────────────────────────────────────────────────────────

deploy-dags: ## Deploy DAGs to MWAA/Astronomer S3 bucket (ENV=dev|staging|prod)
	@echo "🚀  Deploying DAGs to $(ENV) (s3://$(AIRFLOW_BUCKET))..."
	aws s3 sync airflow/dags/ s3://$(AIRFLOW_BUCKET)/dags/ --delete
	aws s3 sync airflow/plugins/ s3://$(AIRFLOW_BUCKET)/plugins/ --delete
	@echo "✅  DAGs deployed."

docker-up: ## Start local Airflow via Docker Compose
	docker compose -f docker-compose.airflow.yml up -d
	@echo "🌐  Airflow UI → http://localhost:8080 (admin / admin)"

docker-down: ## Stop local Airflow
	docker compose -f docker-compose.airflow.yml down

## ─── Terraform ───────────────────────────────────────────────────────────────

terraform-init: ## Initialize Terraform for ENV
	cd infrastructure/terraform/environments/$(ENV) && terraform init

terraform-plan: ## Plan Terraform changes for ENV
	cd infrastructure/terraform/environments/$(ENV) && terraform plan

terraform-apply: ## Apply Terraform changes for ENV (requires confirmation)
	cd infrastructure/terraform/environments/$(ENV) && terraform apply

terraform-destroy: ## Destroy all resources for ENV (DANGER!)
	cd infrastructure/terraform/environments/$(ENV) && terraform destroy

## ─── Snowflake & Iceberg ─────────────────────────────────────────────────────

setup-snowflake: ## Initialize Snowflake schemas, roles, and policies
	snowsql -f snowflake/schemas/bronze/setup.sql
	snowsql -f snowflake/schemas/silver/setup.sql
	snowsql -f snowflake/schemas/gold/setup.sql
	snowsql -f snowflake/security/row_access_policies.sql
	snowsql -f snowflake/security/masking_policies.sql
	snowsql -f snowflake/security/role_hierarchy.sql

setup-iceberg: ## Initialize Iceberg tables in Glue Catalog
	$(VENV)/bin/python scripts/setup/init_iceberg.py

run-compaction: ## Compact Iceberg table (TABLE=<name>)
	$(VENV)/bin/python iceberg/maintenance/compaction_job.py --table $(TABLE)

expire-snapshots: ## Expire Iceberg snapshots older than N days (DAYS=7)
	$(VENV)/bin/python iceberg/maintenance/expire_snapshots.py --older-than-days $(or $(DAYS),7)
