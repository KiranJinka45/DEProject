.PHONY: help up down restart logs test dbt-run dbt-test clean

help:
	@echo "Available commands:"
	@echo "  make up         - Build and start all services via Docker Compose"
	@echo "  make down       - Stop and remove all running containers"
	@echo "  make logs       - Tail logs for all containers"
	@echo "  make test       - Run Python test suite with pytest"
	@echo "  make dbt-run    - Run all dbt staging and mart models"
	@echo "  make dbt-test   - Run all dbt tests (uniqueness, referential integrity)"
	@echo "  make clean      - Clean local temporary files"

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

test:
	python -m pytest tests/ -v

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf dbt/target dbt/dbt_packages .pytest_cache
