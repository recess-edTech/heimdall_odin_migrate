.PHONY: help install install-dev test test-cov lint format type-check security clean build docs serve-docs
.DEFAULT_GOAL := help


PYTHON := python3
PIP := pip
VENV := venv
PROJECT_NAME := heimdall-odin-migrator

help: 
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?

venv: 
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created. Activate with: source $(VENV)/bin/activate"

install: venv 
	$(VENV)/bin/$(PIP) install --upgrade pip
	$(VENV)/bin/$(PIP) install -e .

install-dev: venv 
	$(VENV)/bin/$(PIP) install --upgrade pip
	$(VENV)/bin/$(PIP) install -e ".[dev,test,docs]"

install-pre-commit: install-dev 
	$(VENV)/bin/pre-commit install


setup-env: 
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env file created from template. Please edit with your database credentials."; \
	else \
		echo ".env file already exists."; \
	fi

test: 
	$(VENV)/bin/pytest tests/ -v

test-cov: 
	$(VENV)/bin/pytest tests/ -v --cov=migrator --cov-report=html --cov-report=term-missing

test-integration: 
	$(VENV)/bin/pytest tests/ -v -m integration

test-unit: 
	$(VENV)/bin/pytest tests/ -v -m "not integration"


lint: 
	$(VENV)/bin/flake8 migrator/ tests/
	$(VENV)/bin/pydocstyle migrator/

format: 
	$(VENV)/bin/black migrator/ tests/
	$(VENV)/bin/isort migrator/ tests/

format-check: 
	$(VENV)/bin/black --check migrator/ tests/
	$(VENV)/bin/isort --check-only migrator/ tests/

type-check: 
	$(VENV)/bin/mypy migrator/

security: 
	$(VENV)/bin/bandit -r migrator/ -ll
	$(VENV)/bin/safety check

pre-commit: 
	$(VENV)/bin/pre-commit run --all-files


analyze: 
	$(VENV)/bin/python -m migrator.analyze_schemas

migrate-dry: 
	$(VENV)/bin/python -m migrator.migrate --dry-run --verbose

migrate: 
	$(VENV)/bin/python -m migrator.migrate --verbose

validate-config: 
	$(VENV)/bin/python -c "from migrator.config import *; print('Configuration is valid')"


clean: 
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

build: clean 
	$(VENV)/bin/python -m pip install --upgrade build
	$(VENV)/bin/python -m build

upload-test: build 
	$(VENV)/bin/python -m pip install --upgrade twine
	$(VENV)/bin/python -m twine upload --repository testpypi dist/*

upload: build 
	$(VENV)/bin/python -m pip install --upgrade twine
	$(VENV)/bin/python -m twine upload dist/*


docs: 
	$(VENV)/bin/sphinx-build -b html docs/ docs/_build/html

docs-clean: 
	rm -rf docs/_build/

docs-serve: docs 
	@echo "Documentation available at http://localhost:8000"
	$(VENV)/bin/python -m http.server 8000 -d docs/_build/html


db-setup: 
	@echo "Starting PostgreSQL container for testing..."
	docker run --name migration-test-db -e POSTGRES_PASSWORD=test_password -e POSTGRES_USER=test_user -e POSTGRES_DB=test_db -p 5432:5432 -d postgres:15
	@echo "Waiting for database to be ready..."
	sleep 10

db-teardown: 
	docker stop migration-test-db || true
	docker rm migration-test-db || true


dev-setup: install-dev setup-env install-pre-commit 
	@echo "Development environment set up successfully!"
	@echo "Next steps:"
	@echo "1. Edit .env with your database credentials"
	@echo "2. Run 'make validate-config' to test configuration"
	@echo "3. Run 'make analyze' to analyze database schemas"
	@echo "4. Run 'make migrate-dry' to test migration"

check-all: format-check lint type-check security test 


git-clean: 
	git clean -fd
	git reset --hard HEAD


logs: 
	@echo "Recent migration logs:"
	@ls -la migration_*.log 2>/dev/null | tail -5 || echo "No migration logs found"

tail-logs: 
	@latest_log=$$(ls -t migration_*.log 2>/dev/null | head -1); \
	if [ -n "$$latest_log" ]; then \
		echo "Tailing $$latest_log"; \
		tail -f "$$latest_log"; \
	else \
		echo "No migration logs found"; \
	fi


backup-schemas: 
	@echo "Backing up database schemas..."
	@mkdir -p backups/schemas
	$(VENV)/bin/python -m migrator.analyze_schemas
	@mv *_schema_*.json backups/schemas/ 2>/dev/null || echo "No schema files to move"


docker-build: 
	docker build -t $(PROJECT_NAME) .

docker-run: 
	docker run --env-file .env -it $(PROJECT_NAME)


benchmark: 
	$(VENV)/bin/python -m pytest benchmarks/ -v --benchmark-only


version: 
	@grep version pyproject.toml | head -1 | cut -d'"' -f2

bump-patch: 
	$(VENV)/bin/bump2version patch

bump-minor: 
	$(VENV)/bin/bump2version minor

bump-major: 
	$(VENV)/bin/bump2version major
