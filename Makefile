.PHONY: help install install-dev test test-cov lint format type-check security clean build docs serve-docs
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip
VENV := venv
PROJECT_NAME := heimdall-odin-migrator

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Environment setup
venv: ## Create virtual environment
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created. Activate with: source $(VENV)/bin/activate"

install: venv ## Install production dependencies
	$(VENV)/bin/$(PIP) install --upgrade pip
	$(VENV)/bin/$(PIP) install -e .

install-dev: venv ## Install development dependencies
	$(VENV)/bin/$(PIP) install --upgrade pip
	$(VENV)/bin/$(PIP) install -e ".[dev,test,docs]"

install-pre-commit: install-dev ## Install and set up pre-commit hooks
	$(VENV)/bin/pre-commit install

# Environment configuration
setup-env: ## Create .env file from template
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo ".env file created from template. Please edit with your database credentials."; \
	else \
		echo ".env file already exists."; \
	fi

# Testing
test: ## Run tests
	$(VENV)/bin/pytest tests/ -v

test-cov: ## Run tests with coverage
	$(VENV)/bin/pytest tests/ -v --cov=migrator --cov-report=html --cov-report=term-missing

test-integration: ## Run integration tests (requires database)
	$(VENV)/bin/pytest tests/ -v -m integration

test-unit: ## Run unit tests only
	$(VENV)/bin/pytest tests/ -v -m "not integration"

# Code quality
lint: ## Run linting
	$(VENV)/bin/flake8 migrator/ tests/
	$(VENV)/bin/pydocstyle migrator/

format: ## Format code with black and isort
	$(VENV)/bin/black migrator/ tests/
	$(VENV)/bin/isort migrator/ tests/

format-check: ## Check code formatting
	$(VENV)/bin/black --check migrator/ tests/
	$(VENV)/bin/isort --check-only migrator/ tests/

type-check: ## Run type checking with mypy
	$(VENV)/bin/mypy migrator/

security: ## Run security checks
	$(VENV)/bin/bandit -r migrator/ -ll
	$(VENV)/bin/safety check

pre-commit: ## Run all pre-commit hooks
	$(VENV)/bin/pre-commit run --all-files

# Migration commands
analyze: ## Analyze database schemas
	$(VENV)/bin/python -m migrator.analyze_schemas

migrate-dry: ## Run migration in dry-run mode
	$(VENV)/bin/python -m migrator.migrate --dry-run --verbose

migrate: ## Run actual migration
	$(VENV)/bin/python -m migrator.migrate --verbose

validate-config: ## Validate configuration
	$(VENV)/bin/python -c "from migrator.config import *; print('Configuration is valid')"

# Build and distribution
clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

build: clean ## Build distribution packages
	$(VENV)/bin/python -m pip install --upgrade build
	$(VENV)/bin/python -m build

upload-test: build ## Upload to test PyPI
	$(VENV)/bin/python -m pip install --upgrade twine
	$(VENV)/bin/python -m twine upload --repository testpypi dist/*

upload: build ## Upload to PyPI
	$(VENV)/bin/python -m pip install --upgrade twine
	$(VENV)/bin/python -m twine upload dist/*

# Documentation
docs: ## Build documentation
	$(VENV)/bin/sphinx-build -b html docs/ docs/_build/html

docs-clean: ## Clean documentation build
	rm -rf docs/_build/

docs-serve: docs ## Build and serve documentation locally
	@echo "Documentation available at http://localhost:8000"
	$(VENV)/bin/python -m http.server 8000 -d docs/_build/html

# Database operations
db-setup: ## Setup test databases (requires Docker)
	@echo "Starting PostgreSQL container for testing..."
	docker run --name migration-test-db -e POSTGRES_PASSWORD=test_password -e POSTGRES_USER=test_user -e POSTGRES_DB=test_db -p 5432:5432 -d postgres:15
	@echo "Waiting for database to be ready..."
	sleep 10

db-teardown: ## Stop and remove test database
	docker stop migration-test-db || true
	docker rm migration-test-db || true

# Development workflow
dev-setup: install-dev setup-env install-pre-commit ## Complete development setup
	@echo "Development environment set up successfully!"
	@echo "Next steps:"
	@echo "1. Edit .env with your database credentials"
	@echo "2. Run 'make validate-config' to test configuration"
	@echo "3. Run 'make analyze' to analyze database schemas"
	@echo "4. Run 'make migrate-dry' to test migration"

check-all: format-check lint type-check security test ## Run all checks

# Git operations
git-clean: ## Clean git repository
	git clean -fd
	git reset --hard HEAD

# Monitoring and logs
logs: ## Show recent migration logs
	@echo "Recent migration logs:"
	@ls -la migration_*.log 2>/dev/null | tail -5 || echo "No migration logs found"

tail-logs: ## Tail the latest migration log
	@latest_log=$$(ls -t migration_*.log 2>/dev/null | head -1); \
	if [ -n "$$latest_log" ]; then \
		echo "Tailing $$latest_log"; \
		tail -f "$$latest_log"; \
	else \
		echo "No migration logs found"; \
	fi

# Backup operations
backup-schemas: ## Backup database schemas
	@echo "Backing up database schemas..."
	@mkdir -p backups/schemas
	$(VENV)/bin/python -m migrator.analyze_schemas
	@mv *_schema_*.json backups/schemas/ 2>/dev/null || echo "No schema files to move"

# Docker operations
docker-build: ## Build Docker image
	docker build -t $(PROJECT_NAME) .

docker-run: ## Run in Docker container
	docker run --env-file .env -it $(PROJECT_NAME)

# Performance testing
benchmark: ## Run performance benchmarks
	$(VENV)/bin/python -m pytest benchmarks/ -v --benchmark-only

# Version management
version: ## Show current version
	@grep version pyproject.toml | head -1 | cut -d'"' -f2

bump-patch: ## Bump patch version
	$(VENV)/bin/bump2version patch

bump-minor: ## Bump minor version
	$(VENV)/bin/bump2version minor

bump-major: ## Bump major version
	$(VENV)/bin/bump2version major
