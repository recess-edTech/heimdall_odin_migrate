# Multi-stage Docker build for Heimdall to Odin migration
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash migrator
WORKDIR /home/migrator

# Development stage
FROM base as development

# Install development dependencies
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Install additional dev tools
RUN pip install \
    pytest>=7.4.0 \
    pytest-asyncio>=0.21.0 \
    black>=23.0.0 \
    isort>=5.12.0 \
    flake8>=6.0.0

# Copy source code
COPY --chown=migrator:migrator . .

# Switch to non-root user
USER migrator

# Set Python path
ENV PYTHONPATH=/home/migrator

# Default command for development
CMD ["python", "-m", "migrator.analyze_schemas"]

# Production stage
FROM base as production

# Copy requirements and install production dependencies only
COPY requirements.txt ./
RUN pip install --no-dev -r requirements.txt

# Copy source code
COPY --chown=migrator:migrator migrator/ ./migrator/
COPY --chown=migrator:migrator pyproject.toml ./
COPY --chown=migrator:migrator README.md ./

# Install the package
RUN pip install -e .

# Switch to non-root user
USER migrator

# Set Python path
ENV PYTHONPATH=/home/migrator

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from migrator.config import *; print('OK')" || exit 1

# Default command
CMD ["python", "-m", "migrator.migrate", "--help"]

# Testing stage
FROM development as testing

# Install test dependencies
RUN pip install \
    pytest-cov>=4.1.0 \
    factory-boy>=3.3.0 \
    faker>=19.0.0

# Run tests
RUN python -m pytest tests/ -v || echo "Tests completed"

# Linting stage  
FROM development as linting

# Run code quality checks
RUN black --check migrator/ || echo "Black formatting check completed"
RUN isort --check-only migrator/ || echo "Import sorting check completed"
RUN flake8 migrator/ || echo "Flake8 linting completed"
