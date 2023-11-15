SHELL := /bin/bash

.PHONY: setup
setup: venv lint test-unit

.PHONY: clean-pyc
clean-pyc:
	find . -name '*.pyc' -exec rm -rf {} +
	find . -name '*.pyo' -exec rm -rf {} +
	find . -name '*~' -exec rm -rf {} +
	find . -name '__pycache__' -exec rm -rf {} +

.PHONY: clean-test
clean-test:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf .reports
	rm -rf htmlcov/
	rm -rf .pytest_cache

.PHONY: clean
clean: clean-pyc clean-test

venv: poetry.lock
	poetry install

.PHONY: format
format: venv
	poetry run black nodestream tests
	poetry run isort nodestream tests

.PHONY: lint
lint: venv
	poetry run black nodestream tests --check
	poetry run ruff nodestream tests

.PHONY: test-unit
test-unit: venv
	poetry run pytest -m "not e2e"

.PHONY: test-e2e
test-e2e: venv
	poetry run pytest -m "e2e"

.PHONY: docs-build
docs-build: venv
	poetry run mkdocs build

.PHONY: docs-server
docs-server: venv
	poetry run mkdocs serve

.PHONY: snapshot
snapshot: venv
	poetry run pytest --snapshot-update
