TEST_PATH=tests/
MIN_COVERAGE=80

.PHONY: install
install:
	poetry install

.PHONY: test
test:
	poetry run python -m pytest ${TEST_PATH}

.PHONY: lint
lint:
	poetry run flake8 --exclude=.venv .
	poetry run black --check .

.PHONY: fmt
fmt:
	poetry run black .

.PHONY: coverage
coverage:
	poetry run pytest --cov=table_refresh --cov-fail-under=$(MIN_COVERAGE) $(TEST_PATH)
	poetry run coverage xml
