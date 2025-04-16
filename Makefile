.PHONY: test run build

test:
	poetry run pytest tests/

run:
	poetry run python src/pipeline.py

build:
	poetry build