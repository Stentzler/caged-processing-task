.PHONY: install lint format test docker-build clean

IMAGE_NAME ?= caged-processing-task

install:
	uv sync --all-groups

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

format:
	uv run ruff check src tests --fix
	uv run ruff format src tests

test:
	uv run pytest

docker-build:
	docker build -t $(IMAGE_NAME):latest .

clean:
	rm -rf build dist .pytest_cache .ruff_cache src/*.egg-info
