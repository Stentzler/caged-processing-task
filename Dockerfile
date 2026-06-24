FROM python:3.14-slim AS build-base

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

FROM build-base AS test

COPY tests ./tests
COPY sample ./sample

RUN uv sync --locked \
    && uv run pytest

FROM test AS builder

RUN uv sync --locked --no-dev --no-editable

FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

USER 10001

ENTRYPOINT ["python", "-m", "app"]
