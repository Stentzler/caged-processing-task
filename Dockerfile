FROM python:3.14-alpine AS build-base

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

RUN apk add --no-cache ca-certificates git

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

FROM python:3.14-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

USER 10001

ENTRYPOINT ["python", "-m", "app"]
