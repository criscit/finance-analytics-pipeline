# syntax=docker/dockerfile:1

############################
# Stage 1: export requirements from poetry.lock
############################
FROM python:3.11-slim AS builder
ENV POETRY_VERSION=2.1.4 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"
# Install poetry export plugin for Poetry 2.x
RUN poetry self add poetry-plugin-export
WORKDIR /app
# Copy only lock metadata first -> great caching
COPY pyproject.toml poetry.lock ./
# Export pinned requirements (main deps only)
RUN poetry export -f requirements.txt --output /tmp/requirements.txt --without-hashes --only main

############################
# Stage 2: runtime image installs via pip from exported file
############################
FROM python:3.11-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create DAGSTER_HOME directory
RUN mkdir -p /app/dagster_home

# Install python deps
COPY --from=builder /tmp/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# App code last to keep deps cached on code changes
COPY . .

ENV PYTHONUNBUFFERED=1
