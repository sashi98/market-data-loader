# -----------------------------------------------------------
# market-data-loader -- Dockerfile
#
# Runs the long-lived listeners (indicators_listener.py,
# corporate_actions_listener.py, stock_universe_update_listener.py) as
# standalone background processes -- direct-to-Postgres, bypassing tmt's
# REST API entirely (see README.md). NOT a web server: no EXPOSE, no
# HTTP healthcheck -- each listener's own poll-cycle log output is the
# health signal.
#
# IMPORTANT -- directory shape:
# core/env_validator.py and core/logging_setup.py both resolve the
# shared config/.env and logs/market-data-loader/ as FOUR PARENT
# DIRECTORIES UP from their own file path, i.e. they expect to be laid
# out on disk as <root>/app/market-data-loader (this repo), with
# <root>/config/.env and <root>/logs/ as siblings of app/. WORKDIR below
# reproduces that exact shape inside the image; docker-compose.yml
# mounts config/.env, logs/market-data-loader, and data/market-data-loader
# at the matching paths so the unmodified code just works.
#
# Which listener actually runs is chosen per-container via
# docker-compose's `command:` override -- this one image is shared by
# all three listener containers (indicators / corporate-actions /
# stock-universe), each overriding `command:` to run a different script.
#
# Build:  docker build -t tmt-market-data-loader .
# Run:    docker run tmt-market-data-loader python indicators_listener.py
# -----------------------------------------------------------

FROM python:3.11-slim

# gcc -- psycopg2-binary ships wheels, but pandas/nse/bse's own
# transitive deps occasionally need a compiler
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r tmt && useradd -r -g tmt tmt

# Reproduce the <root>/app/market-data-loader shape env_validator.py and
# logging_setup.py expect (4 parents up from core/*.py -> <root>)
WORKDIR /opt/tmt/app/market-data-loader

# Install dependencies first for layer caching -- only re-runs when
# requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Non-root ownership of the whole /opt/tmt tree, not just WORKDIR --
# config/.env and logs/ get mounted as siblings two levels up and the
# tmt user needs to read/write them
RUN mkdir -p /opt/tmt/config /opt/tmt/logs/market-data-loader /opt/tmt/data/market-data-loader \
    && chown -R tmt:tmt /opt/tmt
USER tmt

# No CMD -- docker-compose.yml's `command:` picks the listener per
# container. None of these serve HTTP, so no EXPOSE/HEALTHCHECK either.
