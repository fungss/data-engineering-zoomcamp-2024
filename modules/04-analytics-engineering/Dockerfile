FROM python:3.10-slim-bullseye

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir
RUN python -m pip install --no-cache-dir dbt-core
RUN python -m pip install --no-cache-dir dbt-bigquery
RUN python -m pip install --no-cache-dir dbterd

# Set docker basics
WORKDIR /usr/app/
COPY ./dbt-docker/ny_taxi_analytics/ /usr/app/
# VOLUME /usr/app/
ENTRYPOINT ["dbt"]