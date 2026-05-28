# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip build \
    && python -m build --wheel --outdir /dist

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1
COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl \
    && rm -f /tmp/*.whl
# Non-root user (nobody = 65534) — see operations.md for hardening notes.
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["exec-rest-api"]
CMD ["--listen", "0.0.0.0:8080"]
