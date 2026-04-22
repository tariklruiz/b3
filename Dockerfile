# Dockerfile
# ============================================================================
# Guia FII — FastAPI backend (Postgres edition)
#
# Reads data from Railway Postgres via DATABASE_URL env var. No SQLite files,
# no Dropbox downloads, no fund_types.json file — all data lives in Postgres.
# ============================================================================

FROM python:3.11-slim

# libpq-dev is needed if we ever swap to psycopg2 from source; psycopg2-binary
# has its own libpq statically linked so strictly not required, but costs
# nothing and covers us if we rebuild from source later.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first so Docker caches the layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY db.py .

# Railway sets $PORT automatically; default to 8000 for local runs
ENV PORT=8000
EXPOSE 8000

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
