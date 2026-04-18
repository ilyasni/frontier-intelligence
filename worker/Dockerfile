FROM python:3.11-slim

WORKDIR /app

# libgomp — для onnxruntime / fastembed (BM25 sparse в Qdrant)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# fastembed + onnxruntime — hybrid search (dense + sparse); см. shared/qdrant_sparse.py
RUN pip install --no-cache-dir --timeout 300 \
    sqlalchemy[asyncio]==2.0.30 \
    asyncpg==0.29.0 \
    redis==5.0.4 \
    prometheus-client==0.20.0 \
    pydantic==2.7.1 \
    pydantic-settings==2.3.1 \
    pyyaml==6.0.1 \
    httpx==0.27.0 \
    openai==1.54.4 \
    boto3==1.34.131 \
    qdrant-client==1.12.0 \
    neo4j==5.23.1 \
    ruptures==1.1.9 \
    "fastembed>=0.3.6" \
    "onnxruntime>=1.17.0"

COPY shared/ /app/shared/
COPY worker/ /app/worker/
RUN mkdir -p /app/scripts
COPY scripts/enqueue_reindex_enriched_posts.py /app/scripts/enqueue_reindex_enriched_posts.py
ENV PYTHONPATH=/app

CMD ["python", "-m", "worker.main"]
