FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

ARG PIP_INDEX_URL=

# Совпадает с worker по qdrant/fastembed + FastAPI для MCP
RUN if [ -n "$PIP_INDEX_URL" ]; then \
      pip install --no-cache-dir --timeout=7200 --retries=40 -i "$PIP_INDEX_URL" \
        fastapi==0.115.6 \
        "uvicorn[standard]==0.32.1" \
        prometheus-client==0.20.0 \
        sqlalchemy[asyncio]==2.0.30 \
        asyncpg==0.29.0 \
        redis==5.0.4 \
        pydantic==2.7.1 \
        pydantic-settings==2.3.1 \
        httpx==0.27.0 \
        openai==1.54.4 \
        qdrant-client==1.12.0 \
        neo4j==5.23.1 \
        fastembed>=0.3.6 \
        onnxruntime>=1.17.0; \
    else \
      pip install --no-cache-dir --timeout=7200 --retries=40 \
        fastapi==0.115.6 \
        "uvicorn[standard]==0.32.1" \
        prometheus-client==0.20.0 \
        sqlalchemy[asyncio]==2.0.30 \
        asyncpg==0.29.0 \
        redis==5.0.4 \
        pydantic==2.7.1 \
        pydantic-settings==2.3.1 \
        httpx==0.27.0 \
        openai==1.54.4 \
        qdrant-client==1.12.0 \
        neo4j==5.23.1 \
        fastembed>=0.3.6 \
        onnxruntime>=1.17.0; \
    fi

COPY shared/ /app/shared/
COPY worker/ /app/worker/
COPY mcp/ /app/mcp/

ENV PYTHONPATH=/app

EXPOSE 8100
EXPOSE 8102

# MCP_PORT из compose (по умолчанию 8100)
CMD ["sh", "-c", "uvicorn mcp.server:app --host 0.0.0.0 --port ${MCP_PORT:-8100}"]
