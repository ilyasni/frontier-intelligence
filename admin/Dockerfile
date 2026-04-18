FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --timeout 120 \
    fastapi==0.115.6 \
    "uvicorn[standard]==0.32.1" \
    prometheus-client==0.20.0 \
    sqlalchemy[asyncio]==2.0.30 \
    asyncpg==0.29.0 \
    redis==5.0.4 \
    apscheduler==3.10.4 \
    pydantic==2.7.1 \
    pydantic-settings==2.3.1 \
    pyyaml==6.0.1 \
    httpx==0.27.0 \
    openai==1.54.4 \
    socksio==1.0.0 \
    boto3==1.34.131 \
    qdrant-client==1.12.0 \
    "fastembed>=0.3.6" \
    "onnxruntime>=1.17.0"

COPY shared/ /app/shared/
COPY admin/ /app/admin/
COPY worker/ /app/worker/
COPY config/ /app/config/
COPY scripts/ /app/scripts/

ENV PYTHONPATH=/app

EXPOSE 8101

CMD ["uvicorn", "admin.backend.main:app", "--host", "0.0.0.0", "--port", "8101"]
