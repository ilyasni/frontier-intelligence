FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout 120     telethon==1.36.0 PySocks==1.7.1 socksio==1.0.0     apscheduler==3.10.4     feedparser==6.0.11     httpx==0.27.0     beautifulsoup4==4.12.3     lxml==5.2.2     sqlalchemy[asyncio]==2.0.30     asyncpg==0.29.0     redis==5.0.4     pydantic-settings==2.3.1     pyyaml==6.0.1     boto3==1.34.131     prometheus-client==0.20.0     croniter>=1.3.8

COPY shared/ /app/shared/
COPY ingest/ /app/ingest/

CMD ["python", "-m", "ingest.main"]
