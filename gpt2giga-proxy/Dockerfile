FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
ENV PYTHONHTTPSVERIFY=0
ENV CURL_CA_BUNDLE=""
ENV REQUESTS_CA_BUNDLE=""
ENV SSL_CERT_FILE=""
ENV HTTPX_VERIFY=false
RUN pip install --no-cache-dir gpt2giga
WORKDIR /app
COPY disable_ssl.py /app/disable_ssl.py
COPY run_proxy.py /app/run_proxy.py
# Liveness по TCP (не тянуть GigaChat /v1/models — иначе unhealthy при сетевых/ключевых сбоях)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import os,socket;s=socket.socket();s.settimeout(3);p=int(os.getenv('GPT2GIGA_PROXY_PORT','8090'));s.connect(('127.0.0.1',p));s.close()"
CMD ["python3", "/app/run_proxy.py"]
