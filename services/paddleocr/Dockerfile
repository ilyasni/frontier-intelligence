# Как в telegram-assistant: официальный образ paddle — системные libGL/gomp/opencv уже согласованы
FROM paddlepaddle/paddle:2.6.1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

ARG PIP_INDEX_URL=

COPY services/paddleocr/requirements.txt /tmp/requirements.txt
RUN if [ -n "$PIP_INDEX_URL" ]; then \
      pip install --no-cache-dir --timeout=7200 --retries=40 -i "$PIP_INDEX_URL" -r /tmp/requirements.txt; \
    else \
      pip install --no-cache-dir --timeout=7200 --retries=40 -r /tmp/requirements.txt; \
    fi

COPY services/paddleocr/app /app/app

EXPOSE 8008

CMD ["uvicorn", "app.server:app", "--host", "0.0.0.0", "--port", "8008", "--timeout-keep-alive", "120"]
