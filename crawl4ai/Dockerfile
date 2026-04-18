FROM python:3.11-slim

WORKDIR /app

COPY requirements-crawl4ai.txt .
RUN pip install --no-cache-dir -r requirements-crawl4ai.txt
RUN python -m playwright install --with-deps chromium

COPY . .

ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

CMD ["python", "-m", "crawl4ai.main"]
