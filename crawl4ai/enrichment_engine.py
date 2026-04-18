"""Crawl4AI enrichment engine — HTTP crawling with BS4, OG extraction, Redis cache, S3 storage."""
import asyncio
import gzip
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, Error as PlaywrightError, Page, async_playwright

import structlog
from shared.metrics import note_crawl_session_recreate, note_rate_limit_event
from shared.s3 import make_s3_client

log = structlog.get_logger()

CACHE_TTL = 3600           # 1 hour
MAX_CONCURRENT = 3
RATE_LIMIT_PER_HOST = 10   # requests/min
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
USER_AGENT = "FrontierIntelligence/1.0 (RSS/Web enrichment)"
MEDIUM_HOSTS = {"medium.com", "www.medium.com"}


def _make_s3(settings) -> tuple[Optional[Any], Optional[str]]:
    return make_s3_client(settings)


class EnrichmentEngine:
    """HTTP crawler: fetch URL → extract markdown/OG/meta → cache in Redis → store HTML in S3."""

    def __init__(self, redis_client, settings):
        self._redis = redis_client
        self._settings = settings
        self._s3, self._bucket = _make_s3(settings)
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self._rate_cache: dict[str, list[float]] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._browser_context: Optional[BrowserContext] = None

    async def start(self):
        self._session = aiohttp.ClientSession(
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )

    async def stop(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._browser_context is not None:
            await self._browser_context.close()
            self._browser_context = None
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            note_crawl_session_recreate("crawl4ai", "closed_or_missing")
            await self.start()
        return self._session

    def _normalize_url(self, url: str) -> str:
        parsed = urlparse(url.lower())
        sorted_query = urlencode(sorted(parse_qs(parsed.query, keep_blank_values=True).items()), doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, sorted_query, ""))

    def _url_hash(self, url: str) -> str:
        return hashlib.sha256(self._normalize_url(url).encode()).hexdigest()

    def _host(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _should_use_browser(self, url: str) -> bool:
        return self._host(url) in MEDIUM_HOSTS

    async def _cache_get(self, key: str) -> Optional[dict]:
        try:
            data = await self._redis.get(key)
            return json.loads(data) if data else None
        except Exception:
            return None

    async def _cache_set(self, key: str, data: dict):
        try:
            await self._redis.setex(key, CACHE_TTL, json.dumps(data, default=str))
        except Exception:
            pass

    async def _check_rate_limit(self, url: str) -> bool:
        host = urlparse(url).netloc
        now = time.time()
        self._rate_cache.setdefault(host, [])
        self._rate_cache[host] = [t for t in self._rate_cache[host] if now - t < 60]
        if len(self._rate_cache[host]) >= RATE_LIMIT_PER_HOST:
            return False
        self._rate_cache[host].append(now)
        return True

    async def _ensure_browser_context(self) -> BrowserContext:
        if self._playwright is None:
            self._playwright = await async_playwright().start()
        if self._browser_context is None:
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                proxy={"server": "socks5://xray:10808"},
                args=[
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            self._browser_context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                ignore_https_errors=True,
            )
        return self._browser_context

    async def _browser_fetch(self, url: str) -> tuple[int, bytes]:
        context = await self._ensure_browser_context()
        page: Optional[Page] = None
        try:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_timeout(4_000)
            html = await page.content()
            title = await page.title()
            status = 200
            if "Just a moment" in title or "cf-browser-verification" in html.lower():
                status = 403
            return status, html.encode("utf-8", errors="ignore")
        finally:
            if page is not None:
                await page.close()

    def _extract_og(self, soup: BeautifulSoup) -> dict:
        og = {}
        for prop, key in [
            ("og:title", "title"), ("og:description", "description"),
            ("og:image", "image"), ("og:url", "url"),
            ("og:type", "type"), ("og:site_name", "site_name"),
        ]:
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"):
                og[key] = tag["content"]
        # Twitter Card fallback
        for tw, key in [("twitter:title", "title"), ("twitter:description", "description"), ("twitter:image", "image")]:
            if key not in og:
                tag = soup.find("meta", attrs={"name": tw})
                if tag and tag.get("content"):
                    og[key] = tag["content"]
        return og

    def _extract_meta(self, soup: BeautifulSoup, content: str) -> dict:
        title = None
        for sel in [lambda s: s.find("meta", property="og:title"),
                    lambda s: s.find("title"),
                    lambda s: s.find("h1")]:
            el = sel(soup)
            if el:
                title = el.get("content") or el.get_text(strip=True)
                if title:
                    break

        desc = None
        for sel in [lambda s: s.find("meta", property="og:description"),
                    lambda s: s.find("meta", attrs={"name": "description"})]:
            el = sel(soup)
            if el and el.get("content"):
                desc = el["content"]
                break
        if not desc:
            p = soup.find("p")
            if p:
                text = p.get_text(strip=True)
                if len(text) > 50:
                    desc = text[:300]

        # Simple language detection
        lang = "ru" if any(ord(c) > 127 for c in content[:200]) else "en"
        return {"title": title, "description": desc, "lang": lang}

    def _to_markdown(self, soup: BeautifulSoup) -> str:
        """Convert main content to simple markdown."""
        # Remove scripts, styles, nav
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        lines = []
        for el in soup.find_all(["h1", "h2", "h3", "h4", "p", "li", "blockquote"]):
            text = el.get_text(strip=True)
            if not text:
                continue
            tag = el.name
            if tag == "h1":
                lines.append(f"# {text}")
            elif tag == "h2":
                lines.append(f"## {text}")
            elif tag == "h3":
                lines.append(f"### {text}")
            elif tag == "h4":
                lines.append(f"#### {text}")
            elif tag == "li":
                lines.append(f"- {text}")
            elif tag == "blockquote":
                lines.append(f"> {text}")
            else:
                lines.append(text)
        return "\n\n".join(lines)

    async def _upload_to_s3(self, workspace_id: str, url_hash: str,
                             content_bytes: bytes, suffix: str, content_type: str) -> Optional[str]:
        if not self._s3:
            return None
        key = f"crawl/{workspace_id}/{url_hash}{suffix}.gz"
        try:
            compressed = gzip.compress(content_bytes)
            self._s3.put_object(
                Bucket=self._bucket, Key=key, Body=compressed,
                ContentType=content_type, ContentEncoding="gzip",
            )
            return key
        except Exception as exc:
            log.warning("S3 upload failed", key=key, error=str(exc))
            return None

    async def enrich_url(self, url: str, workspace_id: str, post_id: str) -> Optional[dict]:
        """Crawl a URL and return enrichment data dict. Returns None if skipped."""
        async with self._semaphore:
            if not await self._check_rate_limit(url):
                log.info("Rate limited", url=url)
                return None

            url_hash = self._url_hash(url)
            cache_key = f"crawl4ai:enrichment:{url_hash}"

            cached = await self._cache_get(cache_key)
            if cached:
                return cached

            # Conditional request headers
            headers = {}
            etag = await self._redis.get(f"crawl4ai:etag:{url_hash}")
            lm = await self._redis.get(f"crawl4ai:lm:{url_hash}")
            if etag:
                headers["If-None-Match"] = etag
            if lm:
                headers["If-Modified-Since"] = lm

            try:
                content_bytes = b""
                content = ""
                etag_value = None
                lm_value = None

                if self._should_use_browser(url):
                    status, content_bytes = await self._browser_fetch(url)
                    if status != 200:
                        log.info("Browser HTTP error", url=url, status=status)
                        return None
                    content = content_bytes.decode("utf-8", errors="ignore")
                else:
                    session = await self._ensure_session()
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 304:
                            return await self._cache_get(cache_key)
                        if resp.status == 429:
                            note_rate_limit_event("crawl4ai", "http_origin", "fetch")
                            retry_after = int(resp.headers.get("Retry-After", 60))
                            await asyncio.sleep(min(retry_after, 120))
                            return None
                        if resp.status != 200:
                            log.info("HTTP error", url=url, status=resp.status)
                            return None

                        content_bytes = await resp.read()
                        content = content_bytes.decode("utf-8", errors="ignore")
                        etag_value = resp.headers.get("ETag")
                        lm_value = resp.headers.get("Last-Modified")

                content_sha = hashlib.sha256(content_bytes).hexdigest()

                # Check content hash cache
                content_cache_key = f"crawl4ai:content:{content_sha}"
                by_content = await self._cache_get(content_cache_key)
                if by_content:
                    await self._cache_set(cache_key, by_content)
                    return by_content

                soup = BeautifulSoup(content, "lxml")
                og = self._extract_og(soup)
                meta = self._extract_meta(soup, content)
                markdown = self._to_markdown(soup)
                word_count = len(content.split())

                # S3 storage (non-blocking, errors are soft)
                html_s3_key = await self._upload_to_s3(
                    workspace_id, url_hash, content_bytes, ".html", "text/html"
                )
                md_s3_key = await self._upload_to_s3(
                    workspace_id, url_hash, markdown.encode(), ".md", "text/markdown"
                )

                # Cache HTTP headers for direct fetches
                if etag_value:
                    await self._redis.setex(f"crawl4ai:etag:{url_hash}", CACHE_TTL, etag_value)
                if lm_value:
                    await self._redis.setex(f"crawl4ai:lm:{url_hash}", CACHE_TTL, lm_value)

                result = {
                    "url": url,
                    "url_hash": url_hash,
                    "content_sha256": content_sha,
                    "title": meta["title"] or og.get("title"),
                    "description": meta["description"] or og.get("description"),
                    "lang": meta["lang"],
                    "word_count": word_count,
                    "md_excerpt": markdown[:2000],
                    "og": og,
                    "html_s3_key": html_s3_key,
                    "md_s3_key": md_s3_key,
                    "crawled_at": datetime.now(timezone.utc).isoformat(),
                    "status": "success",
                }
                await self._cache_set(cache_key, result)
                await self._cache_set(content_cache_key, result)
                return result

            except asyncio.TimeoutError:
                log.warning("Timeout", url=url)
                return None
            except aiohttp.ClientError as exc:
                log.warning("HTTP client error", url=url, error=str(exc))
                return None
            except PlaywrightError as exc:
                log.warning("Browser crawl error", url=url, error=str(exc))
                return None
            except Exception as exc:
                log.warning("Crawl error", url=url, error=str(exc))
                return None
