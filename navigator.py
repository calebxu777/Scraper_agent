import os
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
import markdownify
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
DOMAIN = "safcodental.com"


def _browser_mode() -> str:
    return os.getenv("FETCH_BROWSER", "playwright").lower()


def _playwright_timeout_ms() -> int:
    return int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "30000"))


def _product_wait_selector() -> str:
    return os.getenv(
        "PLAYWRIGHT_WAIT_SELECTOR",
        'a[href*="/product/"], .ais-Hits-item, [data-testid="product-card"]',
    )


@dataclass
class FetchResult:
    markdown: str
    links: list[str]
    http_status: int | None = None
    error: str | None = None
    error_type: str | None = None
    final_url: str | None = None


def _normalize_discovered_url(full_url: str) -> str:
    parsed = urlparse(full_url)
    path = parsed.path.rstrip("/") or "/"

    query_items: list[tuple[str, str]] = []
    if path.startswith("/catalog/"):
        # Preserve pagination-like state so category crawls can advance beyond page 1.
        allowed_query_keys = {"page", "p"}
        query_items = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=False) if key in allowed_query_keys]

    query = urlencode(query_items)
    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def _extract_links_and_markdown(url: str, html: str) -> tuple[list[str], str]:
    soup = BeautifulSoup(html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)

        if DOMAIN in parsed.netloc:
            clean_url = _normalize_discovered_url(full_url)

            junk_paths = ["/checkout", "/login", "/cart", "/account", "/search", "mailto:", "tel:"]
            if not any(junk in clean_url for junk in junk_paths):
                if clean_url != "https://www.safcodental.com/":
                    links.append(clean_url)

    # Keep form content because Safco often renders SKU, size, stock, and cart state inside form blocks.
    for tag in soup(["header", "nav", "footer", "script", "style", "aside", "svg"]):
        tag.decompose()

    markdown = markdownify.markdownify(str(soup), heading_style="ATX")
    return list(set(links)), markdown.strip()


async def _fetch_page_httpx(url: str) -> FetchResult:
    """Fetch the raw HTML over HTTP and extract links plus cleaned markdown."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS, timeout=15.0, follow_redirects=True)
            response.raise_for_status()
    except httpx.ConnectTimeout as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="connect_timeout")
    except httpx.ReadTimeout as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="read_timeout")
    except httpx.ConnectError as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="connect_error")
    except httpx.ProxyError as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="proxy_error")
    except httpx.RemoteProtocolError as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="remote_protocol_error")
    except httpx.RequestError as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type=e.__class__.__name__.lower())
    except httpx.HTTPStatusError as e:
        return FetchResult(
            markdown="",
            links=[],
            http_status=e.response.status_code,
            error=str(e),
            error_type="http_status_error",
            final_url=str(e.response.url),
        )
    except Exception as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type=e.__class__.__name__.lower())

    links, markdown = _extract_links_and_markdown(url, response.text)

    return FetchResult(
        markdown=markdown,
        links=links,
        http_status=response.status_code,
        final_url=str(response.url),
    )


async def _fetch_page_playwright(url: str) -> FetchResult:
    """Render the page in Chromium so Algolia/InstantSearch content reaches the DOM."""
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except ModuleNotFoundError as e:
        return FetchResult(
            markdown="",
            links=[],
            error=str(e),
            error_type="playwright_not_installed",
        )

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(user_agent=HEADERS["User-Agent"])

            response = await page.goto(url, wait_until="domcontentloaded", timeout=_playwright_timeout_ms())
            if response is None:
                await browser.close()
                return FetchResult(markdown="", links=[], error="No response from browser navigation", error_type="playwright_no_response")

            try:
                await page.wait_for_load_state("networkidle", timeout=_playwright_timeout_ms())
            except PlaywrightTimeoutError:
                pass

            try:
                await page.wait_for_selector(_product_wait_selector(), timeout=5000)
            except PlaywrightTimeoutError:
                pass

            html = await page.content()
            final_url = page.url
            status = response.status
            await browser.close()
    except PlaywrightTimeoutError as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type="playwright_timeout")
    except Exception as e:
        return FetchResult(markdown="", links=[], error=str(e), error_type=e.__class__.__name__.lower())

    links, markdown = _extract_links_and_markdown(final_url, html)
    return FetchResult(
        markdown=markdown,
        links=links,
        http_status=status,
        final_url=final_url,
    )


async def fetch_page(url: str) -> FetchResult:
    """
    Fetch a page, render dynamic content when configured, then extract links and cleaned markdown.
    """
    browser_mode = _browser_mode()

    if browser_mode == "httpx":
        return await _fetch_page_httpx(url)

    browser_result = await _fetch_page_playwright(url)
    if browser_result.markdown:
        return browser_result

    if browser_mode == "playwright":
        return browser_result

    return await _fetch_page_httpx(url)
