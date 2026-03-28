import httpx
from bs4 import BeautifulSoup
import markdownify
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
DOMAIN = "safcodental.com"


@dataclass
class FetchResult:
    markdown: str
    links: list[str]
    http_status: int | None = None
    error: str | None = None
    error_type: str | None = None
    final_url: str | None = None


def fetch_page(url: str) -> FetchResult:
    """
    Fetches the URL, extracts all same-domain links, and cleans the HTML.
    Returns markdown, discovered links, and fetch metadata.
    """
    try:
        response = httpx.get(url, headers=HEADERS, timeout=15.0, follow_redirects=True)
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
    
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    # Link Extraction
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        
        if DOMAIN in parsed.netloc:
            clean_url = full_url.split('#')[0].split('?')[0]
            
            # Junk Filter
            junk_paths = ['/checkout', '/login', '/cart', '/account', '/search', 'mailto:', 'tel:']
            if not any(junk in clean_url for junk in junk_paths):
                # Ensure it's not a generic homepage loop
                if clean_url != "https://www.safcodental.com/":
                    links.append(clean_url)

    # HTML Pruning (The "Practical" Cost-Saver)
    for tag in soup(["header", "nav", "footer", "script", "style", "aside", "form", "svg"]):
        tag.decompose()
        
    # Convert remaining DOM to markdown
    markdown = markdownify.markdownify(str(soup), heading_style="ATX")
    
    return FetchResult(
        markdown=markdown.strip(),
        links=list(set(links)),
        http_status=response.status_code,
        final_url=str(response.url),
    )
