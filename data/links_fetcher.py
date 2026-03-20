"""
links_fetcher.py  --  Fetch YouTube video title (best-effort).

Returns None if the title cannot be retrieved — callers should fall back
to a user-supplied title rather than treating this as a hard error.
"""

import re
import urllib.request
import urllib.error
from html.parser import HTMLParser

VALID_PREFIXES = (
    "https://www.youtube.com/",
    "https://youtu.be/",
)

TIMEOUT = 8  # seconds


class FetchError(Exception):
    """Raised only for invalid URLs."""
    pass


def validate_url(url: str) -> None:
    if not any(url.startswith(p) for p in VALID_PREFIXES):
        raise FetchError("Only YouTube URLs are accepted (youtube.com or youtu.be)")


class _TitleParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._in_title = False
        self.title: str = ""

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "title":
            self._in_title = True

    def handle_endtag(self, tag):
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title:
            self.title += data


def _clean_title(raw: str) -> str:
    title = raw.strip()
    if title.endswith(" - YouTube"):
        title = title[: -len(" - YouTube")].strip()
    return title


def fetch_title(url: str) -> str | None:
    """
    Try to fetch the YouTube video title. Returns the title string, or
    None if it cannot be retrieved (network error, bot-check, etc.).

    Raises FetchError only for invalid URLs.
    """
    validate_url(url)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw_bytes = resp.read(131072)
    except Exception:
        return None

    try:
        html = raw_bytes.decode("utf-8", errors="replace")
    except Exception:
        return None

    # Try <title> tag
    parser = _TitleParser()
    parser.feed(html)
    if parser.title:
        cleaned = _clean_title(parser.title)
        if cleaned:
            return cleaned

    # Try og:title meta tag
    m = re.search(
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html
    )
    if m:
        cleaned = _clean_title(m.group(1))
        if cleaned:
            return cleaned

    # Try JSON-embedded title
    m = re.search(r'"title"\s*:\s*"([^"]{3,})"', html)
    if m:
        cleaned = _clean_title(m.group(1))
        if cleaned:
            return cleaned

    return None
