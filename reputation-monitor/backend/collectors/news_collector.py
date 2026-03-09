import hashlib
import html
import logging
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

import feedparser
import requests
import redis

from core.config import settings
from collectors.base_collector import BaseCollector, CollectedPost

logger = logging.getLogger(__name__)

_SEEN_URLS_KEY = "news:seen_urls"
_SEEN_URLS_TTL = 86400 * 7  # 7 days


class _HTMLStripper(HTMLParser):
    """Minimal HTML stripper that accumulates text nodes."""

    def __init__(self):
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str):
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts).strip()


def _strip_html(raw: str) -> str:
    raw = html.unescape(raw or "")
    stripper = _HTMLStripper()
    try:
        stripper.feed(raw)
        return stripper.get_text()
    except Exception:
        return raw


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return "unknown"


def _parse_published(entry) -> datetime:
    """Try to extract a naive UTC datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        value = getattr(entry, attr, None)
        if value:
            try:
                return datetime(*value[:6])
            except Exception:
                continue
    return datetime.now(timezone.utc).replace(tzinfo=None)


class NewsCollector(BaseCollector):
    _GOOGLE_RSS = (
        "https://news.google.com/rss/search?q={keyword}&hl=en-US&gl=US&ceid=US:en"
    )
    _NEWSDATA_API = (
        "https://newsdata.io/api/1/news?apikey={key}&q={keyword}&language=en"
    )

    def __init__(self):
        super().__init__()
        self.redis_sync = redis.from_url(settings.REDIS_URL, decode_responses=True)

    def get_platform_name(self) -> str:
        return "news"

    # ------------------------------------------------------------------
    # Deduplication helpers
    # ------------------------------------------------------------------

    def _is_seen(self, url: str) -> bool:
        return bool(self.redis_sync.sismember(_SEEN_URLS_KEY, url))

    def _mark_seen(self, url: str):
        self.redis_sync.sadd(_SEEN_URLS_KEY, url)
        self.redis_sync.expire(_SEEN_URLS_KEY, _SEEN_URLS_TTL)

    # ------------------------------------------------------------------
    # Source: Google News RSS
    # ------------------------------------------------------------------

    def _collect_google_news(self, keyword: str, since: datetime) -> list[CollectedPost]:
        posts: list[CollectedPost] = []
        url = self._GOOGLE_RSS.format(keyword=requests.utils.quote(keyword))
        try:
            feed = feedparser.parse(url)
            if feed.bozo and not feed.entries:
                logger.warning(f"Google News RSS parse error for '{keyword}': {feed.bozo_exception}")
                return posts

            for entry in feed.entries:
                link = getattr(entry, "link", None) or getattr(entry, "id", None) or ""
                if not link:
                    continue
                if self._is_seen(link):
                    continue

                published = _parse_published(entry)
                if published < since:
                    continue

                title = _strip_html(getattr(entry, "title", ""))
                summary = _strip_html(getattr(entry, "summary", ""))
                content = f"{title} {summary}".strip()

                source_title = "Unknown"
                source = getattr(entry, "source", None)
                if source and isinstance(source, dict):
                    source_title = source.get("title", "Unknown")
                elif hasattr(entry, "tags") and entry.tags:
                    source_title = entry.tags[0].get("term", "Unknown")

                post = CollectedPost(
                    platform="news",
                    post_id=_md5(link),
                    author_id=_domain(link),
                    author_name=source_title,
                    followers_count=0,
                    content=content,
                    posted_at=published,
                    url=link,
                    raw_data={"feed": "google_news"},
                )
                self._mark_seen(link)
                posts.append(post)

        except Exception as e:
            logger.error(f"Google News RSS error for '{keyword}': {e}")

        return posts

    # ------------------------------------------------------------------
    # Source: newsdata.io
    # ------------------------------------------------------------------

    def _collect_newsdata(self, keyword: str, since: datetime) -> list[CollectedPost]:
        posts: list[CollectedPost] = []
        if not settings.NEWSDATA_API_KEY:
            return posts

        url = self._NEWSDATA_API.format(
            key=settings.NEWSDATA_API_KEY,
            keyword=requests.utils.quote(keyword),
        )
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            for article in data.get("results", []):
                link = article.get("link") or article.get("source_url") or ""
                if not link:
                    continue
                if self._is_seen(link):
                    continue

                # Parse published date
                pub_date_str = article.get("pubDate") or article.get("publishedAt") or ""
                try:
                    published = datetime.strptime(pub_date_str[:19], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    published = datetime.now(timezone.utc).replace(tzinfo=None)

                if published < since:
                    continue

                title = _strip_html(article.get("title", ""))
                description = _strip_html(article.get("description", "") or "")
                content = f"{title} {description}".strip()

                source_title = article.get("source_id", _domain(link)) or "Unknown"
                creator_list = article.get("creator") or []
                author_name = (
                    creator_list[0] if isinstance(creator_list, list) and creator_list
                    else source_title
                )

                post = CollectedPost(
                    platform="news",
                    post_id=_md5(link),
                    author_id=_domain(link),
                    author_name=author_name,
                    followers_count=0,
                    content=content,
                    posted_at=published,
                    url=link,
                    raw_data={"feed": "newsdata_io", "category": article.get("category")},
                )
                self._mark_seen(link)
                posts.append(post)

        except requests.HTTPError as e:
            logger.error(f"newsdata.io HTTP error for '{keyword}': {e}")
        except Exception as e:
            logger.error(f"newsdata.io collector error for '{keyword}': {e}")

        return posts

    # ------------------------------------------------------------------
    # Main collect
    # ------------------------------------------------------------------

    def collect(self, keyword: str, since: datetime) -> list[CollectedPost]:
        posts: list[CollectedPost] = []

        google_posts = self._collect_google_news(keyword, since)
        posts.extend(google_posts)
        logger.info(f"News (Google RSS): {len(google_posts)} articles for '{keyword}'")

        # Small delay to avoid hammering external services back-to-back
        time.sleep(0.5)

        newsdata_posts = self._collect_newsdata(keyword, since)
        posts.extend(newsdata_posts)
        logger.info(f"News (newsdata.io): {len(newsdata_posts)} articles for '{keyword}'")

        return posts
