# Requires X API Basic Plan (~$100/month). Search recent tweets limited to last 7 days.

import time
import logging
from datetime import datetime, timezone

import tweepy
import tweepy.errors
import redis

from core.config import settings
from collectors.base_collector import BaseCollector, CollectedPost

logger = logging.getLogger(__name__)

# Maximum requests allowed per 15-minute window on the Basic plan
_RATE_LIMIT_REQUESTS = 450
_WINDOW_SECONDS = 900  # 15 minutes


class TwitterCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.client = tweepy.Client(
            bearer_token=settings.TWITTER_BEARER_TOKEN,
            wait_on_rate_limit=False,
        )
        self.redis_sync = redis.from_url(settings.REDIS_URL, decode_responses=True)

    def get_platform_name(self) -> str:
        return "twitter"

    # ------------------------------------------------------------------
    # Rate-limit helpers
    # ------------------------------------------------------------------

    def _rate_limit_key(self) -> str:
        """Redis key bucketed to the current 15-minute window."""
        window = int(time.time()) // _WINDOW_SECONDS
        return f"twitter:requests:{window}"

    def _get_requests_used(self) -> int:
        return int(self.redis_sync.get(self._rate_limit_key()) or 0)

    def _increment_requests(self):
        key = self._rate_limit_key()
        self.redis_sync.incr(key)
        self.redis_sync.expire(key, _WINDOW_SECONDS * 2)

    def _is_rate_limit_available(self) -> bool:
        return self._get_requests_used() < _RATE_LIMIT_REQUESTS

    # ------------------------------------------------------------------
    # Exponential backoff wrapper
    # ------------------------------------------------------------------

    def _search_with_backoff(self, **kwargs):
        """Call search_recent_tweets with up to 3 retries on TooManyRequests."""
        wait_times = [60, 120, 240]
        for attempt, wait in enumerate(wait_times, start=1):
            try:
                return self.client.search_recent_tweets(**kwargs)
            except tweepy.errors.TooManyRequests:
                if attempt == len(wait_times):
                    logger.error("Twitter rate limit hit after all retries; giving up")
                    raise
                logger.warning(
                    f"Twitter rate limit hit (attempt {attempt}), waiting {wait}s before retry"
                )
                time.sleep(wait)
        return None  # unreachable

    # ------------------------------------------------------------------
    # Main collect
    # ------------------------------------------------------------------

    def collect(self, keyword: str, since: datetime) -> list[CollectedPost]:
        if not settings.TWITTER_BEARER_TOKEN:
            logger.warning("Twitter bearer token not configured, skipping Twitter collection")
            return []

        if not self._is_rate_limit_available():
            logger.warning(
                f"Twitter rate limit window exhausted ({self._get_requests_used()} requests used), "
                "skipping collection"
            )
            return []

        posts: list[CollectedPost] = []

        query = f'"{keyword}" lang:en -is:retweet'

        try:
            self._increment_requests()
            response = self._search_with_backoff(
                query=query,
                tweet_fields=["author_id", "created_at", "public_metrics", "entities", "lang"],
                user_fields=["name", "username", "public_metrics", "created_at"],
                expansions=["author_id"],
                max_results=100,
                start_time=since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since,
            )
        except tweepy.errors.TooManyRequests:
            logger.error("Twitter rate limit exceeded; aborting collection")
            return posts
        except tweepy.errors.TwitterServerError as e:
            logger.error(f"Twitter server error: {e}")
            return posts
        except Exception as e:
            logger.error(f"Twitter collector error: {e}")
            return posts

        if not response or not response.data:
            logger.info(f"Twitter: no tweets found for keyword '{keyword}'")
            return posts

        # Build a lookup map from user ID → user object
        users_by_id: dict[str, tweepy.User] = {}
        if response.includes and "users" in response.includes:
            for user in response.includes["users"]:
                users_by_id[str(user.id)] = user

        for tweet in response.data:
            author_id = str(tweet.author_id)
            user = users_by_id.get(author_id)
            author_name = user.username if user else author_id
            followers_count = (
                user.public_metrics["followers_count"]
                if user and user.public_metrics
                else 0
            )

            # Normalise posted_at to naive UTC
            posted_at = tweet.created_at
            if posted_at and posted_at.tzinfo is not None:
                posted_at = posted_at.replace(tzinfo=None)

            public_metrics = tweet.public_metrics or {}
            post = CollectedPost(
                platform="twitter",
                post_id=str(tweet.id),
                author_id=author_id,
                author_name=author_name,
                followers_count=followers_count,
                content=tweet.text,
                posted_at=posted_at or datetime.now(timezone.utc).replace(tzinfo=None),
                url=f"https://twitter.com/i/web/status/{tweet.id}",
                likes_count=public_metrics.get("like_count", 0),
                replies_count=public_metrics.get("reply_count", 0),
                shares_count=public_metrics.get("retweet_count", 0),
                language=tweet.lang or "en",
                raw_data={
                    "public_metrics": public_metrics,
                    "entities": tweet.entities,
                },
            )
            posts.append(post)

        logger.info(f"Twitter: collected {len(posts)} tweets for keyword '{keyword}'")
        return posts
