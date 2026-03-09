# Instagram monitoring is NOT supported via the YouTube collector.
# Instagram Graph API only allows monitoring owned accounts.

from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import redis
from core.config import settings
from collectors.base_collector import BaseCollector, CollectedPost
import logging

logger = logging.getLogger(__name__)


class YouTubeCollector(BaseCollector):
    # Quota costs per operation
    SEARCH_QUOTA_COST = 100
    COMMENT_THREADS_QUOTA_COST = 1
    DAILY_QUOTA_LIMIT = 10000
    QUOTA_SAFETY_MARGIN = 500  # Stop at 9500 to leave buffer

    def __init__(self):
        super().__init__()
        self.youtube = build('youtube', 'v3', developerKey=settings.YOUTUBE_API_KEY)
        self.redis_sync = redis.from_url(settings.REDIS_URL, decode_responses=True)

    def get_platform_name(self) -> str:
        return "youtube"

    def _get_quota_key(self) -> str:
        return f"youtube:quota:{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"

    def _get_quota_used(self) -> int:
        return int(self.redis_sync.get(self._get_quota_key()) or 0)

    def _increment_quota(self, cost: int):
        key = self._get_quota_key()
        self.redis_sync.incrby(key, cost)
        self.redis_sync.expire(key, 86400 * 2)  # Keep for 2 days

    def _is_quota_available(self, needed: int) -> bool:
        return self._get_quota_used() + needed <= (self.DAILY_QUOTA_LIMIT - self.QUOTA_SAFETY_MARGIN)

    def _get_processed_videos_key(self, keyword: str) -> str:
        return f"youtube:processed_videos:{keyword.lower().replace(' ', '_')}"

    def _is_video_processed(self, keyword: str, video_id: str) -> bool:
        return bool(self.redis_sync.sismember(self._get_processed_videos_key(keyword), video_id))

    def _mark_video_processed(self, keyword: str, video_id: str):
        key = self._get_processed_videos_key(keyword)
        self.redis_sync.sadd(key, video_id)
        self.redis_sync.expire(key, 86400 * 7)  # Keep processed list for 7 days

    def collect(self, keyword: str, since: datetime) -> list[CollectedPost]:
        posts = []

        if not settings.YOUTUBE_API_KEY:
            logger.warning("YouTube API key not configured, skipping YouTube collection")
            return posts

        # Check quota before search
        if not self._is_quota_available(self.SEARCH_QUOTA_COST):
            logger.warning(
                f"YouTube quota nearly exhausted ({self._get_quota_used()} used), skipping collection"
            )
            return posts

        try:
            published_after = since.strftime('%Y-%m-%dT%H:%M:%SZ')
            search_response = self.youtube.search().list(
                q=keyword,
                type='video',
                order='date',
                maxResults=50,
                publishedAfter=published_after,
                relevanceLanguage='en',
            ).execute()
            self._increment_quota(self.SEARCH_QUOTA_COST)

            video_items = search_response.get('items', [])
            logger.info(f"YouTube: found {len(video_items)} videos for keyword '{keyword}'")

            for item in video_items:
                video_id = item['id'].get('videoId')
                if not video_id:
                    continue
                if self._is_video_processed(keyword, video_id):
                    continue
                if not self._is_quota_available(self.COMMENT_THREADS_QUOTA_COST):
                    logger.warning("YouTube quota limit approaching, stopping comment collection")
                    break

                comments = self._fetch_comments(video_id)
                video_url = f"https://www.youtube.com/watch?v={video_id}"

                for comment in comments:
                    snippet = comment['snippet']['topLevelComment']['snippet']
                    post = CollectedPost(
                        platform="youtube",
                        post_id=comment['id'],
                        author_id=snippet.get('authorChannelId', {}).get('value', 'unknown'),
                        author_name=snippet.get('authorDisplayName', 'Unknown'),
                        # YouTube API does not expose subscriber count in comment threads; defaulting to 0
                        followers_count=0,
                        content=snippet.get('textOriginal', ''),
                        posted_at=datetime.fromisoformat(
                            snippet['publishedAt'].replace('Z', '+00:00')
                        ).replace(tzinfo=None),
                        url=video_url,
                        likes_count=snippet.get('likeCount', 0),
                        raw_data=comment,
                    )
                    posts.append(post)

                self._mark_video_processed(keyword, video_id)

        except HttpError as e:
            logger.error(f"YouTube API HTTP error: {e}")
        except Exception as e:
            logger.error(f"YouTube collector error: {e}")

        return posts

    def _fetch_comments(self, video_id: str) -> list[dict]:
        comments = []
        try:
            response = self.youtube.commentThreads().list(
                videoId=video_id,
                maxResults=100,
                order='relevance',
                textFormat='plainText',
            ).execute()
            self._increment_quota(self.COMMENT_THREADS_QUOTA_COST)
            comments = response.get('items', [])
        except HttpError as e:
            if e.resp.status == 403:
                logger.warning(f"Comments disabled for video {video_id}")
            else:
                logger.error(f"Error fetching comments for {video_id}: {e}")
        return comments
