import time
import logging
from datetime import datetime, timezone

import praw
import praw.exceptions
from praw.models import MoreComments

from core.config import settings
from collectors.base_collector import BaseCollector, CollectedPost

logger = logging.getLogger(__name__)


class RedditCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.reddit = praw.Reddit(
            client_id=settings.REDDIT_CLIENT_ID,
            client_secret=settings.REDDIT_CLIENT_SECRET,
            user_agent=settings.REDDIT_USER_AGENT,
        )
        # Read-only mode; no login required for public data
        self.reddit.read_only = True

    def get_platform_name(self) -> str:
        return "reddit"

    def collect(self, keyword: str, since: datetime) -> list[CollectedPost]:
        if not settings.REDDIT_CLIENT_ID or not settings.REDDIT_CLIENT_SECRET:
            logger.warning("Reddit credentials not configured, skipping Reddit collection")
            return []

        posts: list[CollectedPost] = []
        since_ts = since.timestamp()

        # Always search r/all first, then top 5 relevant subreddits
        subreddits_to_search: list[str] = ["all"]
        try:
            relevant = list(self.reddit.subreddits.search(keyword, limit=5))
            subreddits_to_search += [sr.display_name for sr in relevant]
        except Exception as e:
            logger.warning(f"Reddit subreddit search failed: {e}")

        seen_post_ids: set[str] = set()

        for subreddit_name in subreddits_to_search:
            if len(posts) >= settings.MAX_POSTS_PER_COLLECTION:
                break
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                submissions = subreddit.search(
                    keyword,
                    sort="new",
                    time_filter="week",
                    limit=100,
                )
                for submission in submissions:
                    if len(posts) >= settings.MAX_POSTS_PER_COLLECTION:
                        break
                    if submission.created_utc < since_ts:
                        continue
                    if submission.id in seen_post_ids:
                        continue
                    seen_post_ids.add(submission.id)

                    author_name = self._safe_author(submission.author)
                    sub_post = CollectedPost(
                        platform="reddit",
                        post_id=submission.id,
                        author_id=author_name,
                        author_name=author_name,
                        followers_count=submission.subreddit.subscribers,
                        content=f"{submission.title} {submission.selftext or ''}".strip(),
                        posted_at=datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).replace(tzinfo=None),
                        url=submission.url,
                        likes_count=submission.score,
                        replies_count=submission.num_comments,
                        raw_data={
                            "subreddit": submission.subreddit.display_name,
                            "upvote_ratio": submission.upvote_ratio,
                        },
                    )
                    posts.append(sub_post)

                    # Collect top-level comments
                    try:
                        submission.comments.replace_more(limit=0)
                        for comment in submission.comments:
                            if len(posts) >= settings.MAX_POSTS_PER_COLLECTION:
                                break
                            if isinstance(comment, MoreComments):
                                continue
                            comment_author = self._safe_author(comment.author)
                            comment_post = CollectedPost(
                                platform="reddit",
                                post_id=comment.id,
                                author_id=comment_author,
                                author_name=comment_author,
                                followers_count=submission.subreddit.subscribers,
                                content=comment.body,
                                posted_at=datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).replace(tzinfo=None),
                                url=f"https://www.reddit.com{comment.permalink}",
                                likes_count=comment.score,
                                replies_count=len(comment.replies),
                                raw_data={
                                    "subreddit": submission.subreddit.display_name,
                                    "submission_id": submission.id,
                                },
                            )
                            posts.append(comment_post)
                    except Exception as e:
                        logger.warning(f"Failed to fetch comments for submission {submission.id}: {e}")

            except praw.exceptions.PRAWException as e:
                logger.error(f"Reddit PRAW error for r/{subreddit_name}: {e}")
            except Exception as e:
                logger.error(f"Reddit collector error for r/{subreddit_name}: {e}")

            # Rate limit: 1 second between subreddit searches
            time.sleep(1)

        logger.info(f"Reddit: collected {len(posts)} posts/comments for keyword '{keyword}'")
        return posts

    @staticmethod
    def _safe_author(author) -> str:
        """Return author name, handling deleted/suspended accounts gracefully."""
        if author is None:
            return "deleted"
        try:
            return str(author)
        except Exception:
            return "unknown"
