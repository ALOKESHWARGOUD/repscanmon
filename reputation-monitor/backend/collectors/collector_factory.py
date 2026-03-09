from collectors.youtube_collector import YouTubeCollector
from collectors.reddit_collector import RedditCollector
from collectors.twitter_collector import TwitterCollector
from collectors.news_collector import NewsCollector
from collectors.base_collector import BaseCollector

_collectors: dict[str, BaseCollector] = {}


def get_collector(platform: str) -> BaseCollector:
    global _collectors
    if platform not in _collectors:
        if platform == "youtube":
            _collectors[platform] = YouTubeCollector()
        elif platform == "reddit":
            _collectors[platform] = RedditCollector()
        elif platform == "twitter":
            _collectors[platform] = TwitterCollector()
        elif platform == "news":
            _collectors[platform] = NewsCollector()
        else:
            raise ValueError(f"Unknown platform: {platform}")
    return _collectors[platform]


def get_all_collectors() -> list[BaseCollector]:
    return [get_collector(p) for p in ["youtube", "reddit", "twitter", "news"]]
