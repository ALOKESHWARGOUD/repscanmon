from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    DATABASE_URL: str = "postgresql://repuser:reppass@postgres:5432/reputation_db"

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # JWT
    JWT_SECRET_KEY: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 1440

    # API Keys
    YOUTUBE_API_KEY: str = ""
    # Comma-separated list of additional YouTube API keys (rotated when quota is exceeded)
    YOUTUBE_API_KEYS: str = ""
    REDDIT_CLIENT_ID: str = ""
    REDDIT_CLIENT_SECRET: str = ""
    REDDIT_USER_AGENT: str = "ReputationMonitor/1.0"
    TWITTER_BEARER_TOKEN: str = ""
    # Comma-separated list of additional Twitter bearer tokens (rotated when rate limit is hit)
    TWITTER_BEARER_TOKENS: str = ""
    NEWSDATA_API_KEY: str = ""

    def get_youtube_api_keys(self) -> list[str]:
        """Return deduplicated list of all configured YouTube API keys."""
        keys: list[str] = []
        if self.YOUTUBE_API_KEYS:
            keys.extend([k.strip() for k in self.YOUTUBE_API_KEYS.split(",") if k.strip()])
        if self.YOUTUBE_API_KEY and self.YOUTUBE_API_KEY not in keys:
            keys.append(self.YOUTUBE_API_KEY)
        return keys

    def get_twitter_bearer_tokens(self) -> list[str]:
        """Return deduplicated list of all configured Twitter bearer tokens."""
        tokens: list[str] = []
        if self.TWITTER_BEARER_TOKENS:
            tokens.extend([t.strip() for t in self.TWITTER_BEARER_TOKENS.split(",") if t.strip()])
        if self.TWITTER_BEARER_TOKEN and self.TWITTER_BEARER_TOKEN not in tokens:
            tokens.append(self.TWITTER_BEARER_TOKEN)
        return tokens

    # SMTP
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    ALERT_FROM_EMAIL: str = ""

    # Telegram
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # App settings
    COLLECTION_INTERVAL_SECONDS: int = 1800
    STATS_BROADCAST_INTERVAL_SECONDS: int = 30
    MAX_POSTS_PER_COLLECTION: int = 500
    SENTIMENT_BATCH_SIZE: int = 32
    NEGATIVE_SPIKE_THRESHOLD: int = 40
    MIN_CLUSTER_SIZE: int = 3
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
