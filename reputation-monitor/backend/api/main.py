"""
Reputation Monitor API - FastAPI application entry point.
"""
import logging
from fastapi import FastAPI, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded
from api.middleware.rate_limit import limiter
from core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Reputation Monitor API",
    description="Production-grade reputation monitoring and attack detection platform",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Rate limiting
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Max 100 req/min."},
    )


# CORS - restricted to configured dashboard origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
from api.routes import keywords, sentiment, posts, attackers, clusters, scores, alerts, live

API_PREFIX = "/api/v1"
app.include_router(keywords.router, prefix=API_PREFIX)
app.include_router(sentiment.router, prefix=API_PREFIX)
app.include_router(posts.router, prefix=API_PREFIX)
app.include_router(attackers.router, prefix=API_PREFIX)
app.include_router(clusters.router, prefix=API_PREFIX)
app.include_router(scores.router, prefix=API_PREFIX)
app.include_router(alerts.router, prefix=API_PREFIX)
app.include_router(live.router)  # WebSocket routes don't use /api/v1 prefix

# Auth endpoints
from api.middleware.auth import create_access_token
from core.schemas import Token


@app.post("/api/v1/auth/token", response_model=Token, tags=["auth"])
async def get_token(user_id: str = Body(..., embed=True)):
    """
    Development endpoint: generate a JWT token for a user_id.
    In production, integrate with your auth provider (OAuth2, Auth0, etc.)
    """
    token = create_access_token(user_id)
    return Token(access_token=token)


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok", "service": "reputation-monitor-api"}
