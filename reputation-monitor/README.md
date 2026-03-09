# Reputation Monitor - Attack Detection Platform

A production-grade reputation monitoring and coordinated attack detection platform built with FastAPI, Next.js 14, Celery, PostgreSQL, and Redis.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [Quick Start (Docker Compose)](#quick-start-docker-compose)
- [Manual Setup](#manual-setup)
- [Database Migrations](#database-migrations)
- [Adding Your First Keyword](#adding-your-first-keyword)
- [Live Feed WebSocket](#live-feed-websocket)
- [API Reference](#api-reference)
- [API Costs & Budget](#api-costs--budget)
- [Security](#security)
- [Phase 2 Scaling](#phase-2-scaling)
- [Limitations](#limitations)

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Node.js 20+
- Redis 7+
- PostgreSQL 15+

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Collectors                               │
│  YouTube │ Reddit │ Twitter/X │ NewsData.io                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Celery Workers                               │
│  collect → process → analyze → detect                          │
│  (Queues: collection, processing, analysis, detection)         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌──────────────┐    ┌────┴──────┐    ┌────────────────────────── ┐
│  PostgreSQL  │◄───│  FastAPI  │───►│  WebSocket                │
│  (storage)   │    │  REST API │    │  (live feed)              │
└──────────────┘    └───────────┘    └──────────────┬────────────┘
                                                    │
                                                    ▼
                                     ┌──────────────────────────┐
                                     │   Next.js 14 Dashboard   │
                                     │   (live feed + charts)   │
                                     └──────────────────────────┘
```

**Components:**
- **Collectors**: YouTube Data API v3, Reddit API (PRAW), Twitter/X Search API v2, NewsData.io
- **Celery Workers**: Async task pipeline across four queues
- **ML Pipeline**: Sentiment analysis, attack detection, coordinated campaign identification
- **FastAPI**: REST API + WebSocket server
- **Next.js Dashboard**: Real-time live feed with charts and alerting

## Quick Start (Docker Compose)

```bash
cd reputation-monitor/infrastructure
cp .env.example .env
# Edit .env with your API keys
docker-compose up --build

# Run migrations
docker-compose exec api alembic -c database/migrations/alembic.ini upgrade head

# Open dashboard
open http://localhost:3000
```

## Manual Setup

### Backend

```bash
cd reputation-monitor/backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Start API
uvicorn api.main:app --reload

# Start Celery workers
celery -A pipeline.celery_app worker -Q collection,processing,analysis,detection

# Start Celery beat scheduler
celery -A pipeline.celery_app beat
```

### Frontend

```bash
cd reputation-monitor/frontend
npm install
npm run dev
```

## Database Migrations

```bash
# Apply all migrations
alembic -c backend/database/migrations/alembic.ini upgrade head

# Create new migration
alembic -c backend/database/migrations/alembic.ini revision --autogenerate -m "description"

# Rollback one step
alembic -c backend/database/migrations/alembic.ini downgrade -1
```

## Adding Your First Keyword

1. **Get a JWT token:**
```bash
curl -X POST http://localhost:8000/api/v1/auth/token \
  -H "Content-Type: application/json" \
  -d '{"user_id": "admin"}'
```

2. **Add keyword:**
```bash
curl -X POST http://localhost:8000/api/v1/keywords \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"keyword": "your brand name"}'
```

3. **View dashboard:** http://localhost:3000/dashboard

## Live Feed WebSocket

Connect with:
```
ws://localhost:8000/ws/live/{keyword}?token={jwt_token}
```

**Message types:**
- `{"event": "new_post", "data": {...}}` — New post collected and analyzed
- `{"event": "stats_update", "data": {...}}` — Stats broadcast every 30s
- `{"event": "connected", ...}` — Connection established

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/token` | Get JWT token |
| GET | `/api/v1/keywords` | List monitored keywords |
| POST | `/api/v1/keywords` | Add keyword |
| DELETE | `/api/v1/keywords/{id}` | Remove keyword |
| GET | `/api/v1/posts` | List collected posts |
| GET | `/api/v1/posts/{id}` | Get post details |
| GET | `/api/v1/alerts` | List active alerts |
| GET | `/api/v1/stats` | Aggregated stats |
| WS | `/ws/live/{keyword}` | Live feed WebSocket |

Full OpenAPI docs available at: http://localhost:8000/docs

## API Costs & Budget

| Platform | Plan | Cost | Limits |
|----------|------|------|--------|
| YouTube Data API v3 | Free | $0/month | 10,000 quota units/day |
| Reddit API | Free | $0/month | 60 req/min |
| Twitter/X Search API | Basic | ~$100/month | 10,000 tweets/month, last 7 days |
| NewsData.io | Free tier | $0/month | 200 req/day |
| **Total MVP** | | **~$100/month** | ~10,000 posts/day |

## Security

- JWT tokens expire in 24 hours (configurable via `JWT_EXPIRY_HOURS`)
- Rate limiting: 100 req/min per IP (configurable via `RATE_LIMIT`)
- CORS restricted to dashboard origin only
- All secrets managed via environment variables (never hardcoded)
- Passwords and tokens are never logged
- Docker containers run as non-root user

## Phase 2 Scaling

To scale to 10x+ volume (1M+ posts/day):

- **Migrate Celery → Kafka** for high-throughput message streaming
- **Add Elasticsearch** for full-text search across all collected posts
- **ML model fine-tuning** with labeled attack/non-attack data
- **Redis Cluster** for horizontal cache scaling
- **Kubernetes + HPA** for auto-scaling workers based on queue depth
- **Add Instagram Business API** (owned accounts only — see Limitations)
- **Add TikTok Research API** for academic/business research access

## Limitations

### Instagram

> ⚠️ **Instagram data collection is NOT supported.**

Only owned Instagram accounts can be monitored via the Instagram Graph API. Third-party public monitoring of Instagram posts violates Instagram's Terms of Service. There is no compliant way to monitor public Instagram posts for brand mentions without owning the account.

**Workarounds:**
- Monitor your own brand's Instagram account via Graph API
- Use a paid social listening vendor (Brandwatch, Mention, Sprout Social) that has a data partnership agreement with Meta
