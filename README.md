# GaugeMarket Backend

Prediction market intelligence feed. Monitors Polymarket and Kalshi
for unusual contract price movements and surfaces signals before news breaks.

## Deploy to Railway (recommended)

### Step 1 — Push to GitHub

```bash
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/YOURUSERNAME/gaugemarket.git
git push -u origin main
```

### Step 2 — Create Railway project

1. Go to railway.app
2. Sign up with GitHub
3. Click "New Project"
4. Click "Deploy from GitHub repo"
5. Select your gaugemarket repo
6. Railway detects Procfile automatically

### Step 3 — Add PostgreSQL

1. In your Railway project click "New"
2. Click "Database"
3. Click "Add PostgreSQL"
4. Railway injects DATABASE_URL automatically — no config needed

### Step 4 — Set environment variables

In Railway dashboard → your service → Variables:

```
POLL_INTERVAL=300
MIN_PRICE_MOVE=0.05
MIN_SIGNAL_SCORE=50
MIN_VOLUME=1000
```

DATABASE_URL is set automatically by Railway when you add PostgreSQL.

### Step 5 — Deploy

Railway deploys automatically on every git push.

```bash
git add .
git commit -m "update"
git push
```

Done. Railway gives you a URL like:
`https://gaugemarket-production.up.railway.app`

---

## How it works

```
poller.py runs forever (worker process):
  Every 5 minutes:
  1. Pull all active events from Polymarket API
  2. Pull all active markets from Kalshi API
  3. Compare current odds to previous snapshot
  4. Flag movements > 5% in under 30 minutes
  5. Find related contracts in same event + keyword groups
  6. Check Yahoo Finance RSS for news vacuum
  7. Score the signal (0-100)
  8. Store signals scored 50+ in PostgreSQL

api.py runs as web server:
  Serves signals as JSON to your frontend
  GET /feed        → high confidence signals (60+)
  GET /signals     → all signals with filters
  GET /stats       → daily statistics
  GET /health      → health check
  GET /docs        → auto-generated API docs
```

---

## Signal scoring

| Factor | Points |
|--------|--------|
| Price move > 30% | 40 |
| Price move > 20% | 30 |
| Price move > 10% | 20 |
| Price move > 5% | 10 |
| Move in under 5 mins | 25 |
| Move in under 15 mins | 20 |
| Move in under 30 mins | 10 |
| Cross-platform confirmation | 20 |
| Related markets moving | 8-15 |
| News vacuum | 10 |

Signals 50+ are stored. Feed shows 60+.

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| GET /feed | Main feed — high confidence signals |
| GET /signals | All signals with filters |
| GET /signals/:id | Single signal |
| GET /stats | Daily statistics |
| GET /health | Health check |
| GET /docs | Auto docs |

### Query parameters

```
/feed?category=political    filter by category
/feed?limit=5               limit results
/signals?min_score=70       high confidence only
/signals?platform=Polymarket platform filter
```

---

## Categories

political · macro · geopolitical · commodities · crypto · sports · esports · other

---

## Cost on Railway

```
Web service (API):     ~$2-3/mo
Worker (poller):       ~$2-3/mo
PostgreSQL:            Free tier (1GB)
─────────────────────────────────
Total:                 ~$5/mo (hobby plan)
```

Break even: 1 paying user at $29/mo.

---

## Files

```
poller.py        runs forever, pulls APIs, detects signals
api.py           FastAPI server, serves signals as JSON
database.py      PostgreSQL connection and queries
news.py          Yahoo Finance RSS checker + keyword grouping
requirements.txt Python dependencies
Procfile         tells Railway what to run
```
