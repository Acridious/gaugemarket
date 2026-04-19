import json
import os
from fastapi import FastAPI, Query, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from database import (
    get_signals_filtered, get_signal_by_id,
    get_signal_stats, get_recent_signals,
    get_volume_stats, get_price_history,
    get_signals_historical,
)
from constants import ALL_CATEGORIES, SAME_EVENT_CATEGORIES
from datetime import datetime

app = FastAPI(
    title="GaugeMarket API",
    description="Prediction market intelligence feed",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# CORS — locked to your frontend domain in production
# Set ALLOWED_ORIGINS in Railway env vars, comma-separated:
# e.g. "https://gaugemarket.vercel.app,https://gaugemarket.com"
# Falls back to localhost only if not set — never open in production
# ---------------------------------------------------------------------------
_origins_env = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:3000')
ALLOWED_ORIGINS = [o.strip() for o in _origins_env.split(',') if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# API key authentication
# ---------------------------------------------------------------------------
# Set API_KEY in Railway env vars — generate with:
#   python3 -c "import secrets; print(secrets.token_urlsafe(32))"
# Frontend sends it as: X-API-Key: <key>
# Public endpoints (/, /health, /waitlist) are exempt
# ---------------------------------------------------------------------------
API_KEY = os.environ.get('API_KEY', '')

PUBLIC_PATHS = {'/', '/health', '/waitlist', '/docs', '/openapi.json'}

async def require_api_key(request: Request):
    if not API_KEY:
        return  # API_KEY not set — open (dev mode)
    if request.url.path in PUBLIC_PATHS:
        return  # exempt
    key = request.headers.get('X-API-Key', '')
    if key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. "
                   "Set X-API-Key header with your GaugeMarket API key."
        )

# Wire key check as middleware so it applies to ALL routes automatically
@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    if API_KEY and request.url.path not in PUBLIC_PATHS:
        key = request.headers.get('X-API-Key', '')
        if key != API_KEY:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key"}
            )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Signal enrichment
# ---------------------------------------------------------------------------

def get_signal_state(s):
    """
    Honest signal state used for card colouring and labels.

    terminal     — sports/esports contract that has resolved (match over)
    no_news      — no articles found → genuine information gap
    confirmed    — article appeared AFTER the bet → bet preceded news
    explained    — article appeared BEFORE the bet → news explains the move
    uncertain    — article found but timing unclear
    """
    # Sports terminal state takes priority — makes it obvious this is
    # a resolved contract, not a live signal
    if s.get('is_terminal'):
        return 'terminal'

    if s.get('news_vacuum'):
        return 'no_news'

    timing = s.get('news_timing', 'unknown')

    if timing == 'after':
        return 'confirmed'
    elif timing == 'before':
        return 'explained'
    elif timing == 'simultaneous':
        return 'uncertain'
    else:
        return 'uncertain'


def enrich_signal(s):
    s['confidence'] = (
        'extreme' if s['score'] >= 80
        else 'high' if s['score'] >= 70
        else 'medium' if s['score'] >= 60
        else 'low'
    )
    s['state'] = get_signal_state(s)

    # Boolean coercion (stored as 0/1 in Postgres)
    s['is_terminal'] = bool(s.get('is_terminal', 0))

    # sports_context is stored in the signal but may be absent on older rows
    s['sports_context'] = s.get('sports_context') or None
    s['ai_summary'] = s.get('ai_summary') or None
    s['background_headline'] = s.get('background_headline') or None
    s['background_source']   = s.get('background_source') or None
    s['background_url']      = s.get('background_url') or None

    # Build news_articles array from stored fields for frontend list rendering
    # Currently we store up to 3 articles in news_articles_json (if present)
    # or fall back to the single news_headline/source/url fields
    try:
        import json as _json
        raw = s.get('news_articles_json')
        s['news_articles'] = _json.loads(raw) if raw else (
            [{
                'headline': s['news_headline'],
                'source':   s['news_source'],
                'url':      s['news_url'],
                'timing':   s.get('news_timing', 'unknown'),
            }] if s.get('news_headline') else []
        )
    except Exception:
        s['news_articles'] = []

    # Category context for the frontend
    s['is_sports'] = s.get('category') in SAME_EVENT_CATEGORIES

    try:
        rc = s.get('related_contracts', '[]')
        s['related_contracts'] = json.loads(rc) if isinstance(rc, str) else rc
    except Exception:
        s['related_contracts'] = []

    return s


def deduplicate_signals(signals):
    """
    Two-level deduplication:

    1. Same question (same contract, different polls) — keep most recent.
       Detects reversals: if direction flipped, flags is_reversal.

    2. Same event_id (same game, different contracts) — keep highest-scoring
       signal as primary card; merge others as related_contracts.
       Fixes the Kyoto Sanga / Cerezo Osaka duplicate-card problem.
    """
    # Step 1: deduplicate by question
    by_question = {}
    for s in signals:
        key = s['question'].strip().lower()
        if key not in by_question:
            by_question[key] = s
        else:
            existing = by_question[key]
            if s['detected_at'] > existing['detected_at']:
                if existing.get('direction') != s.get('direction'):
                    s['is_reversal'] = True
                    s['reversal_from'] = existing['direction']
                    s['reversal_prev_move'] = round(
                        existing.get('price_move', 0) * 100
                    )
                by_question[key] = s

    deduped = list(by_question.values())

    # Step 2: deduplicate by event_id
    by_event = {}
    for s in deduped:
        event_id = s.get('event_id', '')
        if not event_id or event_id == 'unknown':
            # No event_id — use question as standalone key to avoid collisions
            standalone_key = f"__standalone__{s['question']}"
            by_event[standalone_key] = s
            continue

        if event_id not in by_event:
            by_event[event_id] = s
        else:
            existing = by_event[event_id]
            if s['score'] > existing['score']:
                # New signal scores higher — make it primary, demote existing
                try:
                    existing_rc = json.loads(
                        existing.get('related_contracts', '[]')
                        if isinstance(existing.get('related_contracts'), str)
                        else '[]'
                    )
                except Exception:
                    existing_rc = []

                existing_rc.insert(0, {
                    'question':    existing['question'],
                    'odds':        existing['current_odds'],
                    'prev_odds':   existing['prev_odds'],
                    'platform':    existing['platform'],
                    'event_title': existing.get('event_title', ''),
                    'type':        'same_event',
                })

                try:
                    new_rc = json.loads(
                        s.get('related_contracts', '[]')
                        if isinstance(s.get('related_contracts'), str)
                        else '[]'
                    )
                except Exception:
                    new_rc = []

                seen_q = {c['question'] for c in new_rc}
                for c in existing_rc:
                    if c['question'] not in seen_q:
                        new_rc.append(c)

                s['related_contracts'] = new_rc
                by_event[event_id] = s
            else:
                # Keep existing as primary
                try:
                    rc = json.loads(
                        existing.get('related_contracts', '[]')
                        if isinstance(existing.get('related_contracts'), str)
                        else '[]'
                    )
                except Exception:
                    rc = []

                if not any(c.get('question') == s['question'] for c in rc):
                    rc.append({
                        'question':    s['question'],
                        'odds':        s['current_odds'],
                        'prev_odds':   s['prev_odds'],
                        'platform':    s['platform'],
                        'event_title': s.get('event_title', ''),
                        'type':        'same_event',
                    })
                existing['related_contracts'] = rc

    result = list(by_event.values())
    result.sort(key=lambda x: x['detected_at'], reverse=True)
    return result


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    from database import setup_waitlist
    setup_waitlist()


@app.get("/", include_in_schema=False)
def serve_frontend():
    # Inject API key and URL server-side so they never live in the repo.
    # The browser gets a fully rendered page with credentials embedded,
    # but git only sees the placeholder variables.
    try:
        with open("frontend.html", "r") as f:
            html = f.read()
        api_url = os.environ.get("PUBLIC_API_URL", "")
        api_key = API_KEY or ""
        # Replace the placeholder variables with real values
        html = html.replace(
            "window.__GM_API_URL__ || 'https://web-production-bde4.up.railway.app'",
            f"'{api_url}'" if api_url else "'https://web-production-d8329.up.railway.app'"
        )
        html = html.replace(
            "window.__GM_API_KEY__ || ''",
            f"'{api_key}'"
        )
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=html)
    except Exception:
        return FileResponse("frontend.html")


@app.post("/waitlist")
async def join_waitlist(request: Request):
    """Public endpoint — no API key required. Accepts {email, name?}."""
    from database import save_waitlist_entry
    try:
        body = await request.json()
        email = (body.get('email') or '').strip().lower()
        name  = (body.get('name') or '').strip()
        if not email or '@' not in email:
            raise HTTPException(status_code=400, detail="Valid email required")
        result = save_waitlist_entry(email, name)
        if result == 'duplicate':
            return {"status": "already_registered", "message": "You're already on the list."}
        return {"status": "ok", "message": "You're on the list. We'll be in touch."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/waitlist/count")
async def waitlist_count(request: Request, _=Depends(require_api_key)):
    """Protected — shows waitlist size to admins."""
    from database import get_waitlist_count
    return {"count": get_waitlist_count()}


@app.get("/feed")
def get_feed(category: str = Query(default=None)):
    signals = get_signals_filtered(
        min_score=50,
        category=category,
        limit=500,
    )
    signals = deduplicate_signals(signals)
    return {
        "feed":       [enrich_signal(s) for s in signals],
        "count":      len(signals),
        "categories": ALL_CATEGORIES,
        "timestamp":  datetime.utcnow().isoformat(),
    }


@app.get("/signals")
def get_signals(
    limit:    int = Query(default=20, le=100),
    category: str = Query(default=None),
    min_score: int = Query(default=50),
    platform: str = Query(default=None),
):
    signals = get_signals_filtered(
        min_score=min_score,
        category=category,
        platform=platform,
        limit=limit,
    )
    return {
        "signals":   [enrich_signal(s) for s in signals],
        "count":     len(signals),
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/signals/{signal_id}")
def get_signal(signal_id: int):
    signal = get_signal_by_id(signal_id)
    if not signal:
        return JSONResponse(
            status_code=404,
            content={"error": "Signal not found"},
        )
    return enrich_signal(signal)


@app.get("/stats")
def get_stats():
    stats  = get_signal_stats()
    volume = get_volume_stats()
    return {
        **stats,
        "total_volume_monitored": volume['total_volume'],
        "market_count":           volume['market_count'],
        "platforms_monitored":    1,
        "platforms":              ["Polymarket"],
        "timestamp":              datetime.utcnow().isoformat(),
    }


@app.get("/history/{market_id}")
def get_market_history(market_id: str, hours: int = Query(default=168, le=168)):
    """Price history for a market — used for sparklines. Up to 7 days."""
    history = get_price_history(market_id, hours=hours)
    return {
        "market_id": market_id,
        "history":   history,
        "count":     len(history),
    }


@app.get("/signals/history")
def get_signal_history(
    limit:    int = Query(default=50, le=200),
    offset:   int = Query(default=0),
    category: str = Query(default=None),
    days_back: int = Query(default=30, le=30),
    min_score: int = Query(default=60),
):
    """Historical signal feed for the History tab. Up to 30 days."""
    signals = get_signals_historical(
        min_score=min_score,
        category=category,
        days_back=days_back,
        limit=limit,
        offset=offset,
    )
    return {
        "signals":   [enrich_signal(s) for s in signals],
        "count":     len(signals),
        "days_back": days_back,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/usage")
def get_usage():
    """Real-time Groq budget usage for the current day and last poll cycle."""
    try:
        from groq_client import _daily_total, DAILY_CAP, _usage, BUDGET, budget_summary
        return {
            "groq": {
                "daily_used":  _daily_total,
                "daily_cap":   DAILY_CAP,
                "daily_pct":   round(_daily_total / DAILY_CAP * 100, 1),
                "last_poll":   {k: {"used": _usage[k], "limit": BUDGET[k]}
                                for k in BUDGET},
                "summary":     budget_summary(),
            },
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/health")
def health():
    try:
        get_signal_stats()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"
    return {
        "status":    "ok",
        "database":  db_status,
        "timestamp": datetime.utcnow().isoformat(),
    }
