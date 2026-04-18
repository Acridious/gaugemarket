import json
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from database import (get_signals_filtered, get_signal_by_id,
                      get_signal_stats, get_recent_signals)
from datetime import datetime

app = FastAPI(
    title="GaugeMarket API",
    description="Prediction market intelligence feed",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

CATEGORIES = [
    'political', 'macro', 'geopolitical',
    'commodities', 'crypto', 'sports', 'esports', 'other'
]

def get_signal_state(s):
    """
    Honest signal state based on news vacuum AND article timing.

    no_news      — no articles found at all → real information gap
    confirmed    — article found AND it appeared AFTER the bet → bet preceded news
    explained    — article found AND it appeared BEFORE the bet → news explains the move
    uncertain    — article found but timing unclear
    """
    if s.get('news_vacuum'):
        return 'no_news'

    timing = s.get('news_timing', 'unknown')

    if timing == 'after':
        return 'confirmed'      # bet came first — genuinely interesting
    elif timing == 'before':
        return 'explained'      # news already existed — less interesting
    elif timing == 'simultaneous':
        return 'uncertain'      # unclear — show but caveat
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

    try:
        rc = s.get('related_contracts', '[]')
        s['related_contracts'] = json.loads(rc) if isinstance(rc, str) else rc
    except Exception:
        s['related_contracts'] = []

    return s

def deduplicate_signals(signals):
    """
    One card per contract — always shows the most recent move.
    If direction flips from a previous signal, flags as a reversal.

    Volatile markets can generate many signals on the same contract.
    Showing all of them clutters the feed. Most recent is most relevant.
    """
    seen = {}
    for s in signals:
        key = s['question'].strip().lower()
        if key not in seen:
            seen[key] = s
        else:
            existing = seen[key]
            # Keep most recent
            if s['detected_at'] > existing['detected_at']:
                # Flag reversal if direction flipped
                if existing.get('direction') != s.get('direction'):
                    s['is_reversal'] = True
                    s['reversal_from'] = existing['direction']
                    s['reversal_prev_move'] = round(
                        existing.get('price_move', 0) * 100
                    )
                seen[key] = s

    result = list(seen.values())
    result.sort(key=lambda x: x['detected_at'], reverse=True)
    return result

@app.get("/", include_in_schema=False)
def serve_frontend():
    return FileResponse("frontend.html")

@app.get("/feed")
def get_feed(category: str = Query(default=None)):
    # With lean 48h storage, just return everything — never more than ~200 rows
    signals = get_signals_filtered(
        min_score=50,
        category=category,
        limit=500  # effectively no limit
    )
    signals = deduplicate_signals(signals)
    return {
        "feed": [enrich_signal(s) for s in signals],
        "count": len(signals),
        "categories": CATEGORIES,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/signals")
def get_signals(
    limit: int = Query(default=20, le=100),
    category: str = Query(default=None),
    min_score: int = Query(default=50),
    platform: str = Query(default=None)
):
    signals = get_signals_filtered(
        min_score=min_score,
        category=category,
        platform=platform,
        limit=limit
    )
    return {
        "signals": [enrich_signal(s) for s in signals],
        "count": len(signals),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/signals/{signal_id}")
def get_signal(signal_id: int):
    signal = get_signal_by_id(signal_id)
    if not signal:
        return JSONResponse(
            status_code=404,
            content={"error": "Signal not found"}
        )
    return enrich_signal(signal)

@app.get("/stats")
def get_stats():
    stats = get_signal_stats()
    return {
        **stats,
        "platforms_monitored": 2,
        "platforms": ["Polymarket", "Kalshi"],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
def health():
    try:
        get_signal_stats()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"
    return {
        "status": "ok",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }
