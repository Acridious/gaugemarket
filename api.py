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

def enrich_signal(s):
    import json
    s['confidence'] = (
        'extreme' if s['score'] >= 80
        else 'high' if s['score'] >= 70
        else 'medium' if s['score'] >= 60
        else 'low'
    )
    s['state'] = (
        'no_news' if s.get('news_vacuum')
        else 'confirmed'
    )
    # Parse related contracts JSON
    try:
        rc = s.get('related_contracts', '[]')
        s['related_contracts'] = json.loads(rc) if isinstance(rc, str) else rc
    except:
        s['related_contracts'] = []
    return s

@app.get("/", include_in_schema=False)
def serve_frontend():
    return FileResponse("frontend.html")

@app.get("/feed")
def get_feed(
    limit: int = Query(default=10, le=50),
    category: str = Query(default=None)
):
    signals = get_signals_filtered(
        min_score=50,
        category=category,
        limit=limit
    )
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
