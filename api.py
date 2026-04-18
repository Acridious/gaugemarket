import json
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from database import (
    get_signals_filtered, get_signal_by_id,
    get_signal_stats, get_recent_signals,
    get_volume_stats,
)
from constants import ALL_CATEGORIES, SAME_EVENT_CATEGORIES
from datetime import datetime

app = FastAPI(
    title="GaugeMarket API",
    description="Prediction market intelligence feed",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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

@app.get("/", include_in_schema=False)
def serve_frontend():
    return FileResponse("frontend.html")


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
