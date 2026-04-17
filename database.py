import os
import pg8000.native
from datetime import datetime
from urllib.parse import urlparse

def get_connection():
    url = os.environ.get('DATABASE_URL', '')
    if not url:
        raise Exception("DATABASE_URL environment variable is not set. "
                       "Add PostgreSQL to your Railway project.")
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    parsed = urlparse(url)
    conn = pg8000.native.Connection(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        user=parsed.username,
        password=parsed.password,
        ssl_context=True
    )
    return conn

def setup_db():
    conn = get_connection()

    conn.run('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id SERIAL PRIMARY KEY,
            market_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_title TEXT NOT NULL,
            question TEXT NOT NULL,
            odds REAL NOT NULL,
            volume REAL NOT NULL,
            platform TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')

    conn.run('''
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL,
            event_title TEXT NOT NULL,
            question TEXT NOT NULL,
            platform TEXT NOT NULL,
            prev_odds REAL NOT NULL,
            current_odds REAL NOT NULL,
            price_move REAL NOT NULL,
            direction TEXT NOT NULL,
            volume REAL NOT NULL,
            score INTEGER NOT NULL,
            related_same_event INTEGER DEFAULT 0,
            related_cross_event INTEGER DEFAULT 0,
            news_vacuum INTEGER DEFAULT 1,
            news_headline TEXT,
            news_source TEXT,
            news_url TEXT,
            detected_at TEXT NOT NULL,
            category TEXT DEFAULT 'uncategorised',
            related_contracts TEXT DEFAULT '[]',
            news_timing TEXT DEFAULT 'unknown',
            market_url TEXT
        )
    ''')

    # Cross-event candidates table
    # Stores pairs of signals from different events detected close together
    # Grouper validates these with Groq every 30 mins
    conn.run('''
        CREATE TABLE IF NOT EXISTS cross_event_candidates (
            id SERIAL PRIMARY KEY,
            signal_id_a INTEGER NOT NULL,
            signal_id_b INTEGER NOT NULL,
            question_a TEXT NOT NULL,
            question_b TEXT NOT NULL,
            event_title_a TEXT NOT NULL,
            event_title_b TEXT NOT NULL,
            platform_a TEXT NOT NULL,
            platform_b TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            validated INTEGER DEFAULT 0,
            is_related INTEGER DEFAULT NULL
        )
    ''')

    conn.run('''
        CREATE INDEX IF NOT EXISTS idx_snapshots_market_id 
        ON snapshots(market_id)
    ''')
    conn.run('''
        CREATE INDEX IF NOT EXISTS idx_signals_detected 
        ON signals(detected_at)
    ''')
    conn.run('''
        CREATE INDEX IF NOT EXISTS idx_candidates_validated
        ON cross_event_candidates(validated)
    ''')

    conn.close()
    print("Database ready")

def save_snapshot(market_id, event_id, event_title,
                  question, odds, volume, platform):
    conn = get_connection()
    conn.run('''
        INSERT INTO snapshots 
        (market_id, event_id, event_title, question, 
         odds, volume, platform, timestamp)
        VALUES (:market_id, :event_id, :event_title, :question,
                :odds, :volume, :platform, :timestamp)
    ''', market_id=market_id, event_id=event_id,
        event_title=event_title, question=question,
        odds=odds, volume=volume, platform=platform,
        timestamp=datetime.now().isoformat())
    conn.close()

def get_last_snapshot(market_id):
    conn = get_connection()
    rows = conn.run('''
        SELECT odds, volume, timestamp 
        FROM snapshots 
        WHERE market_id = :market_id
        ORDER BY timestamp DESC 
        LIMIT 1
    ''', market_id=market_id)
    conn.close()
    if rows:
        return {'odds': rows[0][0], 'volume': rows[0][1],
                'timestamp': rows[0][2]}
    return None

def save_signal(signal_data):
    conn = get_connection()
    rows = conn.run('''
        INSERT INTO signals (
            event_id, event_title, question, platform,
            prev_odds, current_odds, price_move, direction,
            volume, score, related_same_event, related_cross_event,
            news_vacuum, news_headline, news_source, news_url,
            detected_at, category, related_contracts, news_timing, market_url
        ) VALUES (
            :event_id, :event_title, :question, :platform,
            :prev_odds, :current_odds, :price_move, :direction,
            :volume, :score, :related_same_event, :related_cross_event,
            :news_vacuum, :news_headline, :news_source, :news_url,
            :detected_at, :category, :related_contracts, :news_timing, :market_url
        )
        RETURNING id
    ''',
        event_id=signal_data['event_id'],
        event_title=signal_data['event_title'],
        question=signal_data['question'],
        platform=signal_data['platform'],
        prev_odds=signal_data['prev_odds'],
        current_odds=signal_data['current_odds'],
        price_move=signal_data['price_move'],
        direction=signal_data['direction'],
        volume=signal_data['volume'],
        score=signal_data['score'],
        related_same_event=signal_data.get('related_same_event', 0),
        related_cross_event=signal_data.get('related_cross_event', 0),
        news_vacuum=1 if signal_data.get('news_vacuum', True) else 0,
        news_headline=signal_data.get('news_headline'),
        news_source=signal_data.get('news_source'),
        news_url=signal_data.get('news_url'),
        detected_at=datetime.now().isoformat(),
        category=signal_data.get('category', 'uncategorised'),
        related_contracts=signal_data.get('related_contracts', '[]'),
        news_timing=signal_data.get('news_timing', 'unknown'),
        market_url=signal_data.get('market_url')
    )
    # RETURNING id gives us the new row's id directly — no lastval() needed
    signal_id = rows[0][0] if rows else None
    conn.close()
    return signal_id

def save_cross_event_candidate(signal_id_a, signal_id_b,
                                question_a, question_b,
                                event_title_a, event_title_b,
                                platform_a, platform_b):
    """
    Save a pair of signals from different events as a cross-event candidate.
    The grouper will validate these with Groq every 30 mins.
    """
    conn = get_connection()
    # Check if this pair already exists to avoid duplicates
    existing = conn.run('''
        SELECT id FROM cross_event_candidates
        WHERE (signal_id_a = :a AND signal_id_b = :b)
           OR (signal_id_a = :b AND signal_id_b = :a)
    ''', a=signal_id_a, b=signal_id_b)

    if not existing:
        conn.run('''
            INSERT INTO cross_event_candidates (
                signal_id_a, signal_id_b,
                question_a, question_b,
                event_title_a, event_title_b,
                platform_a, platform_b,
                detected_at
            ) VALUES (
                :signal_id_a, :signal_id_b,
                :question_a, :question_b,
                :event_title_a, :event_title_b,
                :platform_a, :platform_b,
                :detected_at
            )
        ''',
            signal_id_a=signal_id_a,
            signal_id_b=signal_id_b,
            question_a=question_a,
            question_b=question_b,
            event_title_a=event_title_a,
            event_title_b=event_title_b,
            platform_a=platform_a,
            platform_b=platform_b,
            detected_at=datetime.now().isoformat()
        )
    conn.close()

def get_recent_signals_for_grouping(mins=35):
    """Get recent signals for cross-event candidate collection."""
    conn = get_connection()
    cutoff = (datetime.now() - 
              __import__('datetime').timedelta(minutes=mins)
              ).isoformat()
    rows = conn.run('''
        SELECT id, event_id, event_title, question, 
               platform, category, score
        FROM signals
        WHERE detected_at > :cutoff
        ORDER BY detected_at DESC
        LIMIT 100
    ''', cutoff=cutoff)
    columns = [c['name'] for c in conn.columns]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_unvalidated_candidates(limit=50):
    """Get cross-event candidates that haven't been validated yet."""
    conn = get_connection()
    rows = conn.run('''
        SELECT * FROM cross_event_candidates
        WHERE validated = 0
        ORDER BY detected_at DESC
        LIMIT :limit
    ''', limit=limit)
    columns = [c['name'] for c in conn.columns]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def mark_candidate_validated(candidate_id, is_related):
    """Mark a candidate as validated with Groq's verdict."""
    conn = get_connection()
    conn.run('''
        UPDATE cross_event_candidates
        SET validated = 1, is_related = :is_related
        WHERE id = :id
    ''', is_related=1 if is_related else 0, id=candidate_id)
    conn.close()

def get_signals_filtered(min_score=50, category=None,
                         platform=None, limit=20):
    conn = get_connection()
    query = "SELECT * FROM signals WHERE score >= :min_score"
    params = {'min_score': min_score}
    if category and category != 'all':
        query += " AND category = :category"
        params['category'] = category
    if platform:
        query += " AND platform = :platform"
        params['platform'] = platform
    query += " ORDER BY detected_at DESC LIMIT :limit"
    params['limit'] = limit
    rows = conn.run(query, **params)
    columns = [c['name'] for c in conn.columns]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_signal_by_id(signal_id):
    conn = get_connection()
    rows = conn.run(
        "SELECT * FROM signals WHERE id = :signal_id",
        signal_id=signal_id
    )
    columns = [c['name'] for c in conn.columns]
    conn.close()
    if rows:
        return dict(zip(columns, rows[0]))
    return None

def get_recent_signals(limit=20):
    return get_signals_filtered(min_score=0, limit=limit)

def cleanup_old_snapshots(hours=3):
    """Delete snapshots older than N hours — keeps DB small.
    
    Timestamp stored as TEXT so we compare as text using ISO format.
    cutoff = datetime N hours ago formatted as ISO string.
    """
    conn = get_connection()
    try:
        cutoff = (datetime.now() - 
                  __import__('datetime').timedelta(hours=hours)
                  ).isoformat()
        conn.run(
            "DELETE FROM snapshots WHERE timestamp < :cutoff",
            cutoff=cutoff
        )
        print(f"Cleaned up old snapshots (kept last {hours}h)")
    except Exception as e:
        print(f"Cleanup error (non-fatal): {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_signal_stats():
    conn = get_connection()
    total = conn.run("SELECT COUNT(*) FROM signals")[0][0]
    today = conn.run('''
        SELECT COUNT(*) FROM signals 
        WHERE detected_at::date = CURRENT_DATE
    ''')[0][0]
    high_score = conn.run('''
        SELECT COUNT(*) FROM signals 
        WHERE score >= 70 AND detected_at::date = CURRENT_DATE
    ''')[0][0]
    avg_result = conn.run('''
        SELECT AVG(score) FROM signals
        WHERE detected_at::date = CURRENT_DATE
    ''')
    avg_score = avg_result[0][0] if avg_result else 0
    conn.close()
    return {
        'total': total,
        'today': today,
        'high_confidence': high_score,
        'avg_score': round(float(avg_score or 0), 1)
    }
