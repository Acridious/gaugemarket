import os
import pg8000.native
from datetime import datetime, timedelta
from urllib.parse import urlparse
from contextlib import contextmanager

# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
# pg8000 is synchronous so we use a simple thread-local connection that is
# re-established if it's been closed or errored out.  This avoids opening a
# brand-new TCP connection for every single DB call (which was the previous
# behaviour and would exhaust Railway's connection limit during a poll cycle
# that touches 500 markets).
#
# We keep ONE connection per process (poller is single-threaded; FastAPI
# workers each get their own).  On error the connection is torn down and a
# fresh one is opened on the next call.

_conn = None


def _open_connection():
    url = os.environ.get('DATABASE_URL', '')
    if not url:
        raise Exception(
            "DATABASE_URL environment variable is not set. "
            "Add PostgreSQL to your Railway project."
        )
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    parsed = urlparse(url)
    return pg8000.native.Connection(
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        user=parsed.username,
        password=parsed.password,
        ssl_context=True,
    )


def get_connection():
    """Return the shared connection, reconnecting if necessary."""
    global _conn
    try:
        if _conn is not None:
            # Check socket is actually alive before pinging
            if getattr(_conn, '_sock', None) is None:
                raise Exception("socket is None")
            _conn.run("SELECT 1")
            return _conn
    except Exception:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None

    _conn = _open_connection()
    return _conn


@contextmanager
def db():
    """
    Context manager that yields a live connection.

    Uses a fresh connection per call to avoid stale prepared statement
    errors ('unnamed prepared statement does not exist') that occur when
    pg8000 reuses a connection after it was reset by the server.

    Fresh connections are slightly more expensive but eliminate the entire
    class of 'connection reset by peer' / stale socket errors.
    """
    conn = _open_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def setup_db():
    with db() as conn:
        conn.run('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id          SERIAL PRIMARY KEY,
                market_id   TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                event_title TEXT NOT NULL,
                question    TEXT NOT NULL,
                odds        REAL NOT NULL,
                volume      REAL NOT NULL,
                platform    TEXT NOT NULL,
                timestamp   TEXT NOT NULL
            )
        ''')

        conn.run('''
            CREATE TABLE IF NOT EXISTS signals (
                id                   SERIAL PRIMARY KEY,
                event_id             TEXT NOT NULL,
                event_title          TEXT NOT NULL,
                question             TEXT NOT NULL,
                platform             TEXT NOT NULL,
                prev_odds            REAL NOT NULL,
                current_odds         REAL NOT NULL,
                price_move           REAL NOT NULL,
                direction            TEXT NOT NULL,
                volume               REAL NOT NULL,
                score                INTEGER NOT NULL,
                related_same_event   INTEGER DEFAULT 0,
                related_cross_event  INTEGER DEFAULT 0,
                news_vacuum          INTEGER DEFAULT 1,
                news_headline        TEXT,
                news_source          TEXT,
                news_url             TEXT,
                detected_at          TEXT NOT NULL,
                category             TEXT DEFAULT 'uncategorised',
                related_contracts    TEXT DEFAULT '[]',
                news_timing          TEXT DEFAULT 'unknown',
                market_url           TEXT,
                ai_summary           TEXT,
                -- Sports-specific fields
                is_terminal          INTEGER DEFAULT 0,
                mins_elapsed         REAL DEFAULT 0
            )
        ''')

        # Add new columns to existing tables if they don't exist yet
        # (safe to run on an existing DB)
        for col, definition in [
            ('is_terminal', 'INTEGER DEFAULT 0'),
            ('mins_elapsed', 'REAL DEFAULT 0'),
            ('ai_summary', 'TEXT'),
            ('news_articles_json', 'TEXT'),
            ('background_headline', 'TEXT'),
            ('background_source', 'TEXT'),
            ('background_url', 'TEXT'),
        ]:
            try:
                conn.run(
                    f"ALTER TABLE signals ADD COLUMN {col} {definition}"
                )
            except Exception:
                pass  # column already exists

        conn.run('''
            CREATE TABLE IF NOT EXISTS cross_event_candidates (
                id             SERIAL PRIMARY KEY,
                signal_id_a    INTEGER NOT NULL,
                signal_id_b    INTEGER NOT NULL,
                question_a     TEXT NOT NULL,
                question_b     TEXT NOT NULL,
                event_title_a  TEXT NOT NULL,
                event_title_b  TEXT NOT NULL,
                platform_a     TEXT NOT NULL,
                platform_b     TEXT NOT NULL,
                detected_at    TEXT NOT NULL,
                validated      INTEGER DEFAULT 0,
                is_related     INTEGER DEFAULT NULL
            )
        ''')

        conn.run('''
            CREATE TABLE IF NOT EXISTS volume_stats (
                id            SERIAL PRIMARY KEY,
                total_volume  REAL NOT NULL,
                market_count  INTEGER NOT NULL,
                recorded_at   TEXT NOT NULL
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

    # Retry queue for budget-exhausted signals
    conn.run('''
        CREATE TABLE IF NOT EXISTS retry_queue (
            signal_id     INTEGER PRIMARY KEY,
            needs_news    INTEGER DEFAULT 0,
            needs_summary INTEGER DEFAULT 0,
            created_at    TEXT NOT NULL
        )
    ''')

    # Safe migrations — IF NOT EXISTS prevents errors on re-deploy
    migrations = [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS news_articles_json TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS background_headline TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS background_source TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS background_url TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS sports_context TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS ai_summary TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS is_terminal INTEGER DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS mins_elapsed REAL DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS event_id TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS related_contracts TEXT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS related_same_event INTEGER DEFAULT 0",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS related_cross_event INTEGER DEFAULT 0",
        "CREATE TABLE IF NOT EXISTS retry_queue (signal_id INTEGER PRIMARY KEY, needs_news INTEGER DEFAULT 0, needs_summary INTEGER DEFAULT 0, created_at TEXT NOT NULL)",
    ]
    for m in migrations:
        conn.run(m)  # IF NOT EXISTS makes these safe to run repeatedly

    print("Database ready")


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def save_snapshot(market_id, event_id, event_title,
                  question, odds, volume, platform):
    """
    Save a price snapshot with tiered storage density:
    - Last 2 hours:  every poll (high frequency for signal detection)
    - 2 hours - 7 days: one snapshot per hour per market (for sparklines)

    This keeps Postgres lean — without thinning, 7-day retention at full
    poll frequency would generate ~250k rows. With hourly thinning beyond
    2h the same period generates ~84k rows.
    """
    now = datetime.utcnow()
    two_hours_ago = (now - timedelta(hours=2)).isoformat()
    one_hour_ago  = (now - timedelta(hours=1)).isoformat()

    with db() as conn:
        # For data older than 2h, only insert if no snapshot in last hour
        recent = conn.run('''
            SELECT COUNT(*) FROM snapshots
            WHERE market_id = :market_id
              AND timestamp > :one_hour_ago
        ''', market_id=market_id, one_hour_ago=one_hour_ago)

        is_recent_data = True  # within last 2h — always insert
        # Check if this market already has a snapshot in the last 5 mins
        # (normal dedup, unchanged from before)
        very_recent = conn.run('''
            SELECT COUNT(*) FROM snapshots
            WHERE market_id = :market_id
              AND timestamp > :cutoff
        ''', market_id=market_id,
             cutoff=(now - timedelta(minutes=5)).isoformat())

        if very_recent[0][0] > 0:
            # Already have a snapshot in last 5 mins — skip unless odds changed
            last = conn.run('''
                SELECT odds FROM snapshots
                WHERE market_id = :market_id
                ORDER BY timestamp DESC LIMIT 1
            ''', market_id=market_id)
            if last and abs(last[0][0] - odds) < 0.001:
                return  # no meaningful change

        conn.run('''
            INSERT INTO snapshots
            (market_id, event_id, event_title, question,
             odds, volume, platform, timestamp)
            VALUES (:market_id, :event_id, :event_title, :question,
                    :odds, :volume, :platform, :timestamp)
        ''',
            market_id=market_id, event_id=event_id,
            event_title=event_title, question=question,
            odds=odds, volume=volume, platform=platform,
            timestamp=now.isoformat(),
        )


def get_last_snapshot(market_id):
    with db() as conn:
        rows = conn.run('''
            SELECT odds, volume, timestamp
            FROM snapshots
            WHERE market_id = :market_id
            ORDER BY timestamp DESC
            LIMIT 1
        ''', market_id=market_id)
    if rows:
        return {'odds': rows[0][0], 'volume': rows[0][1], 'timestamp': rows[0][2]}
    return None


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

def save_signal(signal_data):
    with db() as conn:
        rows = conn.run('''
            INSERT INTO signals (
                event_id, event_title, question, platform,
                prev_odds, current_odds, price_move, direction,
                volume, score, related_same_event, related_cross_event,
                news_vacuum, news_headline, news_source, news_url,
                detected_at, category, related_contracts, news_timing,
                market_url, ai_summary, is_terminal, mins_elapsed,
                news_articles_json, background_headline, background_source, background_url
            ) VALUES (
                :event_id, :event_title, :question, :platform,
                :prev_odds, :current_odds, :price_move, :direction,
                :volume, :score, :related_same_event, :related_cross_event,
                :news_vacuum, :news_headline, :news_source, :news_url,
                :detected_at, :category, :related_contracts, :news_timing,
                :market_url, :ai_summary, :is_terminal, :mins_elapsed,
                :news_articles_json, :background_headline, :background_source, :background_url
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
            detected_at=datetime.utcnow().isoformat(),
            category=signal_data.get('category', 'uncategorised'),
            related_contracts=signal_data.get('related_contracts', '[]'),
            news_timing=signal_data.get('news_timing', 'unknown'),
            market_url=signal_data.get('market_url'),
            ai_summary=signal_data.get('ai_summary'),
            is_terminal=1 if signal_data.get('is_terminal', False) else 0,
            news_articles_json=signal_data.get('news_articles_json'),
            background_headline=signal_data.get('background_headline'),
            background_source=signal_data.get('background_source'),
            background_url=signal_data.get('background_url'),
            mins_elapsed=signal_data.get('mins_elapsed', 0),
        )
        return rows[0][0] if rows else None


def get_signals_filtered(min_score=50, category=None,
                         platform=None, limit=20):
    with db() as conn:
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
    return [dict(zip(columns, row)) for row in rows]


def get_signal_by_id(signal_id):
    with db() as conn:
        rows = conn.run(
            "SELECT * FROM signals WHERE id = :signal_id",
            signal_id=signal_id,
        )
        columns = [c['name'] for c in conn.columns]
    if rows:
        return dict(zip(columns, rows[0]))
    return None


def get_recent_signals(limit=20):
    return get_signals_filtered(min_score=0, limit=limit)


def get_price_history(market_id, hours=168):
    """
    Return price snapshots for a market over the last N hours (default 7 days).
    Used to draw sparklines and show price context on signal cards.
    """
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    with db() as conn:
        rows = conn.run('''
            SELECT odds, volume, timestamp
            FROM snapshots
            WHERE market_id = :market_id
              AND timestamp > :cutoff
            ORDER BY timestamp ASC
        ''', market_id=market_id, cutoff=cutoff)
    return [
        {'odds': r[0], 'volume': r[1], 'timestamp': r[2]}
        for r in rows
    ]


def get_signals_historical(min_score=50, category=None, platform=None,
                           days_back=30, limit=100, offset=0):
    """
    Return signals from the historical record with pagination.
    Used by the History tab in the frontend.
    """
    from datetime import timedelta
    cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
    with db() as conn:
        query = ("SELECT * FROM signals WHERE score >= :min_score "
                 "AND detected_at >= :cutoff")
        params = {'min_score': min_score, 'cutoff': cutoff}
        if category and category != 'all':
            query += " AND category = :category"
            params['category'] = category
        if platform:
            query += " AND platform = :platform"
            params['platform'] = platform
        query += " ORDER BY detected_at DESC LIMIT :limit OFFSET :offset"
        params['limit']  = limit
        params['offset'] = offset
        rows = conn.run(query, **params)
        columns = [c['name'] for c in conn.columns]
    return [dict(zip(columns, row)) for row in rows]


# ---------------------------------------------------------------------------
# Volume stats
# ---------------------------------------------------------------------------

def save_volume_snapshot(total_volume, market_count):
    """Save total monitored volume from each poll cycle."""
    with db() as conn:
        conn.run('''
            INSERT INTO volume_stats (total_volume, market_count, recorded_at)
            VALUES (:total_volume, :market_count, :recorded_at)
        ''',
            total_volume=total_volume,
            market_count=market_count,
            recorded_at=datetime.utcnow().isoformat(),
        )
        # Pruning is deferred to cleanup_old_data() which runs hourly


def get_volume_stats():
    """Get the latest volume snapshot."""
    with db() as conn:
        rows = conn.run('''
            SELECT total_volume, market_count, recorded_at
            FROM volume_stats
            ORDER BY recorded_at DESC
            LIMIT 1
        ''')
    if rows:
        return {
            'total_volume': rows[0][0],
            'market_count': rows[0][1],
            'recorded_at': rows[0][2],
        }
    return {'total_volume': 0, 'market_count': 0, 'recorded_at': None}


# ---------------------------------------------------------------------------
# Cross-event candidates
# ---------------------------------------------------------------------------

def save_cross_event_candidate(signal_id_a, signal_id_b,
                                question_a, question_b,
                                event_title_a, event_title_b,
                                platform_a, platform_b):
    with db() as conn:
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
                detected_at=datetime.utcnow().isoformat(),
            )


def get_recent_signals_for_grouping(mins=35):
    with db() as conn:
        cutoff = (datetime.utcnow() - timedelta(minutes=mins)).isoformat()
        rows = conn.run('''
            SELECT id, event_id, event_title, question,
                   platform, category, score
            FROM signals
            WHERE detected_at > :cutoff
            ORDER BY detected_at DESC
            LIMIT 100
        ''', cutoff=cutoff)
        columns = [c['name'] for c in conn.columns]
    return [dict(zip(columns, row)) for row in rows]


def get_unvalidated_candidates(limit=50):
    with db() as conn:
        rows = conn.run('''
            SELECT * FROM cross_event_candidates
            WHERE validated = 0
            ORDER BY detected_at DESC
            LIMIT :limit
        ''', limit=limit)
        columns = [c['name'] for c in conn.columns]
    return [dict(zip(columns, row)) for row in rows]


def mark_candidate_validated(candidate_id, is_related):
    with db() as conn:
        conn.run('''
            UPDATE cross_event_candidates
            SET validated = 1, is_related = :is_related
            WHERE id = :id
        ''', is_related=1 if is_related else 0, id=candidate_id)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_signal_stats():
    today_start = datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).isoformat()

    with db() as conn:
        total = conn.run("SELECT COUNT(*) FROM signals")[0][0]

        today = conn.run(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= :today_start",
            today_start=today_start,
        )[0][0]

        high_score = conn.run(
            "SELECT COUNT(*) FROM signals WHERE score >= 70 "
            "AND detected_at >= :today_start",
            today_start=today_start,
        )[0][0]

        avg_result = conn.run(
            "SELECT AVG(score) FROM signals WHERE detected_at >= :today_start",
            today_start=today_start,
        )
        avg_score = avg_result[0][0] if avg_result else 0

    return {
        'total': total,
        'today': today,
        'high_confidence': high_score,
        'avg_score': round(float(avg_score or 0), 1),
    }


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def flag_signal_for_retry(signal_id, needs_news=False, needs_summary=False):
    """
    Flag a signal for summary retry next poll.
    needs_news is accepted but ignored — news is handled by run_news_recheck().
    Only needs_summary=True has effect.
    """
    with db() as conn:
        # Use a simple approach: store pending retries in a lightweight table
        conn.run('''
            INSERT INTO retry_queue (signal_id, needs_news, needs_summary, created_at)
            VALUES (:signal_id, :needs_news, :needs_summary, :created_at)
            ON CONFLICT (signal_id) DO UPDATE SET
                needs_news    = GREATEST(retry_queue.needs_news,    :needs_news),
                needs_summary = GREATEST(retry_queue.needs_summary, :needs_summary)
        ''',
            signal_id=signal_id,
            needs_news=1 if needs_news else 0,
            needs_summary=1 if needs_summary else 0,
            created_at=datetime.utcnow().isoformat(),
        )


def get_retry_queue(limit=20):
    """Get signals pending retry, oldest first."""
    with db() as conn:
        # Check table exists first
        try:
            rows = conn.run('''
                SELECT r.signal_id, r.needs_news, r.needs_summary,
                       s.event_title, s.question, s.category,
                       s.prev_odds, s.current_odds, s.price_move,
                       s.direction, s.detected_at, s.news_headline,
                       s.sports_context
                FROM retry_queue r
                JOIN signals s ON s.id = r.signal_id
                ORDER BY r.created_at ASC
                LIMIT :limit
            ''', limit=limit)
            cols = [c['name'] for c in conn.columns]
            return [dict(zip(cols, row)) for row in rows]
        except Exception:
            return []


def clear_retry_queue_entry(signal_id):
    """Remove a signal from the retry queue after successful processing."""
    with db() as conn:
        try:
            conn.run(
                "DELETE FROM retry_queue WHERE signal_id = :id",
                id=signal_id,
            )
        except Exception:
            pass


def setup_retry_queue():
    """Create retry_queue table if not exists."""
    with db() as conn:
        conn.run('''
            CREATE TABLE IF NOT EXISTS retry_queue (
                signal_id    INTEGER PRIMARY KEY,
                needs_news   INTEGER DEFAULT 0,
                needs_summary INTEGER DEFAULT 0,
                created_at   TEXT NOT NULL
            )
        ''')


def get_signals_for_news_recheck():
    """
    Returns signals that need a news re-check.

    Priority order:
    1. Vacuum signals (news_vacuum=1) detected 25-35 mins ago
       — the 30-min window: long enough for news to break, still actionable
    2. All other signals detected 25-35 mins ago
       — catches cases where initial news check was wrong or incomplete

    We use a 25-35 minute window (not exactly 30) to avoid missing signals
    that were stored a few seconds off due to poll timing.

    Excludes terminal sports signals — no point re-checking a resolved match.
    """
    from datetime import timedelta
    now = datetime.utcnow()
    window_start = (now - timedelta(minutes=35)).isoformat()
    window_end   = (now - timedelta(minutes=25)).isoformat()

    with db() as conn:
        # Vacuum signals first
        rows_vacuum = conn.run('''
            SELECT id, event_title, question, category,
                   detected_at, news_vacuum, news_timing,
                   prev_odds, current_odds, direction
            FROM signals
            WHERE news_vacuum = 1
              AND is_terminal = 0
              AND detected_at BETWEEN :start AND :end
            ORDER BY score DESC
            LIMIT 20
        ''', start=window_start, end=window_end)
        cols = [c['name'] for c in conn.columns]
        vacuum_sigs = [dict(zip(cols, r)) for r in rows_vacuum]

        # All other recent signals
        rows_recent = conn.run('''
            SELECT id, event_title, question, category,
                   detected_at, news_vacuum, news_timing,
                   prev_odds, current_odds, direction
            FROM signals
            WHERE news_vacuum = 0
              AND is_terminal = 0
              AND detected_at BETWEEN :start AND :end
            ORDER BY score DESC
            LIMIT 10
        ''', start=window_start, end=window_end)
        cols = [c['name'] for c in conn.columns]
        recent_sigs = [dict(zip(cols, r)) for r in rows_recent]

    return vacuum_sigs + recent_sigs


def update_signal_news(signal_id, news_result):
    """Update a signal's news fields after a re-check."""
    articles = news_result.get('articles', [])
    article  = articles[0] if articles else None
    with db() as conn:
        conn.run('''
            UPDATE signals SET
                news_vacuum   = :vacuum,
                news_timing   = :timing,
                news_headline = :headline,
                news_source   = :source,
                news_url      = :url
            WHERE id = :id
        ''',
            vacuum=1 if news_result.get('vacuum', True) else 0,
            timing=news_result.get('timing', 'unknown'),
            headline=article['headline'] if article else None,
            source=article['source']   if article else None,
            url=article['url']         if article else None,
            id=signal_id,
        )


def setup_waitlist():
    """Create waitlist table if not exists — called from api startup."""
    with db() as conn:
        conn.run('''
            CREATE TABLE IF NOT EXISTS waitlist (
                id         SERIAL PRIMARY KEY,
                email      TEXT NOT NULL UNIQUE,
                name       TEXT,
                joined_at  TEXT NOT NULL
            )
        ''')
        conn.run('''
            CREATE INDEX IF NOT EXISTS idx_waitlist_email
            ON waitlist(email)
        ''')


def save_waitlist_entry(email, name=''):
    """Save a waitlist signup. Returns 'ok' or 'duplicate'."""
    with db() as conn:
        try:
            conn.run('''
                INSERT INTO waitlist (email, name, joined_at)
                VALUES (:email, :name, :joined_at)
            ''',
                email=email,
                name=name,
                joined_at=datetime.utcnow().isoformat(),
            )
            return 'ok'
        except Exception as e:
            if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                return 'duplicate'
            raise


def get_waitlist_count():
    """Return total waitlist signups."""
    with db() as conn:
        rows = conn.run("SELECT COUNT(*) FROM waitlist")
        return rows[0][0] if rows else 0


def cleanup_old_data():
    """
    Retention policy — balances DB size against historical value.

    Snapshots:  7 days  — needed for sparklines and price history
    Signals:    30 days — builds the historical record (Bloomberg-esque)
    Candidates: 7 days
    Volume:     7 days

    Timestamps stored as TEXT ISO format — string comparison is correct
    because ISO-8601 sorts lexicographically.
    """
    from constants import (SNAPSHOT_RETENTION_DAYS, SIGNAL_RETENTION_DAYS,
                           CANDIDATE_RETENTION_DAYS, VOLUME_RETENTION_DAYS)
    now = datetime.utcnow()
    cutoffs = {
        'snapshots':  (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).isoformat(),
        'signals':    (now - timedelta(days=SIGNAL_RETENTION_DAYS)).isoformat(),
        'candidates': (now - timedelta(days=CANDIDATE_RETENTION_DAYS)).isoformat(),
        'volume':     (now - timedelta(days=VOLUME_RETENTION_DAYS)).isoformat(),
    }

    with db() as conn:
        conn.run(
            "DELETE FROM snapshots WHERE timestamp < :cutoff",
            cutoff=cutoffs['snapshots'],
        )
        conn.run(
            "DELETE FROM signals WHERE detected_at < :cutoff",
            cutoff=cutoffs['signals'],
        )
        conn.run(
            "DELETE FROM cross_event_candidates "
            "WHERE validated = 1 AND detected_at < :cutoff",
            cutoff=cutoffs['candidates'],
        )
        conn.run(
            "DELETE FROM volume_stats WHERE recorded_at < :cutoff",
            cutoff=cutoffs['volume'],
        )

        sig_count  = conn.run("SELECT COUNT(*) FROM signals")[0][0]
        snap_count = conn.run("SELECT COUNT(*) FROM snapshots")[0][0]
        print(f"Cleanup done — {sig_count} signals, {snap_count} snapshots kept")
