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
        # Lightweight ping — if the connection is dead this raises
        if _conn is not None:
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
    On any exception the shared connection is closed so the next call
    forces a reconnect rather than retrying on a broken socket.
    """
    conn = get_connection()
    try:
        yield conn
    except Exception:
        global _conn
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
        raise


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

    print("Database ready")


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def save_snapshot(market_id, event_id, event_title,
                  question, odds, volume, platform):
    with db() as conn:
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
            timestamp=datetime.utcnow().isoformat(),
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
                market_url, is_terminal, mins_elapsed
            ) VALUES (
                :event_id, :event_title, :question, :platform,
                :prev_odds, :current_odds, :price_move, :direction,
                :volume, :score, :related_same_event, :related_cross_event,
                :news_vacuum, :news_headline, :news_source, :news_url,
                :detected_at, :category, :related_contracts, :news_timing,
                :market_url, :is_terminal, :mins_elapsed
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
            is_terminal=1 if signal_data.get('is_terminal', False) else 0,
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

def cleanup_old_data():
    """
    Lean storage — keep DB tiny while there are no paying customers.

    Signals:    keep last 48 hours
    Snapshots:  keep last 2 hours (all we need for movement detection)
    Candidates: keep last 48 hours
    Volume:     keep last 24 hours

    Timestamps stored as TEXT ISO format — string comparison is correct
    because ISO-8601 sorts lexicographically.
    """
    now = datetime.utcnow()
    cutoffs = {
        'snapshots':   (now - timedelta(hours=2)).isoformat(),
        'signals':     (now - timedelta(hours=48)).isoformat(),
        'candidates':  (now - timedelta(hours=48)).isoformat(),
        'volume':      (now - timedelta(hours=24)).isoformat(),
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
