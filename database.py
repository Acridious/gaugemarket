import os
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL', '')

def get_connection():
    import psycopg2
    url = DATABASE_URL
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    conn = psycopg2.connect(url)
    return conn

def setup_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('''
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

    cur.execute('''
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
            category TEXT DEFAULT 'uncategorised'
        )
    ''')

    cur.execute('''
        CREATE INDEX IF NOT EXISTS idx_snapshots_market_id 
        ON snapshots(market_id)
    ''')

    cur.execute('''
        CREATE INDEX IF NOT EXISTS idx_signals_detected 
        ON signals(detected_at)
    ''')

    conn.commit()
    cur.close()
    conn.close()
    print("Database ready")

def save_snapshot(market_id, event_id, event_title,
                  question, odds, volume, platform):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO snapshots 
        (market_id, event_id, event_title, question, 
         odds, volume, platform, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ''', (market_id, event_id, event_title, question,
          odds, volume, platform, datetime.now().isoformat()))
    conn.commit()
    cur.close()
    conn.close()

def get_last_snapshot(market_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT odds, volume, timestamp 
        FROM snapshots 
        WHERE market_id = %s
        ORDER BY timestamp DESC 
        LIMIT 1
    ''', (market_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {'odds': row[0], 'volume': row[1], 'timestamp': row[2]}
    return None

def save_signal(signal_data):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO signals (
            event_id, event_title, question, platform,
            prev_odds, current_odds, price_move, direction,
            volume, score, related_same_event, related_cross_event,
            news_vacuum, news_headline, news_source, news_url,
            detected_at, category
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        signal_data['event_id'],
        signal_data['event_title'],
        signal_data['question'],
        signal_data['platform'],
        signal_data['prev_odds'],
        signal_data['current_odds'],
        signal_data['price_move'],
        signal_data['direction'],
        signal_data['volume'],
        signal_data['score'],
        signal_data.get('related_same_event', 0),
        signal_data.get('related_cross_event', 0),
        1 if signal_data.get('news_vacuum', True) else 0,
        signal_data.get('news_headline'),
        signal_data.get('news_source'),
        signal_data.get('news_url'),
        datetime.now().isoformat(),
        signal_data.get('category', 'uncategorised')
    ))
    conn.commit()
    cur.close()
    conn.close()

def get_recent_signals(limit=20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT * FROM signals
        ORDER BY detected_at DESC
        LIMIT %s
    ''', (limit,))
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_signals_filtered(min_score=50, category=None,
                         platform=None, limit=20):
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT * FROM signals WHERE score >= %s"
    params = [min_score]
    if category and category != 'all':
        query += " AND category = %s"
        params.append(category)
    if platform:
        query += " AND platform = %s"
        params.append(platform)
    query += " ORDER BY detected_at DESC LIMIT %s"
    params.append(limit)
    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_signal_by_id(signal_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM signals WHERE id = %s", (signal_id,))
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return dict(zip(columns, row)) if row else None

def get_signal_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM signals")
    total = cur.fetchone()[0]
    cur.execute('''
        SELECT COUNT(*) FROM signals 
        WHERE detected_at::date = CURRENT_DATE
    ''')
    today = cur.fetchone()[0]
    cur.execute('''
        SELECT COUNT(*) FROM signals 
        WHERE score >= 70 AND detected_at::date = CURRENT_DATE
    ''')
    high_score = cur.fetchone()[0]
    cur.execute('''
        SELECT AVG(score) FROM signals
        WHERE detected_at::date = CURRENT_DATE
    ''')
    avg_score = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {
        'total': total,
        'today': today,
        'high_confidence': high_score,
        'avg_score': round(float(avg_score or 0), 1)
    }
