import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

CATEGORY_FEEDS = {
    'political': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
    ],
    'macro': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://finance.yahoo.com/rss/topfinstories', 'Yahoo Finance'),
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
    ],
    'geopolitical': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
    ],
    'commodities': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://finance.yahoo.com/rss/topfinstories', 'Yahoo Finance'),
    ],
    'crypto': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
    ],
    'sports': [
        ('https://sports.yahoo.com/rss/', 'Yahoo Sports'),
        ('https://www.espn.com/espn/rss/news', 'ESPN'),
        ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
        ('https://www.espn.com/espn/rss/nba/news', 'ESPN NBA'),
        ('https://www.espn.com/espn/rss/nfl/news', 'ESPN NFL'),
        ('https://www.espn.com/espn/rss/golf/news', 'ESPN Golf'),
    ],
    'esports': [
        ('https://sports.yahoo.com/rss/', 'Yahoo Sports'),
        ('https://www.espn.com/espn/rss/news', 'ESPN'),
    ],
    'other': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
    ]
}

SPORT_SPECIFIC_FEEDS = {
    'soccer': ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
    'football': ('https://www.espn.com/espn/rss/nfl/news', 'ESPN NFL'),
    'nfl': ('https://www.espn.com/espn/rss/nfl/news', 'ESPN NFL'),
    'nba': ('https://www.espn.com/espn/rss/nba/news', 'ESPN NBA'),
    'basketball': ('https://www.espn.com/espn/rss/nba/news', 'ESPN NBA'),
    'mlb': ('https://www.espn.com/espn/rss/mlb/news', 'ESPN MLB'),
    'baseball': ('https://www.espn.com/espn/rss/mlb/news', 'ESPN MLB'),
    'nhl': ('https://www.espn.com/espn/rss/nhl/news', 'ESPN NHL'),
    'hockey': ('https://www.espn.com/espn/rss/nhl/news', 'ESPN NHL'),
    'golf': ('https://www.espn.com/espn/rss/golf/news', 'ESPN Golf'),
    'pga': ('https://www.espn.com/espn/rss/golf/news', 'ESPN Golf'),
    'masters': ('https://www.espn.com/espn/rss/golf/news', 'ESPN Golf'),
    'tennis': ('https://www.espn.com/espn/rss/tennis/news', 'ESPN Tennis'),
    'premier league': ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
    'champions league': ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
    'haaland': ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
    'man city': ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
}

CATEGORY_KEYWORDS = {
    'political': [
        'trump', 'biden', 'president', 'congress',
        'senate', 'election', 'white house', 'executive order',
        'democrat', 'republican', 'policy', 'legislation'
    ],
    'macro': [
        'federal reserve', 'fed', 'fomc', 'rate cut',
        'interest rate', 'inflation', 'cpi', 'gdp',
        'jobs report', 'unemployment', 'payroll', 'recession'
    ],
    'geopolitical': [
        'ukraine', 'russia', 'china', 'iran', 'israel',
        'ceasefire', 'war', 'sanctions', 'nato', 'military',
        'tariff', 'trade war', 'embargo', 'strait of hormuz'
    ],
    'commodities': [
        'oil', 'crude', 'wti', 'brent', 'opec', 'gold',
        'silver', 'natural gas', 'petroleum', 'barrel'
    ],
    'crypto': [
        'bitcoin', 'btc', 'ethereum', 'eth', 'crypto',
        'blockchain', 'defi', 'stablecoin', 'sec crypto'
    ],
    'sports': [
        'nfl', 'nba', 'mlb', 'nhl', 'premier league',
        'champions league', 'world cup', 'masters', 'pga',
        'injury', 'trade', 'transfer', 'roster', 'haaland',
        'scheffler', 'mbappe', 'lebron', 'mahomes'
    ],
    'esports': [
        'esports', 'g2', 'navi', 'faze', 'team liquid',
        'counter strike', 'valorant', 'league of legends',
        'dota', 'iem', 'major tournament'
    ]
}

RELATED_KEYWORDS = {
    'iran': ['iran', 'hormuz', 'tehran', 'middle east',
             'persian gulf', 'irgc', 'nuclear'],
    'tariffs': ['tariff', 'china', 'trade war', 'chinese imports',
                'trade restrictions', 'duties', 'customs'],
    'fed': ['federal reserve', 'fed', 'fomc', 'rate cut',
            'interest rate', 'powell', 'monetary policy'],
    'ukraine': ['ukraine', 'russia', 'ceasefire', 'zelensky',
                'peace deal', 'kyiv', 'moscow', 'nato'],
    'masters': ['masters', 'augusta', 'golf', 'pga tour'],
    'oil': ['oil', 'crude', 'wti', 'brent', 'opec', 'petroleum'],
    'gold': ['gold', 'xau', 'precious metals', 'bullion'],
    'bitcoin': ['bitcoin', 'btc', 'crypto', 'cryptocurrency'],
    'election': ['election', 'vote', 'ballot', 'polling',
                 'candidate', 'campaign'],
    'ceasefire': ['ceasefire', 'peace deal', 'truce',
                  'negotiations', 'armistice']
}

def parse_article_date(pub_date_str):
    """Parse RSS pubDate string into a naive UTC datetime."""
    if not pub_date_str:
        return None
    try:
        dt = parsedate_to_datetime(pub_date_str)
        # Convert to UTC naive datetime for comparison
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def classify_article_timing(article_pub_date, signal_detected_at_str):
    """
    Returns:
      'before'       — article existed before the signal (news explains the move)
      'after'        — article appeared after the signal (signal preceded news)
      'simultaneous' — within 30 minutes either way (unclear)
      'unknown'      — can't determine
    """
    if not article_pub_date:
        return 'unknown'
    try:
        signal_time = datetime.fromisoformat(signal_detected_at_str)
        diff_minutes = (signal_time - article_pub_date).total_seconds() / 60

        if diff_minutes > 30:
            return 'before'       # article published 30+ mins before signal
        elif diff_minutes < -30:
            return 'after'        # article published 30+ mins after signal
        else:
            return 'simultaneous'
    except Exception:
        return 'unknown'

def fetch_rss(url, source_name):
    try:
        response = requests.get(url, timeout=8, headers={
            'User-Agent': 'Mozilla/5.0 GaugeMarket/1.0'
        })
        response.raise_for_status()
        root = ET.fromstring(response.content)
        items = []
        for item in root.findall('.//item'):
            items.append({
                'title': item.findtext('title', ''),
                'link': item.findtext('link', ''),
                'description': item.findtext('description', ''),
                'pubDate': item.findtext('pubDate', ''),
                'source': source_name
            })
        return items
    except Exception as e:
        print(f"RSS fetch error ({url}): {e}")
        return []

def get_event_category(event_title, question):
    text = f"{event_title} {question}".lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return 'other'

def get_keyword_group(event_title, question):
    text = f"{event_title} {question}".lower()
    for group, keywords in RELATED_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return group
    return None

def get_sport_specific_feeds(event_title, question):
    text = f"{event_title} {question}".lower()
    extra_feeds = []
    for sport_kw, feed_tuple in SPORT_SPECIFIC_FEEDS.items():
        if sport_kw in text and feed_tuple not in extra_feeds:
            extra_feeds.append(feed_tuple)
    return extra_feeds

def extract_search_terms(event_title, question):
    text = f"{event_title} {question}".lower()
    terms = []

    for group, keywords in RELATED_KEYWORDS.items():
        matching = [kw for kw in keywords if kw in text]
        if matching:
            terms.extend(matching[:2])

    for sport_kw in SPORT_SPECIFIC_FEEDS.keys():
        if sport_kw in text and sport_kw not in terms:
            terms.append(sport_kw)

    # Extract proper nouns from original (capitalised words = player/team names)
    original = f"{event_title} {question}"
    skip_words = {'Will', 'The', 'For', 'And', 'But', 'That', 'This',
                  'With', 'From', 'When', 'What', 'Does', 'Have', 'Been',
                  'They', 'Before', 'After', 'Which', 'Where', 'Meet',
                  'April', 'January', 'February', 'March', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December'}
    proper_nouns = [
        w.strip('?"\'.,') for w in original.split()
        if len(w) > 3
        and w[0].isupper()
        and w not in skip_words
    ]
    terms.extend([p.lower() for p in proper_nouns[:3]])

    if not terms:
        words = text.split()
        terms = [w for w in words
                 if len(w) > 4
                 and w not in ['will', 'when', 'does', 'what', 'that',
                               'this', 'with', 'from', 'have', 'been',
                               'they', 'their', 'before', 'after',
                               'which', 'where']][:4]

    return list(set(terms))

def check_news_vacuum(event_title, question, category='other',
                      signal_detected_at=None):
    search_terms = extract_search_terms(event_title, question)

    if not search_terms:
        return {
            'vacuum': True,
            'articles': [],
            'timing': 'unknown',
            'checked_at': datetime.now().isoformat()
        }

    feeds_to_check = CATEGORY_FEEDS.get(category, CATEGORY_FEEDS['other'])

    if category == 'sports':
        extra = get_sport_specific_feeds(event_title, question)
        feeds_to_check = extra + [f for f in feeds_to_check if f not in extra]

    articles_found = []
    sources_checked = []
    detected_at = signal_detected_at or datetime.now().isoformat()

    for feed_url, source_name in feeds_to_check:
        if source_name in sources_checked:
            continue
        sources_checked.append(source_name)

        entries = fetch_rss(feed_url, source_name)
        for entry in entries[:25]:
            title = entry.get('title', '').lower()
            desc = entry.get('description', '').lower()
            if any(term in title or term in desc for term in search_terms):
                pub_date_str = entry.get('pubDate', '')
                pub_date = parse_article_date(pub_date_str)
                timing = classify_article_timing(pub_date, detected_at)

                articles_found.append({
                    'headline': entry.get('title', ''),
                    'source': source_name,
                    'url': entry.get('link', ''),
                    'published': pub_date_str,
                    'timing': timing
                })

        if articles_found:
            break

    articles_found = articles_found[:3]

    # Determine overall timing of best article found
    overall_timing = 'unknown'
    if articles_found:
        timings = [a['timing'] for a in articles_found]
        if 'after' in timings:
            overall_timing = 'after'       # article broke after bet — real signal
        elif 'simultaneous' in timings:
            overall_timing = 'simultaneous'
        elif 'before' in timings:
            overall_timing = 'before'      # news existed before bet — explains move

    return {
        'vacuum': len(articles_found) == 0,
        'articles': articles_found,
        'timing': overall_timing,
        'search_terms': search_terms,
        'sources_checked': sources_checked,
        'checked_at': datetime.now().isoformat()
    }
