import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import os as _os

BRAVE_API_KEY  = _os.environ.get('BRAVE_API_KEY', '')
BRAVE_NEWS_URL = 'https://api.search.brave.com/res/v1/news/search'

# ---------------------------------------------------------------------------
# Brave monthly call budget
# ---------------------------------------------------------------------------
# Free tier: 2,000 calls/month. At ~3 calls per intel signal and
# ~50 intel signals/day that's 150/day = 4,500/month — over limit.
# Cap at 50 Brave calls/day (1,500/month) to stay safely under 2,000.
# Sports/esports in-game signals never call Brave so real usage is lower.

BRAVE_DAILY_CAP  = 50   # calls per day — adjust based on your plan
_brave_calls_today = 0
_brave_reset_date  = None

def _brave_budget_remaining():
    global _brave_calls_today, _brave_reset_date
    from datetime import date
    today = date.today()
    if _brave_reset_date != today:
        _brave_reset_date   = today
        _brave_calls_today  = 0
    return _brave_calls_today < BRAVE_DAILY_CAP

def _brave_consume():
    global _brave_calls_today
    _brave_calls_today += 1

if BRAVE_API_KEY:
    print(f"Brave Search API: configured (key length {len(BRAVE_API_KEY)}, daily cap: {BRAVE_DAILY_CAP})")
else:
    print("Brave Search API: NOT configured — set BRAVE_API_KEY env var")

# ---------------------------------------------------------------------------
# NewsAPI.org configuration
# ---------------------------------------------------------------------------
# Free tier: 100 requests/day, searches 150,000+ sources.
# Used as Pass 1b — runs after Brave (or instead of if Brave quota exhausted).
# Docs: https://newsapi.org/docs/endpoints/everything

NEWSAPI_KEY     = _os.environ.get('NEWSAPI_KEY', '')
NEWSAPI_URL     = 'https://newsapi.org/v2/everything'
NEWSAPI_DAILY_CAP = 80  # stay under 100/day free limit

_newsapi_calls_today = 0
_newsapi_reset_date  = None

def _newsapi_budget_remaining():
    global _newsapi_calls_today, _newsapi_reset_date
    from datetime import date
    today = date.today()
    if _newsapi_reset_date != today:
        _newsapi_reset_date  = today
        _newsapi_calls_today = 0
    return _newsapi_calls_today < NEWSAPI_DAILY_CAP

def _newsapi_consume():
    global _newsapi_calls_today
    _newsapi_calls_today += 1

if NEWSAPI_KEY:
    print(f"NewsAPI: configured (key length {len(NEWSAPI_KEY)}, daily cap: {NEWSAPI_DAILY_CAP})")
else:
    print("NewsAPI: NOT configured — set NEWSAPI_KEY env var")


def _newsapi_search(query, category='other', max_results=5):
    """
    Search NewsAPI.org for recent articles matching query.
    Returns list of article dicts compatible with our pipeline.
    """
    if not NEWSAPI_KEY:
        return []
    if not _newsapi_budget_remaining():
        print(f"  NewsAPI daily cap ({NEWSAPI_DAILY_CAP}) reached")
        return []

    # Map our categories to NewsAPI source domains for better relevance
    source_domains = {
        'geopolitical': 'reuters.com,bbc.co.uk,aljazeera.com,apnews.com,theguardian.com',
        'macro':        'reuters.com,bloomberg.com,cnbc.com,ft.com,wsj.com,marketwatch.com',
        'political':    'reuters.com,apnews.com,politico.com,axios.com,bbc.co.uk',
        'crypto':       'coindesk.com,cointelegraph.com,decrypt.co,theblock.co',
        'commodities':  'reuters.com,bloomberg.com,oilprice.com,ft.com',
        'sports':       'espn.com,bbc.co.uk,skysports.com,goal.com,theathletic.com',
        'esports':      'dexerto.com,dotesports.com,esportsinsider.com',
    }.get(category, 'reuters.com,bbc.co.uk,apnews.com,theguardian.com')

    from datetime import datetime, timedelta
    from_date = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')

    _newsapi_consume()
    try:
        resp = _requests.get(
            NEWSAPI_URL,
            params={
                'q':          query,
                'domains':    source_domains,
                'from':       from_date,
                'sortBy':     'relevancy',
                'pageSize':   max_results,
                'language':   'en',
            },
            headers={'X-Api-Key': NEWSAPI_KEY},
            timeout=8,
        )
        if resp.status_code == 426:
            print("  NewsAPI: upgrade required for this feature")
            return []
        if resp.status_code == 429:
            print("  NewsAPI: rate limited")
            return []
        if resp.status_code == 401:
            print("  NewsAPI: invalid API key")
            return []
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for a in data.get('articles', []):
            if not a.get('title') or a['title'] == '[Removed]':
                continue
            articles.append({
                'title':       a.get('title', ''),
                'description': a.get('description', '') or a.get('content', '') or '',
                'link':        a.get('url', ''),
                'source':      a.get('source', {}).get('name', 'NewsAPI'),
                'pubDate':     a.get('publishedAt', ''),
            })
        return articles

    except Exception as e:
        print(f"  NewsAPI error: {e}")
        return []

# ---------------------------------------------------------------------------
# Brave Search News API
# ---------------------------------------------------------------------------

def _brave_search_news(query, freshness='pw', count=5):
    """
    Search for news articles using Brave Search API.

    freshness: 'pd' = past day, 'pw' = past week, 'pm' = past month
    Returns list of article dicts compatible with RSS article format.
    Falls back to empty list if API key not set or request fails.
    """
    if not BRAVE_API_KEY:
        return []
    try:
        response = requests.get(
            BRAVE_NEWS_URL,
            headers={
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip',
                'X-Subscription-Token': BRAVE_API_KEY,
            },
            params={
                'q':         query,
                'count':     count,
                'freshness': freshness,
                'text_decorations': False,
            },
            timeout=8,
        )
        response.raise_for_status()
        data     = response.json()
        results  = data.get('results', [])
        articles = []
        for r in results:
            articles.append({
                'title':       r.get('title', ''),
                'link':        r.get('url', ''),
                'description': r.get('description', ''),
                'pubDate':     r.get('age', ''),   # Brave returns relative age
                'source':      r.get('source', {}).get('name', 'Brave News'),
                '_brave':      True,
            })
        return articles
    except Exception as e:
        print(f"  Brave news error: {e}")
        return []


def _brave_freshness_for_age(max_age_days):
    """Map max age days to Brave freshness parameter."""
    if max_age_days <= 1:  return 'pd'
    if max_age_days <= 7:  return 'pw'
    return 'pm'


# Free RSS feeds by category
# Priority order matters — first match stops further searching
CATEGORY_FEEDS = {
    'political': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://www.cnbc.com/id/10000113/device/rss/rss.html', 'CNBC Politics'),
        ('https://feeds.bbci.co.uk/news/politics/rss.xml', 'BBC Politics'),
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
    ],
    'macro': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://www.cnbc.com/id/20910258/device/rss/rss.html', 'CNBC Economy'),
        ('https://www.cnbc.com/id/10000664/device/rss/rss.html', 'CNBC Finance'),
        ('https://feeds.marketwatch.com/marketwatch/topstories/', 'MarketWatch'),
        ('https://finance.yahoo.com/rss/topfinstories', 'Yahoo Finance Top'),
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
    ],
    'geopolitical': [
        ('https://feeds.bbci.co.uk/news/world/rss.xml', 'BBC World'),
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://www.cnbc.com/id/100727362/device/rss/rss.html', 'CNBC World'),
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
    ],
    'commodities': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://www.cnbc.com/id/10000664/device/rss/rss.html', 'CNBC Finance'),
        ('https://feeds.marketwatch.com/marketwatch/topstories/', 'MarketWatch'),
        ('https://finance.yahoo.com/rss/topfinstories', 'Yahoo Finance Top'),
    ],
    'crypto': [
        ('https://finance.yahoo.com/rss/', 'Yahoo Finance'),
        ('https://www.cnbc.com/id/19854910/device/rss/rss.html', 'CNBC Tech'),
        ('https://feeds.marketwatch.com/marketwatch/topstories/', 'MarketWatch'),
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
    ],
    'sports': [
        ('https://sports.yahoo.com/rss/', 'Yahoo Sports'),
        ('https://www.espn.com/espn/rss/news', 'ESPN'),
        ('https://www.espn.com/espn/rss/soccer/news', 'ESPN Soccer'),
        ('https://www.espn.com/espn/rss/nba/news', 'ESPN NBA'),
        ('https://www.espn.com/espn/rss/nfl/news', 'ESPN NFL'),
        ('https://www.espn.com/espn/rss/golf/news', 'ESPN Golf'),
        ('https://feeds.bbci.co.uk/sport/rss.xml', 'BBC Sport'),
    ],
    'esports': [
        ('https://www.dexerto.com/feed/', 'Dexerto'),
        ('https://dotesports.com/feed', 'Dot Esports'),
        ('https://esportsinsider.com/feed', 'Esports Insider'),
        ('https://www.gosugamers.net/rss', 'GosuGamers'),
    ],
    'other': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://feeds.bbci.co.uk/news/rss.xml', 'BBC News'),
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

# Sports-specific signals in contract text — used as a fast pre-check before Groq.
# The goal is to catch ANY contract that is about a sporting event, match, or
# player performance, regardless of whether we recognise the team or player name.
# Patterns are chosen because they ONLY appear in sports/esports markets.
_SPORTS_SIGNALS = [
    # Bet structure keywords — universal across all sports markets
    'o/u', 'over/under', 'over under',
    'moneyline', 'money line',
    'handicap', 'spread',
    'first half', 'second half', 'full time', 'half time', 'halftime',
    'match winner', 'game winner', 'series winner', 'set winner',
    'win the match', 'win the game', 'win the series', 'win the set',
    'to win', ' vs ', ' v ',                 # "Team A vs Team B"

    # Stat prop keywords — player performance bets
    'points o/u', 'assists o/u', 'rebounds o/u', 'goals o/u',
    'strikeouts o/u', 'yards o/u', 'kills o/u',
    'total points', 'total goals', 'total runs', 'total assists',

    # Explicit score/result language
    'correct score', 'both teams to score', 'btts',
    'clean sheet', 'first scorer', 'last scorer', 'anytime scorer',
    'double chance', 'draw no bet',
    'set 1', 'set 2', 'set 3', 'set 4', 'set 5',   # tennis sets
    'map 1', 'map 2', 'map 3', 'map 4', 'map 5',   # esports maps
    'map handicap', 'map winner',
    'quarter 1', 'quarter 2', 'quarter 3', 'quarter 4',
    'innings', 'wicket', 'century',                 # cricket
    'round winner', 'fight winner', 'ko win', 'tko',# combat sports
    'lap ', 'qualifying', 'pole position',           # motorsport

    # League / competition names
    'nba', 'nfl', 'mlb', 'nhl', 'nba 2k',
    'epl', 'mls', 'ufc', 'pfl', 'bellator',
    'premier league', 'la liga', 'serie a', 'bundesliga', 'ligue 1',
    'eredivisie', 'süper lig', 'super lig',          # Turkish football
    'chinese super league', 'j-league', 'k-league',  # Asian football
    'a-league', 'mls cup',
    'champions league', 'europa league', 'conference league',
    'world cup', 'euros', 'copa america', 'copa del rey', 'fa cup',
    'carabao cup', 'dfb-pokal',
    'pga', 'lpga', 'masters', 'the open', 'us open',
    'wimbledon', 'roland garros', 'australian open',  # tennis slams
    'atp', 'wta', 'itf',
    'tour de france', "giro d'italia", 'vuelta',     # cycling
    'formula 1', 'formula one', 'f1 ', 'motogp', 'nascar',
    'wnba', 'ncaa', 'ipl', 'big bash',               # more leagues

    'odd/even', 'odd even',  # prop bet format used in both sports and esports
    # Match format variants — "Team A vs Team B", "Team A vs. Team B", "Team A v Team B"
    ' vs ', ' vs. ', ' vs.\n', '(vs.', '(vs ',
    ' v ', ' v. ',
    # Common sports result format
    ' to beat ', ' beats ', ' beat ',
    ' defeats ', ' def ',

    # Player stats that only appear in sports props
    'rebounds', 'assists', 'strikeouts', 'touchdowns',
    'rushing yards', 'passing yards', 'receiving yards',
    'home runs', 'batting average',
]

def _fast_sports_check(event_title, question):
    """
    Returns True if the contract is obviously a sports market.
    Runs before Groq to catch markets that keyword-match sport patterns
    regardless of whether we recognise the team/player name.
    E.g. "Fatih Karagümrük SK vs. Eyüpspor: O/U 2.5" catches on 'o/u' and 'vs '.
    """
    text = f"{event_title} {question}".lower()
    return any(sig in text for sig in _SPORTS_SIGNALS)

def _fast_esports_check(event_title, question):
    """
    Esports check runs first — before the general sports check —
    so "Map Handicap: EDG vs JD Gaming" gets esports, not sports.
    """
    text = f"{event_title} {question}".lower()
    esports_signals = [
        # Game titles
        'counter-strike', 'cs2', 'csgo', 'cs:go',
        'valorant', 'league of legends', 'lol ',
        'dota 2', 'dota2',
        'overwatch', 'rocket league',
        'starcraft', 'warcraft',
        'call of duty', 'cod ', 'apex legends',
        'rainbow six', 'r6 ',
        'mobile legends', 'wild rift', 'arena of valor',
        # Teams / orgs
        'g2 esports', ' g2 ',
        'navi ', 'natus vincere',
        'faze clan', 'faze ',
        'team liquid', 'team vitality', 'team secret',
        'fnatic', 'astralis', 'cloud9', 'c9 ',
        'virtus.pro', 'virtus pro',
        'evil geniuses', ' eg ',
        'edg ', 'jd gaming', 'jdg',        # LPL teams (EDG triggered in screenshot)
        'gen.g', 'gen g', 't1 ', 'faker',
        'sentinels', 'nrg ', '100 thieves',
        # Tournament names
        'iem ', 'esl one', 'blast premier', 'major tournament',
        'worlds ', 'msi ', 'lck ', 'lcs ', 'lec ', 'lpl ',
        # Esports-specific bet types
        'map winner', 'map handicap',
        'round winner', 'kill race', 'first blood',
        'first tower', 'first dragon', 'first baron',
        # Esports prop patterns
        'total kills', 'odd/even', 'odd even',
        'first kill', 'most kills', 'kill diff',
        'map 1:', 'map 2:', 'map 3:',   # "Map 1: ..." format
        'pistol round', 'eco round',
    ]
    return any(sig in text for sig in esports_signals)


def get_event_category(event_title, question):
    """
    Classify the signal category.

    Order of operations:
    1. Fast deterministic pre-checks for sports/esports — these are
       unambiguous and a small LLM reliably gets them wrong on short
       player prop questions (e.g. "Anthony Edwards: Points O/U 27.5"
       has no obvious sport keyword but is clearly an NBA prop).
    2. Groq classification for everything else (macro, geo, political,
       crypto, commodities) where context matters.
    3. Keyword fallback if Groq is unavailable or returns an invalid value.
    """
    # Step 1: deterministic sports/esports check — beats Groq for props
    if _fast_esports_check(event_title, question):
        return 'esports'
    if _fast_sports_check(event_title, question):
        return 'sports'

    # Step 2: Groq for non-obvious categories
    from groq_client import groq_available, GROQ_API_KEY, GROQ_URL, GROQ_MODEL
    import requests as _req

    if groq_available():
        try:
            prompt = (
                "Classify this prediction market contract into exactly one category.\n\n"
                f'Event: "{event_title}"\n'
                f'Question: "{question}"\n\n'
                "Categories and what they cover:\n"
                "- political: elections, politicians, heads of state, government decisions, voting, legislation\n"
                "- macro: economy, interest rates, inflation, GDP, central banks, jobs data, recession\n"
                "- geopolitical: wars, invasions, international conflicts, sanctions, diplomacy, military action\n"
                "- commodities: oil, gold, gas, metals, agricultural products, OPEC\n"
                "- crypto: bitcoin, ethereum, any cryptocurrency, blockchain, DeFi, NFT\n"
                "- other: entertainment, awards, weather, science, anything else\n\n"
                "NOTE: If the contract involves any athlete, team, sport, match, "
                "tournament, player stats, or game result — answer 'sports'. "
                "This includes player prop bets even if the sport is not named.\n\n"
                "Reply with only the single category word, nothing else."
            )
            # Check category budget before calling
            from groq_client import budget_remaining
            if not budget_remaining('category'):
                print("  Groq category budget exhausted — using keyword fallback")
                raise Exception("budget_exhausted")
            response = _req.post(
                GROQ_URL,
                headers={
                    'Authorization': f'Bearer {GROQ_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'model': GROQ_MODEL,
                    'messages': [{'role': 'user', 'content': prompt}],
                    'max_tokens': 10,
                    'temperature': 0,
                },
                timeout=8,
            )
            # Manually consume category budget
            from groq_client import _consume
            _consume('category')
            response.raise_for_status()
            result = (
                response.json()['choices'][0]['message']['content']
                .strip().lower().split()[0]
            )
            valid = {'sports', 'esports', 'political', 'macro',
                     'geopolitical', 'commodities', 'crypto', 'other'}
            if result in valid:
                return result
        except Exception as e:
            print(f"Groq category error: {e} — falling back to keywords")

    # Step 3: keyword fallback
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

def is_article_relevant(article_headline, event_title, question, article_description=''):
    """
    Ask Groq if a news article is genuinely about this specific contract.
    Uses groq_client so the API key and model live in one place.
    Defaults to False on error — better to show no news than wrong news.
    """
    from groq_client import groq_yes_no
    prompt = (
        "Does this news article contain information that would directly "
        "affect the probability of this prediction market contract?\n\n"
        f"CONTRACT: {question}\n"
        f"EVENT: {event_title}\n\n"
        f"ARTICLE: {article_headline}\n"
        + (f"CONTEXT: {article_description[:300]}\n\n" if article_description else "\n\n")
        + "Answer YES if the article mentions the same person, team, event, "
        "or directly related development. Answer NO if the article is about "
        "something unrelated.\n"
        "Answer only YES or NO."
    )
    result = groq_yes_no(prompt, slot='news')
    if not result:
        print(f"  Groq: irrelevant — {article_headline[:50]}")
    return result

def _looks_like_ingame(event_title, question):
    """
    Returns True if this sports contract is almost certainly an in-game
    prop that won't have a news article.

    Pre-game contracts (team winner, match result, injury-related props)
    CAN have news — lineup leaks, injury reports, team news.
    In-game props (player rebounds O/U 0.5, total kills, first scorer)
    will never have a relevant news article.
    """
    text = f"{event_title} {question}".lower()

    # In-game prop patterns — these resolve during live play
    # and will never have a news article explaining them
    ingame_patterns = [
        # Low threshold O/U props — only meaningful live
        'o/u 0.', 'o/u 1.', 'o/u 2.',
        # Player/team stat props
        'rebounds o/u', 'assists o/u', 'points o/u',
        'kills o/u', 'total kills', 'over/under',
        # Esports in-game events
        'first blood', 'first kill', 'penta kill', 'pentakill',
        'destroy barracks', 'slay a dragon', 'slay dragon',
        'ends in daytime', 'ends in night', 'ends at night',
        'any player', 'both teams destroy', 'both teams slay',
        'both teams score', 'btts',
        'first tower', 'first dragon', 'first baron', 'first roshan',
        'map winner', 'round winner',
        'odd/even', 'correct score',
        # Sports in-game
        'first scorer', 'first goal', 'next goal', 'next point',
        'anytime scorer', 'last scorer',
        # Kill/score totals that only make sense live
        'total kills', 'total goals', 'total runs',
        # Time-based esports props
        'game duration', 'game time', 'game length',
    ]
    return any(p in text for p in ingame_patterns)


def _score_article(article, detected_at):
    """
    Score an article by credibility and timing for surface ranking.
    Higher = better. Used to pick the best article when multiple found.
    """
    from constants import news_source_weight
    weight  = news_source_weight(article.get('source', ''))
    timing  = article.get('timing', 'unknown')
    # After = capital moved first = most interesting
    t_score = {'after': 1.0, 'simultaneous': 0.6,
               'before': 0.3, 'unknown': 0.2}.get(timing, 0.2)
    return weight * t_score


def check_news_vacuum(event_title, question, category='other',
                      signal_detected_at=None):
    # For sports/esports, only check news for pre-game moves.
    # In-game moves (goals, red cards, injuries during play) will never
    # have a news article — skipping saves the Groq news budget for
    # signals that can actually have news attached.
    # sports_context is passed via signal_detected_at's sibling arg —
    # we detect it from the category + question pattern here instead.
    _is_ingame = (
        category in ('sports', 'esports')
        and signal_detected_at is not None
        and _looks_like_ingame(event_title, question)
    )
    if _is_ingame:
        return {
            'vacuum': True,
            'articles': [],
            'timing': 'unknown',
            'checked_at': datetime.utcnow().isoformat(),
            'background_article': None,
        }

    search_terms = extract_search_terms(event_title, question)

    if not search_terms:
        return {
            'vacuum': True,
            'articles': [],
            'timing': 'unknown',
            'checked_at': datetime.utcnow().isoformat()
        }

    detected_at = signal_detected_at or datetime.utcnow().isoformat()
    articles_found = []
    sources_checked = []

    # ── Pass 1: Brave Search API (primary)
    # Scope the query by category to avoid cross-domain false matches.
    # "Aurora" means a Dota 2 team in esports context, a crypto protocol
    # in crypto context, and a weather phenomenon in general.
    # Adding the category scope ("aurora esports dota2") ensures Brave
    # returns results from the right domain.
    if BRAVE_API_KEY and search_terms:
        category_scope = {
            'esports':     'esports',
            'sports':      'sports',
            'crypto':      'crypto',
            'macro':       'economy fed',
            'political':   'politics',
            'geopolitical': 'geopolitics',
            'commodities': 'commodities oil',
        }.get(category, '')
        query_terms = search_terms[:3]
        if category_scope:
            query_terms = [category_scope] + query_terms
        query = ' '.join(query_terms)
        entries = _brave_search_news(query, freshness='pd', count=5)
        for entry in entries:
            headline    = entry.get('title', '')
            description = entry.get('description', '')
            if not headline:
                continue
            # Pre-filter: combined title + description must contain
            # at least one search term — catches "atmosphere at Wembley"
            # where the team name is in description but not title
            combined_lower = (headline + ' ' + description).lower()
            if not any(t in combined_lower for t in search_terms):
                continue
            if not is_article_relevant(headline, event_title, question, description):
                continue
            pub_date = parse_article_date(entry.get('pubDate', ''))
            timing   = classify_article_timing(pub_date, detected_at)
            articles_found.append({
                'headline': headline,
                'source':   entry.get('source', 'Brave News'),
                'url':      entry.get('link', ''),
                'published': entry.get('pubDate', ''),
                'timing':   timing,
            })

    # ── Pass 2: RSS feeds (fallback if Brave found nothing or no API key)
    if not articles_found:
        feeds_to_check = CATEGORY_FEEDS.get(category, CATEGORY_FEEDS['other'])
        if category == 'sports':
            extra = get_sport_specific_feeds(event_title, question)
            feeds_to_check = extra + [f for f in feeds_to_check if f not in extra]

        for feed_url, source_name in feeds_to_check:
            if source_name in sources_checked:
                continue
            sources_checked.append(source_name)

            entries = fetch_rss(feed_url, source_name)
            for entry in entries[:25]:
                title    = entry.get('title', '').lower()
                desc     = entry.get('description', '').lower()
                combined = title + ' ' + desc

                primary   = search_terms[0] if search_terms else ''
                secondary = search_terms[1:] if len(search_terms) > 1 else []

                primary_matches   = primary and primary in combined
                secondary_matches = not secondary or any(t in combined for t in secondary)

                if not secondary:
                    if primary not in title:
                        continue
                elif not (primary_matches and secondary_matches):
                    continue

                headline    = entry.get('title', '')
                description = entry.get('description', '')

                if not is_article_relevant(headline, event_title, question, description):
                    print(f"  Groq rejected: {headline[:60]}")
                    continue

                pub_date_str = entry.get('pubDate', '')
                pub_date     = parse_article_date(pub_date_str)
                timing       = classify_article_timing(pub_date, detected_at)

                articles_found.append({
                    'headline': headline,
                    'source':   source_name,
                    'url':      entry.get('link', ''),
                    'published': pub_date_str,
                    'timing':   timing,
                })

            if articles_found:
                break

    # Determine overall timing of best article found
    overall_timing = 'unknown'
    if articles_found:
        timings = [a['timing'] for a in articles_found]
        if 'after' in timings:
            overall_timing = 'after'
        elif 'simultaneous' in timings:
            overall_timing = 'simultaneous'
        elif 'before' in timings:
            overall_timing = 'before'

    # Sort by credibility score — best article surfaces first
    articles_found.sort(key=lambda a: _score_article(a, detected_at), reverse=True)
    articles_found = articles_found[:3]

    # Background news fallback for non-sports categories.
    sports_cats = {'sports', 'esports'}
    background_article = None
    if not articles_found and category not in sports_cats:
        feeds_to_check = CATEGORY_FEEDS.get(category, CATEGORY_FEEDS['other'])
        background_article = _find_background_news(
            search_terms, feeds_to_check, max_age_days=4
        )

    return {
        'vacuum': len(articles_found) == 0,
        'articles': articles_found,
        'timing': overall_timing,
        'search_terms': search_terms,
        'sources_checked': sources_checked,
        'checked_at': datetime.now().isoformat(),
        'background_article': background_article,
    }


# ---------------------------------------------------------------------------
# AI summary generation
# ---------------------------------------------------------------------------




def generate_signal_summary(
    event_title, question, prev_odds, current_odds,
    price_move, direction, category,
    news_article=None, news_vacuum=True,
    sports_context=None, background_article=None,
    related_contracts=None,
):
    """Generate a 2-3 sentence explanation of why this contract is moving."""
    from groq_client import groq_complete, groq_available
    if not groq_available():
        return None

    prev_pct = round(prev_odds * 100)
    curr_pct = round(current_odds * 100)
    move_pct = round(price_move * 100)
    dir_word = 'up' if direction == 'YES' else 'down'

    cat_ctx = {
        'geopolitical': "Geopolitical contracts move slowly. Sharp unexplained moves often reflect diplomatic back-channels, early military intelligence, or leaked policy positions.",
        'macro':        "Macro contracts rarely move sharply without cause. Likely causes: central bank leaks, early access to economic data, or large institutional positioning.",
        'political':    "Political contracts without news often reflect internal polling, leaked decisions, or campaign intelligence before it reaches the press.",
        'crypto':       "Crypto markets move on on-chain data, large wallet movements, exchange intelligence, or macro risk flows.",
        'commodities':  "Commodity contracts move on supply-chain intelligence, OPEC signalling, or geopolitical developments affecting supply routes.",
        'sports':       "Pre-game sports moves without news may reflect lineup leaks, injury information in team circles, or sharp money from informed bettors.",
        'esports':      "Esports moves without news may reflect roster changes or scrim results shared within team communities before public announcement.",
    }.get(category, "Unexplained prediction market moves often reflect private information or early positioning.")

    ctx_parts = []
    if background_article:
        bg_hl = background_article.get('headline', '')
        if bg_hl:
            ctx_parts.append("BACKGROUND: " + bg_hl + " (" + background_article.get('source', '') + ")")
    if related_contracts:
        try:
            import json as _j
            rels = _j.loads(related_contracts) if isinstance(related_contracts, str) else related_contracts
            if rels:
                ctx_parts.append("RELATED MARKETS MOVING: " + '; '.join(
                    r['question'][:50] for r in rels[:3]
                ))
        except Exception:
            pass
    ctx_block = ('\n'.join(ctx_parts) + '\n\n') if ctx_parts else ''

    if news_article and not news_vacuum:
        headline    = news_article.get('headline', '')
        description = (news_article.get('description', '') or '')[:500]
        source      = news_article.get('source', '')
        timing      = news_article.get('timing', 'unknown')
        timing_map  = {
            'before':       'Article published BEFORE the move - market may be catching up.',
            'after':        'Article published AFTER the move - capital moved ahead of reporting.',
            'simultaneous': 'Article and move appeared at roughly the same time.',
        }
        timing_note = timing_map.get(timing, '')
        prompt = (
            "You are a prediction market analyst explaining a price movement.\n\n"
            "CONTRACT: " + question + "\n"
            "EVENT: " + event_title + "\nCATEGORY: " + category + "\n"
            "MOVE: " + str(prev_pct) + "% to " + str(curr_pct) + "% (" + dir_word + ", " + str(move_pct) + " points)\n\n"
            "NEWS:\nHeadline: " + headline + "\nSource: " + source + "\n"
            "Context: " + description + "\nTiming: " + timing_note + "\n\n"
            + ctx_block
            + "Write 2-3 sentences: explain what the news says, connect it to the move, "
            "note whether capital led or lagged the news. "
            "Only use facts from the article. Do not start with I."
        )
    else:
        sports_note = (
            ' This is a pre-game move - likely relates to lineup, injuries, or conditions.'
            if sports_context == 'pre_game_move' else ''
        )
        prompt = (
            "You are a prediction market analyst explaining an unusual move where no public news was found.\n\n"
            "CONTRACT: " + question + "\n"
            "EVENT: " + event_title + "\nCATEGORY: " + category + "\n"
            "MOVE: " + str(prev_pct) + "% to " + str(curr_pct) + "% (" + dir_word + ", " + str(move_pct) + " points)\n\n"
            "CATEGORY CONTEXT: " + cat_ctx + sports_note + "\n\n"
            + ctx_block
            + "Write 2-3 sentences. State the move and that no public news explains it. "
            "Then speculate on what this may reflect using the context provided. "
            "ALWAYS label speculation with: 'This may reflect', 'One possible explanation is', "
            "'Consistent with', or 'Could suggest'. Never state speculation as fact. Do not start with I."
        )

    summary = groq_complete(prompt, max_tokens=180, temperature=0.4)
    return summary if summary else None


def _find_background_news(search_terms, feeds, max_age_days=4):
    """
    Fallback: find a background context article up to max_age_days old.

    Requires BOTH primary AND at least one secondary search term to match.
    This prevents "Iran arsenal" articles appearing for "Iran diplomatic meeting"
    contracts — the primary term (iran) matches but the secondary terms
    (diplomatic, meeting, talks) do not.

    No Groq check to preserve budget, but the two-term requirement
    filters out the worst false matches.
    Returns a single article dict with timing='background', or None.
    """
    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)

    if not search_terms:
        return None

    primary   = search_terms[0]
    secondary = search_terms[1:] if len(search_terms) > 1 else []

    sources_tried = set()
    for feed_url, source_name in feeds:
        if source_name in sources_tried:
            continue
        sources_tried.add(source_name)

        entries = fetch_rss(feed_url, source_name)
        for entry in entries[:30]:
            title = entry.get('title', '').lower()
            desc  = entry.get('description', '').lower()
            combined = title + ' ' + desc

            # Require primary term in title (not just description)
            if primary not in title:
                continue

            # Require at least one secondary term anywhere
            # If no secondary terms available, skip — single-term match
            # is too loose for background context
            if not secondary:
                continue
            if not any(t in combined for t in secondary):
                continue

            pub_date = parse_article_date(entry.get('pubDate', ''))
            if pub_date and pub_date < cutoff:
                continue

            return {
                'headline': entry.get('title', ''),
                'source':   source_name,
                'url':      entry.get('link', ''),
                'published': entry.get('pubDate', ''),
                'timing':   'background',
            }
    return None
