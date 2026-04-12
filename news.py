import feedparser
import requests
from datetime import datetime, timedelta

YAHOO_FINANCE_RSS = "https://finance.yahoo.com/rss/"
YAHOO_NEWS_RSS = "https://news.yahoo.com/rss/"

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
        'injury', 'trade', 'transfer', 'roster'
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

def extract_search_terms(event_title, question):
    text = f"{event_title} {question}".lower()
    terms = []
    for group, keywords in RELATED_KEYWORDS.items():
        matching = [kw for kw in keywords if kw in text]
        if matching:
            terms.extend(matching[:2])
    if not terms:
        words = text.split()
        terms = [w for w in words 
                 if len(w) > 4 
                 and w not in ['will', 'when', 'does', 'what',
                               'that', 'this', 'with', 'from',
                               'have', 'been', 'they', 'their']][:3]
    return terms

def check_news_vacuum(event_title, question):
    search_terms = extract_search_terms(event_title, question)
    
    if not search_terms:
        return {
            'vacuum': True,
            'articles': [],
            'checked_at': datetime.now().isoformat()
        }
    
    articles_found = []
    
    feeds_to_check = [YAHOO_FINANCE_RSS, YAHOO_NEWS_RSS]
    
    for feed_url in feeds_to_check:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:
                title = entry.get('title', '').lower()
                summary = entry.get('summary', '').lower()
                
                if any(term in title or term in summary 
                       for term in search_terms):
                    published = entry.get('published', '')
                    articles_found.append({
                        'headline': entry.get('title', ''),
                        'source': 'Yahoo Finance' 
                                  if 'finance' in feed_url 
                                  else 'Yahoo News',
                        'url': entry.get('link', ''),
                        'published': published
                    })
        except Exception as e:
            print(f"News check error: {e}")
            continue
    
    articles_found = articles_found[:3]
    
    return {
        'vacuum': len(articles_found) == 0,
        'articles': articles_found,
        'search_terms': search_terms,
        'checked_at': datetime.now().isoformat()
    }
