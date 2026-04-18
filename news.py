import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Free RSS feeds by category
# Priority order matters — first match stops further searching
CATEGORY_FEEDS = {
    'political': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://www.cnbc.com/id/10000113/device/rss/rss.html', 'CNBC Politics'),
        ('https://feeds.bbci.co.uk/news/politics/rss.xml', 'BBC Politics'),
        ('https://apnews.com/rss/politics', 'AP News'),
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
        ('https://apnews.com/rss/world-news', 'AP International'),
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
        ('https://sports.yahoo.com/rss/', 'Yahoo Sports'),
        ('https://www.espn.com/espn/rss/news', 'ESPN'),
    ],
    'other': [
        ('https://news.yahoo.com/rss/', 'Yahoo News'),
        ('https://apnews.com/rss/ap-top-news', 'AP News'),
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

def get_event_category(event_title, question):
    """
    Classify the signal category using Groq Llama 3.1 8B.
    Falls back to keyword matching if Groq is unavailable.
    """
    groq_key = os.environ.get('GROQ_API_KEY', '')

    if groq_key:
        try:
            prompt = (
                "Classify this prediction market question into exactly one category.\n\n"
                + f"Event: \"{event_title}\"\n"
                + f"Question: \"{question}\"\n\n"
                + "Categories:\n"
                + "- sports: any sport, athlete, team, match, tournament, score, stats\n"
                + "- esports: gaming tournaments, video game competitions\n"
                + "- political: elections, politicians, government policy, voting\n"
                + "- macro: economy, interest rates, inflation, GDP, central banks, jobs\n"
                + "- geopolitical: wars, international relations, sanctions, diplomacy, military\n"
                + "- commodities: oil, gold, gas, metals, agricultural products\n"
                + "- crypto: bitcoin, ethereum, cryptocurrency, blockchain, DeFi\n"
                + "- other: anything that does not clearly fit above\n\n"
                + "Reply with only the category name, nothing else."
            )
            response = requests.post(
                'https://api.groq.com/openai/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {groq_key}',
                    'Content-Type': 'application/json'
                },
                json={
                    'model': 'llama-3.1-8b-instant',
                    'messages': [{'role': 'user', 'content': prompt}],
                    'max_tokens': 10,
                    'temperature': 0
                },
                timeout=8
            )
            response.raise_for_status()
            result = (response.json()['choices'][0]['message']['content']
                      .strip().lower().split()[0])
            valid = {'sports', 'esports', 'political', 'macro',
                     'geopolitical', 'commodities', 'crypto', 'other'}
            if result in valid:
                return result
        except Exception as e:
            print(f"Groq category error: {e} — falling back to keywords")

    # Fallback: keyword matching
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

    Uses a strict prompt with explicit examples of YES and NO cases.
    Defaults to False on error — better to show no news than wrong news.
    """
    groq_key = os.environ.get('GROQ_API_KEY', '')
    if not groq_key:
        return False  # no key — don't show potentially wrong articles

    try:
        prompt = (
            "You are checking if a news article is directly relevant to a "
            "prediction market contract. Be strict — only say YES if the "
            "article is specifically and directly about the exact same topic.\n\n"
            + f"PREDICTION MARKET CONTRACT:\n"
            + f"Event: {event_title}\n"
            + f"Question: {question}\n\n"
            + f"NEWS ARTICLE HEADLINE:\n{article_headline}\n"
            + (f"ARTICLE CONTEXT:\n{article_description[:400]}\n\n" if article_description else "\n")
            + "Rules:\n"
            + "- YES: article is directly about the same specific event, location, person, or team\n"
            + "- NO: article is about a different event, different country, different team\n"
            + "- NO: article is only loosely related by topic (e.g. weather article about wrong city)\n"
            + "- NO: article mentions a related concept but not this specific contract\n"
            + "- When in doubt, answer NO\n\n"
            + "Answer with only YES or NO, nothing else."
        )
        response = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {groq_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'llama-3.1-8b-instant',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 5,
                'temperature': 0
            },
            timeout=8
        )
        response.raise_for_status()
        answer = (response.json()['choices'][0]['message']['content']
                  .strip().upper())
        is_relevant = answer.startswith('YES')
        if not is_relevant:
            print(f"  Groq: irrelevant — {article_headline[:50]}")
        return is_relevant
    except Exception as e:
        print(f"  Groq relevance error: {e}")
        return False  # on error — don't show potentially wrong articles

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
            combined = title + ' ' + desc

            # Require the PRIMARY search term (first one) to match
            # AND at least one secondary term — reduces false matches
            primary = search_terms[0] if search_terms else ''
            secondary = search_terms[1:] if len(search_terms) > 1 else []

            primary_matches = primary and primary in combined
            secondary_matches = not secondary or any(
                t in combined for t in secondary
            )

            # For single search term — must appear in title (not just description)
            if not secondary:
                if primary not in title:
                    continue
            elif not (primary_matches and secondary_matches):
                continue

            headline = entry.get('title', '')
            description = entry.get('description', '')

            # Final gate — ask Groq with headline + description context
            # Description gives Groq enough to disambiguate e.g.
            # "atmosphere" meaning weather vs political mood
            if not is_article_relevant(headline, event_title, question, description):
                print(f"  Groq rejected: {headline[:60]}")
                continue

            pub_date_str = entry.get('pubDate', '')
            pub_date = parse_article_date(pub_date_str)
            timing = classify_article_timing(pub_date, detected_at)

            articles_found.append({
                'headline': headline,
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
            overall_timing = 'after'
        elif 'simultaneous' in timings:
            overall_timing = 'simultaneous'
        elif 'before' in timings:
            overall_timing = 'before'

    return {
        'vacuum': len(articles_found) == 0,
        'articles': articles_found,
        'timing': overall_timing,
        'search_terms': search_terms,
        'sources_checked': sources_checked,
        'checked_at': datetime.now().isoformat()
    }
