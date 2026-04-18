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
    # Common sports result format: "Team A to beat Team B"
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
        "You are checking if a news article is directly relevant to a "
        "prediction market contract. Be strict — only say YES if the "
        "article is specifically and directly about the exact same topic.\n\n"
        f"PREDICTION MARKET CONTRACT:\n"
        f"Event: {event_title}\n"
        f"Question: {question}\n\n"
        f"NEWS ARTICLE HEADLINE:\n{article_headline}\n"
        + (f"ARTICLE CONTEXT:\n{article_description[:400]}\n\n" if article_description else "\n")
        + "Rules:\n"
        "- YES: article is directly about the same specific event, location, person, or team\n"
        "- NO: article is about a different event, different country, different team\n"
        "- NO: article is only loosely related by topic (e.g. weather article about wrong city)\n"
        "- NO: article mentions a related concept but not this specific contract\n"
        "- When in doubt, answer NO\n\n"
        "Answer with only YES or NO, nothing else."
    )
    result = groq_yes_no(prompt)
    if not result:
        print(f"  Groq: irrelevant — {article_headline[:50]}")
    return result

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


# ---------------------------------------------------------------------------
# AI summary generation
# ---------------------------------------------------------------------------

def generate_signal_summary(
    event_title, question, prev_odds, current_odds,
    price_move, direction, category,
    news_article=None, news_vacuum=True,
    sports_context=None
):
    """
    Generate a 2-3 sentence AI summary of the signal.

    Two completely different prompts depending on whether news was found:

    NEWS FOUND — summarise what happened and connect it to the contract.
    Example output: "Haaland has been ruled out of Saturday's match with
    a knee injury sustained in training. This directly explains the sharp
    drop in City's win probability from 68% to 51%, as he has scored in
    7 of their last 9 home games."

    NO NEWS (vacuum) — reason about what smart money might know.
    Example output: "No public news explains this move. A 17-point shift
    on a geopolitical contract in under 10 minutes typically suggests
    informed positioning — possible sources include diplomatic back-channels,
    leaked policy decisions, or early intelligence on a military development."
    """
    from groq_client import groq_yes_no, groq_available, GROQ_API_KEY, GROQ_URL, GROQ_MODEL
    import requests as _requests

    if not groq_available():
        return None

    prev_pct    = round(prev_odds * 100)
    curr_pct    = round(current_odds * 100)
    move_pct    = round(price_move * 100)
    dir_word    = 'up' if direction == 'YES' else 'down'

    if news_article and not news_vacuum:
        # ── NEWS FOUND ──────────────────────────────────────────────────
        headline    = news_article.get('headline', '')
        description = news_article.get('description', '') or ''
        source      = news_article.get('source', '')
        timing      = news_article.get('timing', 'unknown')

        timing_note = {
            'before':        'The article was published before the price move.',
            'after':         'The article appeared after the price move — capital moved first.',
            'simultaneous':  'The article and price move appeared at roughly the same time.',
        }.get(timing, '')

        prompt = (
            "You are a prediction market analyst. Write exactly 2-3 sentences "
            "explaining what happened and what it means for this contract. "
            "Be specific and direct. Do not use vague language. "
            "Do not start with 'I' or repeat the question back.\n\n"
            f"CONTRACT: {question}\n"
            f"EVENT: {event_title}\n"
            f"CATEGORY: {category}\n"
            f"PRICE MOVE: {prev_pct}% → {curr_pct}% ({dir_word}, {move_pct} points)\n\n"
            f"NEWS HEADLINE: {headline}\n"
            f"NEWS SOURCE: {source}\n"
            f"ARTICLE CONTEXT: {description[:600]}\n"
            f"TIMING: {timing_note}\n\n"
            "Write 2-3 sentences: first explain what the news says in plain language, "
            "then connect it specifically to why this contract moved in this direction "
            "by this amount. If the article appeared before the move, note the market "
            "may be catching up. If after, note capital moved ahead of public news."
        )
    else:
        # ── NEWS VACUUM ─────────────────────────────────────────────────
        # Category-specific reasoning about what informed money might know
        category_context = {
            'geopolitical': (
                "Geopolitical contracts move slowly by nature. An unexplained move "
                "of this size often reflects off-channel intelligence: diplomatic "
                "back-channels, early military intelligence, or leaked policy decisions."
            ),
            'macro': (
                "Macro contracts rarely move sharply without cause. Unexplained moves "
                "often precede central bank leaks, early access to economic data, "
                "or positioning ahead of a scheduled announcement."
            ),
            'political': (
                "Political contracts without news often reflect internal polling, "
                "leaked decisions, or early knowledge of an announcement before "
                "it reaches the press."
            ),
            'crypto': (
                "Crypto prediction markets can move on on-chain data, large wallet "
                "movements, or exchange intelligence not yet visible in news feeds."
            ),
            'commodities': (
                "Commodity contracts sometimes move on supply-chain intelligence, "
                "early OPEC signalling, or weather/logistics data before it is "
                "publicly reported."
            ),
            'sports': (
                "Pre-game sports contracts without news may reflect lineup leaks, "
                "injury information shared in team circles, or sharp money from "
                "bettors with insider knowledge of team preparations."
            ),
            'esports': (
                "Esports contract moves without news may reflect roster information "
                "or scrim results shared within team communities before public announcement."
            ),
        }.get(category, (
            "Unexplained moves on prediction markets can reflect private information, "
            "early positioning, or off-channel intelligence not yet visible in news."
        ))

        sports_note = ''
        if sports_context == 'large_ingame_move':
            sports_note = ' Note: this contract is moving during a live event — a goal, red card, or key moment may explain the move even without a news article.'
        elif sports_context == 'pre_game_move':
            sports_note = ' This is a pre-game move, suggesting the information may relate to team selection or player availability.'

        prompt = (
            "You are a prediction market analyst. Write exactly 2-3 sentences "
            "about this unusual price movement where NO public news was found. "
            "Be specific and analytical. Do not use vague language. "
            "Do not start with 'I' or repeat the question back.\n\n"
            f"CONTRACT: {question}\n"
            f"EVENT: {event_title}\n"
            f"CATEGORY: {category}\n"
            f"PRICE MOVE: {prev_pct}% → {curr_pct}% ({dir_word}, {move_pct} points)\n\n"
            f"CONTEXT: {category_context}{sports_note}\n\n"
            "Write 2-3 sentences: first describe the move plainly and note there is "
            "no public news to explain it, then give a specific and plausible reason "
            "why informed capital might be moving in this direction based on the "
            "contract topic and category. Be concrete, not generic."
        )

    try:
        response = _requests.post(
            GROQ_URL,
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': GROQ_MODEL,
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 150,
                'temperature': 0.3,   # slight creativity — avoids robotic output
            },
            timeout=10,
        )
        response.raise_for_status()
        summary = (
            response.json()['choices'][0]['message']['content']
            .strip()
        )
        return summary if summary else None
    except Exception as e:
        print(f"  Summary generation error: {e}")
        return None
