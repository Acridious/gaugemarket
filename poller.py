"""
GaugeMarket Poller — runs forever, polls Polymarket every POLL_INTERVAL seconds,
detects unusual price movements, and stores signals in PostgreSQL.

Key fixes vs previous version:
  - timedelta.total_seconds() instead of .seconds (wrapping bug)
  - Shared DB connection (no per-call reconnect)
  - Import json only at top level
  - SKIP_WORDS imported from constants (single source of truth)
  - Sports-aware signal filtering: terminal odds, match-end noise suppression
  - Geopolitical bonus scoring for slow-moving but significant contracts
  - Volume snapshot pruning moved to hourly cleanup
  - Pagination failures logged properly
"""

import requests
import time
import os
import json
from datetime import datetime, timedelta

from database import (
    setup_db, save_snapshot, get_last_snapshot,
    save_signal,
    get_signal_stats, cleanup_old_data,
    save_volume_snapshot,
)
from inline_grouper import run_inline_grouper
from news import check_news_vacuum, get_event_category, get_keyword_group, RELATED_KEYWORDS, generate_signal_summary
from constants import (
    SKIP_WORDS,
    CAUSAL_CATEGORIES,
    SAME_EVENT_CATEGORIES,
    SCORE_STORE_MIN,
    SPORTS_TERMINAL_ODDS_LOW,
    SPORTS_TERMINAL_ODDS_HIGH,
    SPORTS_MIN_SIGNAL_MINS,
    GEOPOLITICAL_MOVE_BONUS,
)

POLYMARKET_API = 'https://gamma-api.polymarket.com'

POLL_INTERVAL    = int(os.environ.get('POLL_INTERVAL', 300))
MIN_PRICE_MOVE   = float(os.environ.get('MIN_PRICE_MOVE', 0.05))
MIN_SIGNAL_SCORE = int(os.environ.get('MIN_SIGNAL_SCORE', SCORE_STORE_MIN))
MIN_VOLUME       = float(os.environ.get('MIN_VOLUME', 1000))


# ---------------------------------------------------------------------------
# Polymarket fetching
# ---------------------------------------------------------------------------

def fetch_polymarket_events():
    """
    Fetch active Polymarket events ordered by 24h volume.
    Paginates up to 500 events (5 pages × 100).
    Logs how many pages succeeded so silent truncation is visible.
    """
    all_events = []
    pages_ok = 0

    for offset in range(0, 500, 100):
        try:
            response = requests.get(
                f'{POLYMARKET_API}/events',
                params={
                    'active': True,
                    'closed': False,
                    'limit': 100,
                    'offset': offset,
                    'order': 'volume24hr',
                    'ascending': False,
                },
                timeout=10,
            )
            response.raise_for_status()
            page = response.json()
            if not page:
                break
            all_events.extend(page)
            pages_ok += 1
        except Exception as e:
            print(f"Polymarket fetch error (offset {offset}, page {pages_ok + 1}/5): {e}")
            break

    print(f"Fetched {len(all_events)} events from Polymarket ({pages_ok}/5 pages OK)")
    return all_events


# Tags that indicate contracts with no information edge — skip entirely.
# Weather moves on public forecast data. Science/space moves on public announcements.
# Reality TV is pure opinion with no tradeable edge.
#
# NOTE: Awards (Oscars, Grammys, BAFTAs etc.) are intentionally NOT skipped —
# academy members and industry insiders have been documented trading on early
# knowledge of results, making these legitimate insider trading signals.
_POLY_SKIP_TAGS = {
    'weather', 'climate',
    'science', 'space', 'nasa', 'astronomy',
    'reality tv', 'reality show',
}

# Fast keyword check for skip categories — used when tags aren't conclusive
_SKIP_QUESTION_PATTERNS = [
    # Weather — purely public forecast data, no edge
    'temperature', 'rainfall', 'snowfall', 'hurricane', 'tornado',
    'inches of rain', 'inches of snow', 'degrees fahrenheit', 'degrees celsius',
    'weather', 'forecast',
    # Reality TV — no information edge
    'will win bachelor', 'will win bachelorette', 'will win survivor',
    'will win big brother', 'will win american idol',
    'dancing with the stars', 'x factor winner',
    # Space / science — public announcements, no edge
    'rocket launch', 'will nasa ', 'will spacex launch',
    'solar flare', 'will the sun',
]

def _should_skip_event(event, question=''):
    """
    Returns True if this contract has no information edge and should be
    filtered out entirely — not stored, not scored, not shown.
    """
    tags = event.get('tags', []) or []
    for tag in tags:
        label = (tag.get('label', '') or '').lower().strip()
        if label in _POLY_SKIP_TAGS:
            return True

    text = f"{event.get('title', '')} {question}".lower()
    return any(p in text for p in _SKIP_QUESTION_PATTERNS)


# Tag label → our category mapping.
# Polymarket's tags array on each event contains labels like "Sports", "Soccer",
# "NBA", "Crypto", "Politics" etc. We map these to our internal categories.
# This is checked BEFORE any pattern matching or Groq — it's authoritative.
_POLY_TAG_CATEGORY = {
    # Sports tags
    'sports': 'sports', 'soccer': 'sports', 'football': 'sports',
    'basketball': 'sports', 'baseball': 'sports', 'hockey': 'sports',
    'tennis': 'sports', 'golf': 'sports', 'rugby': 'sports',
    'cricket': 'sports', 'motorsports': 'sports', 'mma': 'sports',
    'boxing': 'sports', 'cycling': 'sports', 'athletics': 'sports',
    'nba': 'sports', 'nfl': 'sports', 'mlb': 'sports', 'nhl': 'sports',
    'epl': 'sports', 'nba 2k': 'sports', 'ufc': 'sports', 'f1': 'sports',
    'pga': 'sports', 'champions league': 'sports',
    # Esports tags
    'esports': 'esports', 'e-sports': 'esports',
    # Political tags
    'politics': 'political', 'political': 'political', 'elections': 'political',
    'us politics': 'political', 'government': 'political',
    # Macro tags
    'economics': 'macro', 'economy': 'macro', 'finance': 'macro',
    'federal reserve': 'macro', 'interest rates': 'macro',
    # Geopolitical tags
    'geopolitics': 'geopolitical', 'geopolitical': 'geopolitical',
    'world': 'geopolitical', 'international': 'geopolitical',
    'war': 'geopolitical', 'military': 'geopolitical',
    # Crypto tags
    'crypto': 'crypto', 'cryptocurrency': 'crypto', 'bitcoin': 'crypto',
    'ethereum': 'crypto', 'defi': 'crypto', 'blockchain': 'crypto',
    # Commodities tags
    'commodities': 'commodities', 'oil': 'commodities', 'gold': 'commodities',
    'energy': 'commodities',
}

def _category_from_polymarket_event(event):
    """
    Extract category directly from Polymarket's own tags and slug.
    Returns a category string or None if we can't determine it confidently.

    Priority:
    1. Event slug starts with /sports/ — authoritative, always sports
    2. Tags array contains a known sports/category tag
    3. None — fall through to our own categorisation logic
    """
    slug = event.get('slug', '') or ''

    # Slug-based: polymarket.com/sports/epl/epl-new-bou-2026-04-18
    # The slug field in the API is just the path segment e.g. "epl-new-bou-2026-04-18"
    # but the event also has a 'series' or parent context we can check via tags
    # Most reliable: check if any tag label maps to a category
    tags = event.get('tags', []) or []
    for tag in tags:
        label = (tag.get('label', '') or '').lower().strip()
        if label in _POLY_TAG_CATEGORY:
            return _POLY_TAG_CATEGORY[label]

    return None


def process_polymarket_events(events):
    processed = []
    for event in events:
        # Extract Polymarket's own category from tags — authoritative
        poly_category = _category_from_polymarket_event(event)

        markets = event.get('markets', [])
        for market in markets:
            try:
                # Skip weather/entertainment/science before any processing
                if _should_skip_event(event, market.get('question', '')):
                    continue

                outcome_prices = market.get('outcomePrices', '[]')
                if isinstance(outcome_prices, str):
                    prices = json.loads(outcome_prices)
                else:
                    prices = outcome_prices

                if not prices:
                    continue

                yes_odds = float(prices[0]) if prices else 0
                volume   = float(market.get('volume', 0))

                if volume < 1:
                    continue

                processed.append({
                    'market_id':      f"poly_{market.get('id', '')}",
                    'event_id':       f"poly_event_{event.get('id', '')}",
                    'event_title':    event.get('title', ''),
                    'event_slug':     event.get('slug', ''),
                    'market_slug':    market.get('slug', ''),
                    'question':       market.get('question', ''),
                    'odds':           yes_odds,
                    'volume':         volume,
                    'platform':       'Polymarket',
                    'poly_category':  poly_category,  # None if unknown
                })
            except Exception:
                continue

    return processed


# ---------------------------------------------------------------------------
# Sports: terminal odds detection
# ---------------------------------------------------------------------------

def is_terminal_sports_odds(odds, category):
    """
    Returns True if a sports/esports contract's odds have reached a value
    that almost certainly means the match has concluded rather than a genuine
    information signal.

    A contract going to 2% or 98% during or after a match is normal resolution,
    not a signal.  We still store the signal (so the historical record is
    complete) but flag it so the frontend and scoring can treat it differently.
    """
    if category not in SAME_EVENT_CATEGORIES:
        return False
    return odds <= SPORTS_TERMINAL_ODDS_LOW or odds >= SPORTS_TERMINAL_ODDS_HIGH


def sports_context_label(odds, prev_odds, category):
    """
    Human-readable context label for sports signals.
    Tells the frontend WHY a sports contract is moving.
    """
    if category not in SAME_EVENT_CATEGORIES:
        return None

    move = odds - prev_odds

    if odds <= SPORTS_TERMINAL_ODDS_LOW:
        return 'match_resolved_no'   # market settling after loss/elimination
    if odds >= SPORTS_TERMINAL_ODDS_HIGH:
        return 'match_resolved_yes'  # market settling after win/qualification

    # Mid-game drift patterns
    if abs(move) >= 0.20:
        return 'large_ingame_move'   # goal scored, injury, red card etc
    if abs(move) >= 0.10:
        return 'moderate_ingame_move'

    return 'pre_game_move'           # odds shifting before the match starts


# ---------------------------------------------------------------------------
# Team extraction and game-level grouping
# ---------------------------------------------------------------------------

TEAM_ALIASES = {
    # NBA
    'warriors': ['warriors', 'golden state', 'gsw'],
    'suns': ['suns', 'phoenix'],
    'lakers': ['lakers', 'los angeles lakers', 'lal'],
    'celtics': ['celtics', 'boston'],
    'nuggets': ['nuggets', 'denver'],
    'heat': ['heat', 'miami'],
    'bucks': ['bucks', 'milwaukee'],
    'nets': ['nets', 'brooklyn'],
    'knicks': ['knicks', 'new york'],
    'sixers': ['sixers', '76ers', 'philadelphia'],
    'bulls': ['bulls', 'chicago'],
    'cavaliers': ['cavaliers', 'cavs', 'cleveland'],
    'hawks': ['hawks', 'atlanta'],
    'pacers': ['pacers', 'indiana'],
    'magic': ['magic', 'orlando'],
    'raptors': ['raptors', 'toronto'],
    'pistons': ['pistons', 'detroit'],
    'hornets': ['hornets', 'charlotte'],
    'wizards': ['wizards', 'washington'],
    'spurs': ['spurs', 'san antonio'],
    'mavs': ['mavs', 'mavericks', 'dallas'],
    'rockets': ['rockets', 'houston'],
    'grizzlies': ['grizzlies', 'memphis'],
    'pelicans': ['pelicans', 'new orleans'],
    'thunder': ['thunder', 'oklahoma city', 'okc'],
    'jazz': ['jazz', 'utah'],
    'clippers': ['clippers', 'la clippers'],
    'kings': ['kings', 'sacramento'],
    'trailblazers': ['trailblazers', 'blazers', 'portland'],
    'timberwolves': ['timberwolves', 'wolves', 'minnesota'],
    # NFL
    'chiefs': ['chiefs', 'kansas city'],
    'eagles': ['eagles', 'philadelphia'],
    'cowboys': ['cowboys', 'dallas'],
    'patriots': ['patriots', 'new england'],
    'packers': ['packers', 'green bay'],
    '49ers': ['49ers', 'san francisco', 'niners'],
    'ravens': ['ravens', 'baltimore'],
    'bills': ['bills', 'buffalo'],
    # Soccer
    'man city': ['man city', 'manchester city'],
    'man utd': ['man utd', 'manchester united'],
    'arsenal': ['arsenal'],
    'liverpool': ['liverpool'],
    'chelsea': ['chelsea'],
    'real madrid': ['real madrid'],
    'barcelona': ['barcelona', 'barca'],
}


def extract_teams(text):
    text_lower = text.lower()
    return {
        team for team, aliases in TEAM_ALIASES.items()
        if any(alias in text_lower for alias in aliases)
    }


def get_game_key(market):
    teams = extract_teams(
        f"{market.get('event_title', '')} {market.get('question', '')}"
    )
    if len(teams) >= 2:
        return 'game_' + '_'.join(sorted(teams))
    elif len(teams) == 1:
        return f"game_{list(teams)[0]}_{market['event_id']}"
    return market['event_id']


def find_related_markets(signal_market, all_markets):
    same_event = []
    signal_game_key = get_game_key(signal_market)
    signal_teams = extract_teams(
        f"{signal_market.get('event_title', '')} {signal_market.get('question', '')}"
    )

    for market in all_markets:
        if market['market_id'] == signal_market['market_id']:
            continue

        if market['event_id'] == signal_market['event_id']:
            same_event.append(market)
            continue

        market_game_key = get_game_key(market)
        if (market_game_key == signal_game_key and
                signal_game_key != signal_market['event_id']):
            same_event.append(market)
            continue

        market_teams = extract_teams(
            f"{market.get('event_title', '')} {market.get('question', '')}"
        )
        if signal_teams and market_teams and (signal_teams & market_teams):
            same_event.append(market)

    # Deduplicate
    seen = set()
    deduped = []
    for m in same_event:
        if m['market_id'] not in seen:
            seen.add(m['market_id'])
            deduped.append(m)

    return deduped, []  # cross_event handled by AI grouper


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_signal(price_move, mins_elapsed, same_event_count, cross_event_count,
                 is_cross_platform, news_vacuum, volume=0, category='other',
                 is_terminal=False):
    """
    Score a signal 0-100+.

    Key business logic differences by category:
    - Sports: terminal-odds moves are scored at 0 (resolved, not a signal)
    - Sports: very fast large moves (goal scored) are interesting even without
      the news vacuum, so the speed bonus applies in full
    - Geopolitical: contracts move slowly by nature, so a 5-10% move is
      relatively more significant and earns a bonus
    - Macro/political: cross-platform confirmation is the strongest signal
      because these markets are liquid on multiple platforms
    """
    # Terminal sports odds = match over, not a signal
    if is_terminal:
        return 0

    score = 0

    # --- Price move ---
    if price_move >= 0.30:   score += 40
    elif price_move >= 0.20: score += 30
    elif price_move >= 0.10: score += 20
    elif price_move >= 0.05: score += 10
    elif price_move >= 0.02: score += 5

    # --- Speed ---
    if mins_elapsed <= 5:    score += 25
    elif mins_elapsed <= 15: score += 20
    elif mins_elapsed <= 30: score += 10

    # --- Cross-platform convergence ---
    if is_cross_platform:    score += 20

    # --- Related markets moving together ---
    related_count = same_event_count + cross_event_count
    if related_count >= 3:   score += 15
    elif related_count >= 1: score += 8

    # --- News vacuum (no public explanation found) ---
    if news_vacuum:          score += 10

    # --- Volume credibility ---
    if volume >= 1_000_000:  score += 15
    elif volume >= 100_000:  score += 10
    elif volume >= 10_000:   score += 5
    elif volume < 1_000:     score -= 5

    # --- Category-specific adjustments ---
    if category == 'geopolitical':
        # Geopolitical contracts move slowly by nature.
        # A 5-10% move on a geo contract in 30 mins is more significant
        # than the same move on a sports binary outcome.
        score += GEOPOLITICAL_MOVE_BONUS

    if category in SAME_EVENT_CATEGORIES:
        # Sports contracts legitimately swing large percentages mid-game.
        # Without this, every goal, injury, or wicket would score too high.
        # We keep the speed bonus (fast = genuinely unusual even in sports)
        # but dampen the price move component slightly for small moves.
        if price_move < 0.10 and mins_elapsed > 15:
            score -= 5  # slow small sports move = normal pre-match drift

    return max(score, 0)


# ---------------------------------------------------------------------------
# Related contracts builder
# ---------------------------------------------------------------------------

def _build_related_contracts(same_event, cross_event):
    contracts = []

    for m in same_event:
        prev = get_last_snapshot(m['market_id'])
        contracts.append({
            'question':    m['question'],
            'odds':        m['odds'],
            'prev_odds':   prev['odds'] if prev else m['odds'],
            'platform':    m['platform'],
            'event_title': m.get('event_title', ''),
            'type':        'same_event',
        })

    for m in cross_event:
        contracts.append({
            'question':    m['question'],
            'odds':        m['odds'],
            'prev_odds':   m['odds'],
            'platform':    m['platform'],
            'event_title': m.get('event_title', ''),
            'type':        'cross_event',
        })

    return contracts


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

def detect_signals(all_markets):
    signals = []
    now = datetime.utcnow()

    for market in all_markets:
        # Only save snapshot if odds changed — cuts storage ~95%
        prev = get_last_snapshot(market['market_id'])
        if prev is None or abs(market['odds'] - prev['odds']) > 0.001:
            save_snapshot(
                market['market_id'],
                market['event_id'],
                market['event_title'],
                market['question'],
                market['odds'],
                market['volume'],
                market['platform'],
            )

        if not prev:
            continue

        prev_odds    = prev['odds']
        current_odds = market['odds']
        price_move   = abs(current_odds - prev_odds)

        if price_move < MIN_PRICE_MOVE:
            continue

        prev_time = datetime.fromisoformat(prev['timestamp'])
        # FIX: use total_seconds() — .seconds wraps at 3600 and gives wrong
        # results for elapsed times over 1 hour
        mins_elapsed = (now - prev_time).total_seconds() / 60

        if mins_elapsed > 60:
            continue

        # Use Polymarket's own tag-based category first — it's authoritative.
        # Only fall through to our own categorisation (pattern match + Groq)
        # if the API didn't give us a clear tag.
        category = (
            market.get('poly_category')
            or get_event_category(market['event_title'], market['question'])
        )

        # ---- Sports-specific guard ----
        # Reject signals from markets that have already resolved (terminal odds).
        # A contract going to 0% or 100% as a match ends is routine, not a signal.
        terminal = is_terminal_sports_odds(current_odds, category)

        # Also skip very early in a sports match — the first 2 minutes of
        # poll data often reflect stale pre-event snapshots catching up.
        if category in SAME_EVENT_CATEGORIES and mins_elapsed < SPORTS_MIN_SIGNAL_MINS:
            continue

        same_event, cross_event = find_related_markets(market, all_markets)

        is_cross_platform = any(
            m['platform'] != market['platform'] for m in cross_event
        )

        news_result = check_news_vacuum(
            market['event_title'],
            market['question'],
            category=category,
            signal_detected_at=now.isoformat(),
        )

        signal_score = score_signal(
            price_move,
            mins_elapsed,
            len(same_event),
            len(cross_event),
            is_cross_platform,
            news_result['vacuum'],
            volume=market['volume'],
            category=category,
            is_terminal=terminal,
        )

        if signal_score < MIN_SIGNAL_SCORE:
            continue

        direction = 'YES' if current_odds > prev_odds else 'NO'

        news_article = (
            news_result['articles'][0] if news_result['articles'] else None
        )

        sports_label = sports_context_label(current_odds, prev_odds, category)

        # Generate AI summary — runs after news check so it has article context.
        # For vacuum signals: reasons about what informed capital might know.
        # For news signals: connects article content to the specific contract move.
        ai_summary = generate_signal_summary(
            event_title=market['event_title'],
            question=market['question'],
            prev_odds=prev_odds,
            current_odds=current_odds,
            price_move=price_move,
            direction=direction,
            category=category,
            news_article=news_article,
            news_vacuum=news_result['vacuum'],
            sports_context=sports_label,
        )

        signal = {
            'event_id':            market['event_id'],
            'event_title':         market['event_title'],
            'question':            market['question'],
            'platform':            market['platform'],
            'prev_odds':           prev_odds,
            'current_odds':        current_odds,
            'price_move':          price_move,
            'direction':           direction,
            'volume':              market['volume'],
            'score':               signal_score,
            'related_same_event':  len(same_event),
            'related_cross_event': len(cross_event),
            'is_cross_platform':   is_cross_platform,
            'news_vacuum':         news_result['vacuum'],
            'news_timing':         news_result.get('timing', 'unknown'),
            'news_headline':       news_article['headline'] if news_article else None,
            'news_source':         news_article['source'] if news_article else None,
            'news_url':            news_article['url'] if news_article else None,
            'category':            category,
            'detected_at':         now.isoformat(),
            'is_terminal':         terminal,
            'mins_elapsed':        round(mins_elapsed, 1),
            'sports_context':      sports_label,
            'ai_summary':          ai_summary,
            'market_url': (
                f"https://polymarket.com/event/{market.get('event_slug')}"
                if market.get('event_slug')
                else 'https://polymarket.com'
            ),
            'related_contracts': json.dumps(
                _build_related_contracts(same_event[:4], cross_event[:3])
            ),
        }

        signal_id = save_signal(signal)
        signal['db_id'] = signal_id
        signals.append(signal)

        confidence = (
            'EXTREME' if signal_score >= 80
            else 'HIGH' if signal_score >= 70
            else 'MEDIUM' if signal_score >= 60
            else 'LOW'
        )

        terminal_note = ' [TERMINAL — match likely over]' if terminal else ''
        print(f'''
{"="*60}
SIGNAL DETECTED [{confidence}] Score: {signal_score}{terminal_note}
{"="*60}
Event:     {market["event_title"]}
Question:  {market["question"]}
Platform:  {market["platform"]}
Move:      {prev_odds:.1%} → {current_odds:.1%} ({direction})
Change:    +{price_move:.1%} in {mins_elapsed:.1f} mins
Category:  {category}{f" / {sports_label}" if sports_label else ""}
Related:   {len(same_event)} same-event | {len(cross_event)} cross-event
Summary:   {ai_summary[:80] + "..." if ai_summary and len(ai_summary) > 80 else ai_summary or "—"}
News:      {"VACUUM" if news_result["vacuum"] else f"FOUND ({news_result.get("timing","?")}): {news_article["headline"][:60] if news_article else ""}..."}
{"="*60}
        ''')

    return signals


# ---------------------------------------------------------------------------
# Cross-event candidate collection
# ---------------------------------------------------------------------------

def collect_cross_event_candidates(signals):
    if len(signals) < 2:
        return

    candidates_saved = 0

    for i, sig_a in enumerate(signals):
        for sig_b in signals[i + 1:]:
            if sig_a['event_id'] == sig_b['event_id']:
                continue

            words_a = set(sig_a['question'].lower().split()) - SKIP_WORDS
            words_b = set(sig_b['question'].lower().split()) - SKIP_WORDS
            common  = words_a & words_b

            # Sports: a single shared word (team / tournament name) is enough
            cat_a = sig_a.get('category', 'other')
            cat_b = sig_b.get('category', 'other')
            is_sports = cat_a in SAME_EVENT_CATEGORIES or cat_b in SAME_EVENT_CATEGORIES
            min_common = 1 if is_sports else 2

            if len(common) < min_common:
                continue

            sig_a_id = sig_a.get('db_id')
            sig_b_id = sig_b.get('db_id')
            if not sig_a_id or not sig_b_id:
                continue

            try:
                save_cross_event_candidate(
                    signal_id_a=sig_a_id,
                    signal_id_b=sig_b_id,
                    question_a=sig_a['question'],
                    question_b=sig_b['question'],
                    event_title_a=sig_a['event_title'],
                    event_title_b=sig_b['event_title'],
                    platform_a=sig_a['platform'],
                    platform_b=sig_b['platform'],
                )
                candidates_saved += 1
            except Exception as e:
                print(f"Error saving candidate: {e}")

    if candidates_saved:
        print(f"Saved {candidates_saved} cross-event candidates for Groq")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    print("GaugeMarket Poller starting...")
    setup_db()

    poll_count = 0

    while True:
        poll_count += 1
        print(f"\n[Poll #{poll_count}] {datetime.utcnow().strftime('%H:%M:%S')} UTC")

        poly_events  = fetch_polymarket_events()
        poly_markets = process_polymarket_events(poly_events)
        print(f"Polymarket: {len(poly_markets)} active markets")

        all_markets  = poly_markets
        total_volume = sum(m.get('volume', 0) for m in all_markets)
        save_volume_snapshot(total_volume, len(all_markets))
        print(f"Total: {len(all_markets)} markets | ${total_volume:,.0f} volume")

        signals = detect_signals(all_markets)
        run_inline_grouper(signals)  # causal linking via Groq, inline

        # Cleanup runs hourly (every 12 polls at 5-min interval)
        if poll_count % 12 == 0:
            cleanup_old_data()

        stats = get_signal_stats()
        print(
            f"Signals today: {stats['today']} | "
            f"Total: {stats['total']} | "
            f"High confidence: {stats['high_confidence']}"
        )

        print(f"Sleeping {POLL_INTERVAL}s until next poll...")
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run()
