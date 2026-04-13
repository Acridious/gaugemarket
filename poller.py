import requests
import time
import os
import json
from datetime import datetime
from database import (setup_db, save_snapshot, get_last_snapshot,
                      save_signal, save_cross_event_candidate,
                      get_signal_stats)
from news import (check_news_vacuum, get_event_category, 
                  get_keyword_group, RELATED_KEYWORDS)

POLYMARKET_API = 'https://gamma-api.polymarket.com'
KALSHI_API = 'https://trading-api.kalshi.com/trade-api/v2'

POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 300))
MIN_PRICE_MOVE = float(os.environ.get('MIN_PRICE_MOVE', 0.05))
MIN_SIGNAL_SCORE = int(os.environ.get('MIN_SIGNAL_SCORE', 50))
MIN_VOLUME = float(os.environ.get('MIN_VOLUME', 1000))

def fetch_polymarket_events():
    try:
        response = requests.get(
            f'{POLYMARKET_API}/events',
            params={
                'active': True,
                'closed': False,
                'limit': 100,
                'order': 'volume',
                'ascending': False
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Polymarket fetch error: {e}")
        return []

def fetch_kalshi_markets():
    # Kalshi now requires authentication on their API
    # Returning empty list until auth is configured
    # Polymarket data is sufficient for signal detection
    return []

def process_polymarket_events(events):
    processed = []
    for event in events:
        markets = event.get('markets', [])
        for market in markets:
            try:
                outcome_prices = market.get('outcomePrices', '[]')
                if isinstance(outcome_prices, str):
                    import json
                    prices = json.loads(outcome_prices)
                else:
                    prices = outcome_prices
                
                if not prices:
                    continue
                
                yes_odds = float(prices[0]) if prices else 0
                volume = float(market.get('volume', 0))
                
                if volume < MIN_VOLUME:
                    continue
                
                processed.append({
                    'market_id': f"poly_{market.get('id', '')}",
                    'event_id': f"poly_event_{event.get('id', '')}",
                    'event_title': event.get('title', ''),
                    'question': market.get('question', ''),
                    'odds': yes_odds,
                    'volume': volume,
                    'platform': 'Polymarket'
                })
            except Exception as e:
                continue
    
    return processed

def process_kalshi_markets(markets):
    processed = []
    for market in markets:
        try:
            yes_price = market.get('yes_ask', 0)
            if isinstance(yes_price, str):
                yes_price = float(yes_price) / 100
            else:
                yes_price = float(yes_price) / 100
            
            volume = float(market.get('volume', 0))
            
            if volume < MIN_VOLUME:
                continue
            
            processed.append({
                'market_id': f"kal_{market.get('ticker', '')}",
                'event_id': f"kal_event_{market.get('event_ticker', '')}",
                'event_title': market.get('event_title', 
                               market.get('title', '')),
                'question': market.get('title', ''),
                'odds': yes_price,
                'volume': volume,
                'platform': 'Kalshi'
            })
        except Exception as e:
            continue
    
    return processed

def find_related_markets(signal_market, all_markets):
    """
    Same-event grouping uses Polymarket's own event structure — free and accurate.
    Cross-event grouping is handled by the AI grouper (grouper.py) every 30 mins.
    """
    same_event = []

    for market in all_markets:
        if market['market_id'] == signal_market['market_id']:
            continue
        if market['event_id'] == signal_market['event_id']:
            same_event.append(market)

    return same_event, []  # cross_event left empty — AI grouper handles this

def score_signal(price_move, mins_elapsed, 
                 same_event_count, cross_event_count,
                 is_cross_platform, news_vacuum):
    score = 0
    
    if price_move >= 0.30:   score += 40
    elif price_move >= 0.20: score += 30
    elif price_move >= 0.10: score += 20
    elif price_move >= 0.05: score += 10
    
    if mins_elapsed <= 5:    score += 25
    elif mins_elapsed <= 15: score += 20
    elif mins_elapsed <= 30: score += 10
    
    if is_cross_platform:    score += 20
    
    related_count = same_event_count + cross_event_count
    if related_count >= 3:   score += 15
    elif related_count >= 1: score += 8
    
    if news_vacuum:          score += 10
    
    return score

def detect_signals(all_markets):
    signals = []
    now = datetime.now()
    
    for market in all_markets:
        prev = get_last_snapshot(market['market_id'])
        
        save_snapshot(
            market['market_id'],
            market['event_id'],
            market['event_title'],
            market['question'],
            market['odds'],
            market['volume'],
            market['platform']
        )
        
        if not prev:
            continue
        
        prev_odds = prev['odds']
        current_odds = market['odds']
        price_move = abs(current_odds - prev_odds)
        
        if price_move < MIN_PRICE_MOVE:
            continue
        
        prev_time = datetime.fromisoformat(prev['timestamp'])
        mins_elapsed = (now - prev_time).seconds / 60
        
        if mins_elapsed > 60:
            continue
        
        same_event, cross_event = find_related_markets(
            market, all_markets
        )
        
        is_cross_platform = any(
            m['platform'] != market['platform'] 
            for m in cross_event
        )

        category = get_event_category(
            market['event_title'],
            market['question']
        )
        
        news_result = check_news_vacuum(
            market['event_title'],
            market['question'],
            category=category,
            signal_detected_at=now.isoformat()
        )
        
        signal_score = score_signal(
            price_move,
            mins_elapsed,
            len(same_event),
            len(cross_event),
            is_cross_platform,
            news_result['vacuum']
        )
        
        if signal_score < MIN_SIGNAL_SCORE:
            continue
        
        direction = 'YES' if current_odds > prev_odds else 'NO'
        
        news_article = (news_result['articles'][0] 
                       if news_result['articles'] else None)
        
        signal = {
            'event_id': market['event_id'],
            'event_title': market['event_title'],
            'question': market['question'],
            'platform': market['platform'],
            'prev_odds': prev_odds,
            'current_odds': current_odds,
            'price_move': price_move,
            'direction': direction,
            'volume': market['volume'],
            'score': signal_score,
            'related_same_event': len(same_event),
            'related_cross_event': len(cross_event),
            'is_cross_platform': is_cross_platform,
            'news_vacuum': news_result['vacuum'],
            'news_timing': news_result.get('timing', 'unknown'),
            'news_headline': (news_article['headline'] 
                             if news_article else None),
            'news_source': (news_article['source'] 
                           if news_article else None),
            'news_url': (news_article['url'] 
                        if news_article else None),
            'category': category,
            'detected_at': now.isoformat(),
            'related_contracts': json.dumps([
                {
                    'question': m['question'],
                    'odds': m['odds'],
                    'platform': m['platform'],
                    'event_title': m.get('event_title', ''),
                    'type': 'same_event'
                }
                for m in same_event[:4]
            ] + [
                {
                    'question': m['question'],
                    'odds': m['odds'],
                    'platform': m['platform'],
                    'event_title': m.get('event_title', ''),
                    'type': 'cross_event'
                }
                for m in cross_event[:3]
            ])
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
        
        print(f'''
{"="*60}
SIGNAL DETECTED [{confidence}] Score: {signal_score}
{"="*60}
Event:     {market['event_title']}
Question:  {market['question']}
Platform:  {market['platform']}
Move:      {prev_odds:.1%} → {current_odds:.1%} ({direction})
Change:    +{price_move:.1%} in {mins_elapsed:.0f} mins
Category:  {category}
Related:   {len(same_event)} same-event | {len(cross_event)} cross-event
Cross-plat:{is_cross_platform}
News:      {"VACUUM - no articles found" if news_result["vacuum"] 
            else f"FOUND: {news_article['headline'][:60]}..."}
{"="*60}
        ''')
    
    return signals

def collect_cross_event_candidates(signals):
    if len(signals) < 2:
        return

    SKIP_WORDS = {
        'will', 'the', 'a', 'an', 'be', 'is', 'are', 'by', 'in',
        'on', 'at', 'to', 'for', 'of', 'win', 'lose', 'before',
        'after', 'during', 'most', 'least', 'first', 'last', 'next',
        'have', 'has', 'had', 'does', 'did', 'when', 'what', 'which',
        'that', 'this', 'with', 'from', 'than', 'more', 'there'
    }

    candidates_saved = 0

    for i, sig_a in enumerate(signals):
        for sig_b in signals[i+1:]:
            if sig_a['event_id'] == sig_b['event_id']:
                continue

            words_a = set(sig_a['question'].lower().split()) - SKIP_WORDS
            words_b = set(sig_b['question'].lower().split()) - SKIP_WORDS
            common = words_a & words_b

            if len(common) < 2:
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
                    platform_b=sig_b['platform']
                )
                candidates_saved += 1
            except Exception as e:
                print(f"  Error saving candidate: {e}")

    if candidates_saved:
        print(f"Saved {candidates_saved} cross-event candidates for Groq")

def run():
    print("GaugeMarket Poller starting...")
    setup_db()
    
    poll_count = 0
    
    while True:
        poll_count += 1
        print(f"\n[Poll #{poll_count}] {datetime.now().strftime('%H:%M:%S')}")
        
        poly_events = fetch_polymarket_events()
        poly_markets = process_polymarket_events(poly_events)
        print(f"Polymarket: {len(poly_markets)} active markets")
        
        kal_markets_raw = fetch_kalshi_markets()
        kal_markets = process_kalshi_markets(kal_markets_raw)
        print(f"Kalshi: {len(kal_markets)} active markets")
        
        all_markets = poly_markets + kal_markets
        print(f"Total: {len(all_markets)} markets to monitor")
        
        signals = detect_signals(all_markets)
        collect_cross_event_candidates(signals)
        
        stats = get_signal_stats()
        print(f"Signals today: {stats['today']} | "
              f"Total: {stats['total']} | "
              f"High confidence: {stats['high_confidence']}")
        
        print(f"Sleeping {POLL_INTERVAL}s until next poll...")
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    run()
