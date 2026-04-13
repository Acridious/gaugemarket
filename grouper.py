"""
Cross-event AI grouper.
Runs every 30 minutes. Takes recent signals and asks Claude
whether cross-event candidates are genuinely about the same
real world event. Updates related_contracts in the DB.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from database import get_connection

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL = 'llama-3.1-8b-instant'
GROUPER_INTERVAL_MINS = 30

def get_recent_signals(mins=35):
    """Get signals from the last N minutes that have cross-event candidates."""
    conn = get_connection()
    cutoff = (datetime.now() - timedelta(minutes=mins)).isoformat()
    rows = conn.run('''
        SELECT id, event_title, question, platform, related_contracts
        FROM signals
        WHERE detected_at > :cutoff
        ORDER BY detected_at DESC
        LIMIT 50
    ''', cutoff=cutoff)
    columns = [c['name'] for c in conn.columns]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_all_recent_questions(mins=35):
    """Get all questions from recent signals for cross-referencing."""
    conn = get_connection()
    cutoff = (datetime.now() - timedelta(minutes=mins)).isoformat()
    rows = conn.run('''
        SELECT DISTINCT question, event_title, platform
        FROM signals
        WHERE detected_at > :cutoff
    ''', cutoff=cutoff)
    columns = [c['name'] for c in conn.columns]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def ask_groq(prompt):
    """Call Groq API using Llama 3.1 8B — fast, free, no rate limit concerns."""
    if not GROQ_API_KEY:
        print("No GROQ_API_KEY set — skipping AI grouping")
        return None
    try:
        response = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': GROQ_MODEL,
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 10,
                'temperature': 0
            },
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        return data['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"Groq API error: {e}")
        return None

def are_same_event(question_a, question_b, event_a, event_b):
    """
    Ask Claude if two questions are about the same real world event.
    Returns True/False. Uses cheapest model (Haiku) for cost efficiency.
    """
    prompt = f"""Are these two prediction market questions about the SAME real world event happening at the SAME time in the SAME location?

Question 1: "{question_a}" (from event: "{event_a}")
Question 2: "{question_b}" (from event: "{event_b}")

Answer only YES or NO. Do not explain."""

    result = ask_groq(prompt)
    if result is None:
        return False
    return result.strip().upper().startswith('YES')

def group_signals_with_ai(signals, all_questions):
    """
    For each signal, validate its cross-event related contracts using AI.
    Remove ones that aren't genuinely related.
    """
    if not GROQ_API_KEY:
        return

    updated = 0

    for signal in signals:
        try:
            rc = signal.get('related_contracts', '[]')
            contracts = json.loads(rc) if isinstance(rc, str) else rc
        except Exception:
            contracts = []

        if not contracts:
            continue

        # Separate same-event (keep as-is) from cross-event (validate with AI)
        same_event = [c for c in contracts if c.get('type') == 'same_event']
        cross_event = [c for c in contracts if c.get('type') == 'cross_event']

        if not cross_event:
            continue  # nothing to validate

        validated_cross = []
        for candidate in cross_event:
            related_q = candidate.get('question', '')
            related_event = candidate.get('event_title', '')

            # Skip if obviously same event title
            if (signal['event_title'].lower()[:30] ==
                    related_event.lower()[:30]):
                validated_cross.append(candidate)
                continue

            is_related = are_same_event(
                signal['question'],
                related_q,
                signal['event_title'],
                related_event
            )

            if is_related:
                validated_cross.append(candidate)
                print(f"  AI: RELATED — {signal['question'][:50]} "
                      f"<-> {related_q[:50]}")
            else:
                print(f"  AI: UNRELATED — removed {related_q[:50]}")

        # Update related_contracts with validated list
        new_contracts = same_event + validated_cross
        if len(new_contracts) != len(contracts):
            conn = get_connection()
            conn.run(
                "UPDATE signals SET related_contracts = :rc WHERE id = :id",
                rc=json.dumps(new_contracts),
                id=signal['id']
            )
            conn.close()
            updated += 1

    print(f"AI grouper: validated {len(signals)} signals, updated {updated}")

def find_new_cross_event_matches(signals, all_questions):
    """
    Also look for cross-platform convergence:
    signals from different platforms about the same real world event.
    """
    if not GROQ_API_KEY or len(signals) < 2:
        return

    # Group signals by platform
    poly_signals = [s for s in signals if s['platform'] == 'Polymarket']
    kal_signals = [s for s in signals if s['platform'] == 'Kalshi']

    if not poly_signals or not kal_signals:
        return

    print(f"Checking cross-platform convergence: "
          f"{len(poly_signals)} Polymarket x {len(kal_signals)} Kalshi signals")

    for poly_sig in poly_signals[:10]:  # limit to avoid too many API calls
        for kal_sig in kal_signals[:10]:

            # Quick keyword pre-filter — avoid obvious mismatches
            poly_words = set(poly_sig['question'].lower().split())
            kal_words = set(kal_sig['question'].lower().split())
            common = poly_words & kal_words - {
                'will', 'the', 'a', 'an', 'be', 'is', 'are',
                'by', 'in', 'on', 'at', 'to', 'for', 'of',
                'win', 'lose', 'before', 'after', 'during'
            }

            if len(common) < 2:
                continue  # not enough overlap to even ask AI

            is_same = are_same_event(
                poly_sig['question'],
                kal_sig['question'],
                poly_sig['event_title'],
                kal_sig['event_title']
            )

            if not is_same:
                continue

            print(f"  CROSS-PLATFORM MATCH FOUND:")
            print(f"  Poly: {poly_sig['question'][:60]}")
            print(f"  Kal:  {kal_sig['question'][:60]}")

            # Add Kalshi signal to Polymarket signal's related contracts
            conn = get_connection()
            try:
                rc_raw = poly_sig.get('related_contracts', '[]')
                existing = json.loads(rc_raw) if isinstance(rc_raw, str) else rc_raw
                already_there = any(
                    c.get('question') == kal_sig['question']
                    for c in existing
                )
                if not already_there:
                    existing.append({
                        'question': kal_sig['question'],
                        'odds': 0,
                        'platform': 'Kalshi',
                        'event_title': kal_sig['event_title'],
                        'type': 'cross_platform'
                    })
                    conn.run(
                        "UPDATE signals SET related_contracts = :rc WHERE id = :id",
                        rc=json.dumps(existing),
                        id=poly_sig['id']
                    )
                    print(f"  Added Kalshi signal to Polymarket signal #{poly_sig['id']}")
            except Exception as e:
                print(f"  Error updating cross-platform: {e}")
            finally:
                conn.close()

def run_grouper():
    """Main grouper loop. Runs every 30 minutes."""
    print("AI Grouper starting...")

    if not GROQ_API_KEY:
        print("WARNING: GROQ_API_KEY not set. AI grouping disabled.")
        print("Set this in Railway variables to enable cross-event validation.")

    while True:
        print(f"\n[Grouper] {datetime.now().strftime('%H:%M:%S')}")

        signals = get_recent_signals(mins=GROUPER_INTERVAL_MINS + 5)
        all_questions = get_all_recent_questions(mins=GROUPER_INTERVAL_MINS + 5)

        print(f"Signals to validate: {len(signals)}")

        if signals and GROQ_API_KEY:
            # Step 1: Remove invalid cross-event matches
            group_signals_with_ai(signals, all_questions)

            # Step 2: Find new cross-platform matches
            find_new_cross_event_matches(signals, all_questions)
        else:
            print(f"Skipping AI validation (no signals or no GROQ_API_KEY)")

        sleep_secs = GROUPER_INTERVAL_MINS * 60
        print(f"Grouper sleeping {GROUPER_INTERVAL_MINS} mins...")
        import time
        time.sleep(sleep_secs)

if __name__ == '__main__':
    run_grouper()
