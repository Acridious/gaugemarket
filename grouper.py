"""
Cross-event AI grouper using Groq Llama 3.1 8B.
Runs every 30 minutes.
Reads unvalidated cross-event candidates from DB,
asks Groq if each pair is genuinely related,
then updates related_contracts on confirmed signals.

Uses category-aware prompts:
- Sports/esports: strict same-event matching
- Political/macro/geopolitical/commodities: causal relationship detection
"""

import os
import json
import time
import requests
from datetime import datetime
from database import (get_unvalidated_candidates, mark_candidate_validated,
                      get_signal_by_id, get_connection)

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL = 'llama-3.1-8b-instant'
GROUPER_INTERVAL_MINS = 30

# Categories where causal relationships matter
# e.g. Fed cuts → Bitcoin rallies, Iran ceasefire → oil drops
CAUSAL_CATEGORIES = {
    'political', 'macro', 'geopolitical', 'commodities', 'crypto'
}

# Categories where strict same-event matching is correct
# e.g. same match, same tournament, same player
SAME_EVENT_CATEGORIES = {
    'sports', 'esports', 'other'
}

def build_prompt(question_a, event_a, question_b, event_b,
                 category_a='other', category_b='other'):
    """
    Build the right prompt based on signal categories.

    For causal categories (macro, political, geopolitical):
    Ask whether one event logically affects the probability of the other.
    Captures cross-market spillover and downstream effects.

    For same-event categories (sports, esports):
    Ask whether both questions are about the exact same real world event.
    Avoids grouping unrelated matches or tournaments.
    """

    # Use causal prompt if either signal is in a causal category
    use_causal = (category_a in CAUSAL_CATEGORIES or
                  category_b in CAUSAL_CATEGORIES)

    if use_causal:
        return (
            f"In financial and political prediction markets, "
            f"would a major unexpected move on this question:\n"
            f'"{question_a}"\n\n'
            f"logically and directly affect the probability of "
            f"this question:\n"
            f'"{question_b}"\n\n'
            f"Consider: direct causation, strong historical correlations, "
            f"shared underlying drivers (e.g. same political figure, "
            f"same geopolitical event, same economic indicator).\n"
            f"Do NOT answer YES just because both involve finance or politics generally.\n"
            f"Only answer YES if there is a clear, specific causal or "
            f"correlational link.\n"
            f"Answer only YES or NO."
        )
    else:
        return (
            f"Are these two prediction market questions about the EXACT SAME "
            f"real world event at the SAME time in the SAME location?\n\n"
            f'Question 1: "{question_a}" (event: "{event_a}")\n'
            f'Question 2: "{question_b}" (event: "{event_b}")\n\n'
            f"Answer only YES or NO."
        )

def get_relationship_type(category_a, category_b):
    """Return the relationship type label for the card."""
    use_causal = (category_a in CAUSAL_CATEGORIES or
                  category_b in CAUSAL_CATEGORIES)
    if use_causal:
        return 'causal'
    return 'cross_event'

def ask_groq(question_a, event_a, question_b, event_b,
             category_a='other', category_b='other'):
    """
    Ask Groq whether two signals are related.
    Uses category-aware prompts.
    Returns True (related) or False (unrelated).
    """
    if not GROQ_API_KEY:
        return False

    prompt = build_prompt(
        question_a, event_a, question_b, event_b,
        category_a, category_b
    )

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
                'max_tokens': 5,
                'temperature': 0
            },
            timeout=10
        )
        response.raise_for_status()
        answer = (response.json()['choices'][0]['message']['content']
                  .strip().upper())
        return answer.startswith('YES')
    except Exception as e:
        print(f"Groq error: {e}")
        return False

def update_signal_related_contracts(signal_id, new_contract):
    """Add a validated related contract to a signal's related_contracts JSON."""
    conn = get_connection()
    try:
        rows = conn.run(
            "SELECT related_contracts FROM signals WHERE id = :id",
            id=signal_id
        )
        if not rows:
            return

        existing_raw = rows[0][0] or '[]'
        try:
            existing = json.loads(existing_raw)
        except Exception:
            existing = []

        already_there = any(
            c.get('question') == new_contract['question']
            for c in existing
        )
        if not already_there:
            existing.append(new_contract)
            conn.run(
                "UPDATE signals SET related_contracts = :rc, "
                "related_cross_event = related_cross_event + 1 "
                "WHERE id = :id",
                rc=json.dumps(existing),
                id=signal_id
            )
    except Exception as e:
        print(f"Error updating signal {signal_id}: {e}")
    finally:
        conn.close()

def get_signal_category(signal_id):
    """Get the category of a signal from the DB."""
    conn = get_connection()
    try:
        rows = conn.run(
            "SELECT category FROM signals WHERE id = :id",
            id=signal_id
        )
        return rows[0][0] if rows else 'other'
    except Exception:
        return 'other'
    finally:
        conn.close()

def run_grouper():
    print("AI Grouper starting...")
    print(f"Model: {GROQ_MODEL} via Groq")
    print(f"Causal categories: {sorted(CAUSAL_CATEGORIES)}")
    print(f"Same-event categories: {sorted(SAME_EVENT_CATEGORIES)}")

    if not GROQ_API_KEY:
        print("WARNING: GROQ_API_KEY not set — AI grouping disabled")
        print("Add GROQ_API_KEY to Railway variables to enable")

    while True:
        print(f"\n[Grouper] {datetime.now().strftime('%H:%M:%S')}")

        candidates = get_unvalidated_candidates(limit=50)
        print(f"Unvalidated candidates: {len(candidates)}")

        if not candidates:
            print("No candidates to validate")
        elif not GROQ_API_KEY:
            print("Skipping — no GROQ_API_KEY set")
        else:
            validated = 0
            confirmed = 0

            for c in candidates:
                # Get categories for both signals
                cat_a = get_signal_category(c['signal_id_a'])
                cat_b = get_signal_category(c['signal_id_b'])

                use_causal = (cat_a in CAUSAL_CATEGORIES or
                              cat_b in CAUSAL_CATEGORIES)
                prompt_type = 'causal' if use_causal else 'same-event'

                is_related = ask_groq(
                    c['question_a'], c['event_title_a'],
                    c['question_b'], c['event_title_b'],
                    cat_a, cat_b
                )

                mark_candidate_validated(c['id'], is_related)
                validated += 1

                if is_related:
                    confirmed += 1
                    rel_type = get_relationship_type(cat_a, cat_b)
                    print(f"  [{prompt_type}] RELATED ({rel_type}): "
                          f"{c['question_a'][:40]} "
                          f"<-> {c['question_b'][:40]}")

                    update_signal_related_contracts(
                        c['signal_id_a'],
                        {
                            'question': c['question_b'],
                            'odds': 0,
                            'platform': c['platform_b'],
                            'event_title': c['event_title_b'],
                            'type': rel_type
                        }
                    )
                    update_signal_related_contracts(
                        c['signal_id_b'],
                        {
                            'question': c['question_a'],
                            'odds': 0,
                            'platform': c['platform_a'],
                            'event_title': c['event_title_a'],
                            'type': rel_type
                        }
                    )
                else:
                    print(f"  [{prompt_type}] UNRELATED: "
                          f"pair #{c['id']} discarded")

                # Small delay between Groq calls
                time.sleep(0.5)

            print(f"Validated {validated} — {confirmed} confirmed related")

        sleep_secs = GROUPER_INTERVAL_MINS * 60
        print(f"Grouper sleeping {GROUPER_INTERVAL_MINS} mins...")
        time.sleep(sleep_secs)

if __name__ == '__main__':
    run_grouper()
