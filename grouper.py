"""
Cross-event AI grouper using Groq Llama 3.1 8B.
Runs every 30 minutes.
Reads unvalidated cross-event candidates from DB,
asks Groq if each pair is genuinely about the same real world event,
then updates related_contracts on confirmed signals.
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

def ask_groq(question_a, event_a, question_b, event_b):
    """
    Ask Groq Llama 3.1 8B if two prediction market questions
    are about the same real world event.
    Returns True (related) or False (unrelated).
    """
    if not GROQ_API_KEY:
        return False

    prompt = (
        f"Are these two prediction market questions about the SAME "
        f"real world event at the SAME time in the SAME location?\n\n"
        f'Question 1: "{question_a}" (event: "{event_a}")\n'
        f'Question 2: "{question_b}" (event: "{event_b}")\n\n'
        f"Answer only YES or NO."
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
        answer = response.json()['choices'][0]['message']['content'].strip().upper()
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

        # Don't add duplicates
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

def run_grouper():
    print("AI Grouper starting...")
    print(f"Model: {GROQ_MODEL} via Groq")

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
                is_related = ask_groq(
                    c['question_a'], c['event_title_a'],
                    c['question_b'], c['event_title_b']
                )

                mark_candidate_validated(c['id'], is_related)
                validated += 1

                if is_related:
                    confirmed += 1
                    print(f"  RELATED: {c['question_a'][:45]} "
                          f"<-> {c['question_b'][:45]}")

                    # Add each signal to the other's related_contracts
                    update_signal_related_contracts(
                        c['signal_id_a'],
                        {
                            'question': c['question_b'],
                            'odds': 0,
                            'platform': c['platform_b'],
                            'event_title': c['event_title_b'],
                            'type': 'cross_event'
                        }
                    )
                    update_signal_related_contracts(
                        c['signal_id_b'],
                        {
                            'question': c['question_a'],
                            'odds': 0,
                            'platform': c['platform_a'],
                            'event_title': c['event_title_a'],
                            'type': 'cross_event'
                        }
                    )
                else:
                    print(f"  UNRELATED: removed pair #{c['id']}")

                # Small delay between Groq calls — stay within rate limits
                time.sleep(0.5)

            print(f"Validated {validated} — {confirmed} confirmed related")

        sleep_secs = GROUPER_INTERVAL_MINS * 60
        print(f"Grouper sleeping {GROUPER_INTERVAL_MINS} mins...")
        time.sleep(sleep_secs)

if __name__ == '__main__':
    run_grouper()
