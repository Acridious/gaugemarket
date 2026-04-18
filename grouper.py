"""
Cross-event AI grouper using Groq Llama 3.1 8B.
Runs every 30 minutes as a separate Railway worker process.

Reads unvalidated cross-event candidates from DB, asks Groq if each pair
is genuinely related, then updates related_contracts on confirmed signals.

Uses category-aware prompts:
- Sports/esports: strict same-event matching
- Political/macro/geopolitical/commodities: causal relationship detection
"""

import os
import json
import time
import requests
from datetime import datetime

from database import (
    get_unvalidated_candidates, mark_candidate_validated,
    get_signal_by_id, get_connection,
    get_recent_signals_for_grouping,
    save_cross_event_candidate,
    db,
)
from constants import (
    SKIP_WORDS,
    CAUSAL_CATEGORIES,
    SAME_EVENT_CATEGORIES,
)

GROQ_API_KEY        = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL          = 'llama-3.1-8b-instant'
GROUPER_INTERVAL_MINS = 30


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def build_prompt(question_a, event_a, question_b, event_b,
                 category_a='other', category_b='other'):
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
    use_causal = (category_a in CAUSAL_CATEGORIES or
                  category_b in CAUSAL_CATEGORIES)
    return 'causal' if use_causal else 'cross_event'


# ---------------------------------------------------------------------------
# Groq call
# ---------------------------------------------------------------------------

def ask_groq(question_a, event_a, question_b, event_b,
             category_a='other', category_b='other'):
    if not GROQ_API_KEY:
        return False

    prompt = build_prompt(
        question_a, event_a, question_b, event_b,
        category_a, category_b,
    )

    try:
        response = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {GROQ_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'model': GROQ_MODEL,
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 5,
                'temperature': 0,
            },
            timeout=10,
        )
        response.raise_for_status()
        answer = (
            response.json()['choices'][0]['message']['content']
            .strip().upper()
        )
        return answer.startswith('YES')
    except Exception as e:
        print(f"Groq error: {e}")
        return False


# ---------------------------------------------------------------------------
# Signal updates
# ---------------------------------------------------------------------------

def update_signal_related_contracts(signal_id, new_contract):
    with db() as conn:
        rows = conn.run(
            "SELECT related_contracts FROM signals WHERE id = :id",
            id=signal_id,
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
                id=signal_id,
            )


def get_signal_category(signal_id):
    with db() as conn:
        rows = conn.run(
            "SELECT category FROM signals WHERE id = :id",
            id=signal_id,
        )
        return rows[0][0] if rows else 'other'


# ---------------------------------------------------------------------------
# Candidate collection from recent signals
# ---------------------------------------------------------------------------

def collect_candidates_from_recent_signals():
    """
    Look at all signals from last 35 mins across ALL poll cycles.
    Find pairs from different events with keyword overlap.
    Save as candidates for Groq to validate.
    This catches cross-event signals that didn't fire in the same poll.
    """
    signals = get_recent_signals_for_grouping(mins=35)
    if len(signals) < 2:
        print(f"Not enough recent signals to pair ({len(signals)})")
        return

    saved = 0
    for i, sig_a in enumerate(signals):
        for sig_b in signals[i + 1:]:
            if sig_a['event_id'] == sig_b['event_id']:
                continue

            words_a = set(sig_a['question'].lower().split()) - SKIP_WORDS
            words_b = set(sig_b['question'].lower().split()) - SKIP_WORDS
            common  = words_a & words_b

            cat_a = sig_a.get('category', 'other')
            cat_b = sig_b.get('category', 'other')
            is_sports = (cat_a in SAME_EVENT_CATEGORIES or
                         cat_b in SAME_EVENT_CATEGORIES)
            min_common = 1 if is_sports else 2

            if len(common) < min_common:
                continue

            try:
                save_cross_event_candidate(
                    signal_id_a=sig_a['id'],
                    signal_id_b=sig_b['id'],
                    question_a=sig_a['question'],
                    question_b=sig_b['question'],
                    event_title_a=sig_a['event_title'],
                    event_title_b=sig_b['event_title'],
                    platform_a=sig_a['platform'],
                    platform_b=sig_b['platform'],
                )
                saved += 1
            except Exception:
                pass  # duplicate pairs silently ignored

    print(f"Collected {saved} new cross-event candidates from {len(signals)} recent signals")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_grouper():
    print("AI Grouper starting...")
    print(f"Model: {GROQ_MODEL} via Groq")
    print(f"Causal categories: {sorted(CAUSAL_CATEGORIES)}")
    print(f"Same-event categories: {sorted(SAME_EVENT_CATEGORIES)}")

    if not GROQ_API_KEY:
        print("WARNING: GROQ_API_KEY not set — AI grouping disabled")
        print("Add GROQ_API_KEY to Railway variables to enable")

    while True:
        print(f"\n[Grouper] {datetime.utcnow().strftime('%H:%M:%S')} UTC")

        collect_candidates_from_recent_signals()

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
                cat_a = get_signal_category(c['signal_id_a'])
                cat_b = get_signal_category(c['signal_id_b'])

                use_causal   = cat_a in CAUSAL_CATEGORIES or cat_b in CAUSAL_CATEGORIES
                prompt_type  = 'causal' if use_causal else 'same-event'

                is_related = ask_groq(
                    c['question_a'], c['event_title_a'],
                    c['question_b'], c['event_title_b'],
                    cat_a, cat_b,
                )

                mark_candidate_validated(c['id'], is_related)
                validated += 1

                if is_related:
                    confirmed += 1
                    rel_type = get_relationship_type(cat_a, cat_b)
                    print(
                        f"  [{prompt_type}] RELATED ({rel_type}): "
                        f"{c['question_a'][:40]} <-> {c['question_b'][:40]}"
                    )
                    update_signal_related_contracts(
                        c['signal_id_a'],
                        {
                            'question':    c['question_b'],
                            'odds':        0,
                            'platform':    c['platform_b'],
                            'event_title': c['event_title_b'],
                            'type':        rel_type,
                        },
                    )
                    update_signal_related_contracts(
                        c['signal_id_b'],
                        {
                            'question':    c['question_a'],
                            'odds':        0,
                            'platform':    c['platform_a'],
                            'event_title': c['event_title_a'],
                            'type':        rel_type,
                        },
                    )
                else:
                    print(f"  [{prompt_type}] UNRELATED: pair #{c['id']} discarded")

                time.sleep(0.5)  # stay within Groq rate limits

            print(f"Validated {validated} — {confirmed} confirmed related")

        print(f"Grouper sleeping {GROUPER_INTERVAL_MINS} mins...")
        time.sleep(GROUPER_INTERVAL_MINS * 60)


if __name__ == '__main__':
    run_grouper()
