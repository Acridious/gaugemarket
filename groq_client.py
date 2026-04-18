"""
Thin Groq client used across news.py and poller.py.

Kept as a separate module so the API key, model, and retry logic
live in one place rather than being duplicated.
"""

import os
import requests

GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')
GROQ_MODEL   = 'llama-3.1-8b-instant'
GROQ_URL     = 'https://api.groq.com/openai/v1/chat/completions'

# ---------------------------------------------------------------------------
# Per-poll call budget
# ---------------------------------------------------------------------------
# Groq free tier allows ~30 req/min. Each poll cycle is 5 minutes so we have
# headroom, but the inline grouper can flood calls on busy matchdays.
# We split the budget explicitly so high-value calls (news, summaries) always
# get through even when there are many signals to group.
#
# Budget per poll cycle:
#   GROUPER   — cross-event causal linking (nice to have, can be skipped)
#   NEWS      — article relevance filtering (important, affects signal state)
#   SUMMARY   — AI summary generation (important, user-facing)
#   CATEGORY  — category classification fallback (only fires without API tags)

BUDGET = {
    'grouper':  10,   # max Groq calls for inline grouper per poll
    'news':     20,   # max Groq calls for news relevance per poll
    'summary':  15,   # max Groq calls for AI summaries per poll
    'category': 10,   # max Groq calls for category classification per poll
}

_usage = {'grouper': 0, 'news': 0, 'summary': 0, 'category': 0}


def reset_poll_budget():
    """Call this at the start of each poll cycle to reset counters."""
    for k in _usage:
        _usage[k] = 0


def budget_remaining(slot):
    """Returns True if this slot still has budget left."""
    return _usage.get(slot, 0) < BUDGET.get(slot, 0)


def _consume(slot):
    """Increment usage for a slot. Returns False if over budget."""
    if not budget_remaining(slot):
        return False
    _usage[slot] = _usage.get(slot, 0) + 1
    return True


def budget_summary():
    return ' | '.join(f"{k}: {_usage[k]}/{BUDGET[k]}" for k in BUDGET)


def groq_yes_no(prompt, timeout=8, retries=3, slot='news'):
    """
    Ask Groq a YES/NO question.

    slot: budget slot to charge ('grouper', 'news', 'category').
    Returns False immediately if the slot is over budget for this poll.
    Retries with exponential backoff on 429 rate limit errors.
    """
    if not GROQ_API_KEY:
        return False
    if not _consume(slot):
        print(f"  Groq budget exhausted for slot '{slot}' — skipping call")
        return False

    for attempt in range(retries):
        try:
            response = requests.post(
                GROQ_URL,
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
                timeout=timeout,
            )
            if response.status_code == 429:
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"  Groq 429 — waiting {wait}s before retry {attempt+1}/{retries}")
                import time; time.sleep(wait)
                continue
            response.raise_for_status()
            answer = (
                response.json()['choices'][0]['message']['content']
                .strip().upper()
            )
            return answer.startswith('YES')
        except Exception as e:
            if attempt == retries - 1:
                print(f"  Groq error: {e}")
            return False
    return False


def groq_complete(prompt, max_tokens=150, temperature=0.3, timeout=10, retries=3, slot='summary'):
    """
    Ask Groq for a text completion (used for AI summaries).
    slot: budget slot to charge ('summary').
    Returns None immediately if slot is over budget.
    Retries with exponential backoff on 429.
    """
    if not GROQ_API_KEY:
        return None
    if not _consume(slot):
        print(f"  Groq budget exhausted for slot '{slot}' — skipping summary")
        return None

    for attempt in range(retries):
        try:
            response = requests.post(
                GROQ_URL,
                headers={
                    'Authorization': f'Bearer {GROQ_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'model': GROQ_MODEL,
                    'messages': [{'role': 'user', 'content': prompt}],
                    'max_tokens': max_tokens,
                    'temperature': temperature,
                },
                timeout=timeout,
            )
            if response.status_code == 429:
                wait = 2 ** attempt
                print(f"  Groq 429 — waiting {wait}s before retry {attempt+1}/{retries}")
                import time; time.sleep(wait)
                continue
            response.raise_for_status()
            return (
                response.json()['choices'][0]['message']['content']
                .strip()
            )
        except Exception as e:
            if attempt == retries - 1:
                print(f"  Groq completion error: {e}")
            return None
    return None


def groq_available():
    return bool(GROQ_API_KEY)
