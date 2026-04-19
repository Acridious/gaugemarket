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
# ---------------------------------------------------------------------------
# Groq daily usage tracking
# ---------------------------------------------------------------------------
# Free tier: 14,400 req/day on llama-3.1-8b-instant
# We track calls across all slots to stay well under this limit.
# Hard daily cap set to 12,000 (83% of limit) — leaves headroom for
# news re-checks and burst activity on busy matchdays.
#
# Per-poll budgets set so worst case (288 polls/day) stays under daily cap:
#   grouper:  5 × 288 = 1,440/day
#   news:     10 × 288 = 2,880/day
#   summary:  8 × 288 = 2,304/day
#   category: 5 × 288 = 1,440/day
#   Total:    28 × 288 = 8,064/day — 56% of daily limit, safe headroom

DAILY_CAP = 12_000  # hard stop regardless of per-poll budgets

BUDGET = {
    'grouper':  5,    # cross-event causal linking (nice to have)
    'news':     15,   # article relevance — sports excluded so budget goes to intel
    'summary':  7,    # AI summaries for current poll signals
    'category': 5,    # category classification fallback
    'retry':    6,    # reserved exclusively for retry queue — never touched by main poll
                      # separate slot prevents circular budget exhaustion:
                      # if poll exhausts 'summary', retry still has its own 6 calls
}
# Note: sports in-game signals skip news/summary entirely (see news.py)
# so budgets are exclusively for pre-game sports and intelligence categories

_usage       = {'grouper': 0, 'news': 0, 'summary': 0, 'category': 0, 'retry': 0}
_daily_total = 0        # total calls today across all slots
_daily_reset = None     # date of last reset


# ---------------------------------------------------------------------------
# Per-minute rate limiter
# ---------------------------------------------------------------------------
# Groq free tier: 30 req/min hard limit.
# We target 25 req/min (83%) to stay comfortably under it.
# Tracks call timestamps in a sliding 60-second window — when we're
# approaching the limit we sleep just long enough to let old calls
# fall out of the window rather than hitting 429 and waiting 7 seconds.

import time as _time
import collections as _collections

RATE_LIMIT_PER_MIN = 25       # target, not the hard limit
_call_times = _collections.deque()  # timestamps of recent calls


def _rate_limit_wait():
    """
    Block until it's safe to make another Groq call.
    Checks the sliding 60-second window and sleeps only as long as needed.
    Much cheaper than hitting 429 (1s + 2s + 4s = 7s wasted per retry).
    """
    now = _time.monotonic()

    # Drop calls older than 60 seconds from the window
    while _call_times and now - _call_times[0] > 60:
        _call_times.popleft()

    if len(_call_times) >= RATE_LIMIT_PER_MIN:
        # Oldest call in window — sleep until it's 60s old
        sleep_for = 60 - (now - _call_times[0]) + 0.1
        if sleep_for > 0:
            print(f"  Groq rate limit: sleeping {sleep_for:.1f}s to stay under {RATE_LIMIT_PER_MIN}/min")
            _time.sleep(sleep_for)

    _call_times.append(_time.monotonic())


def _check_daily_reset():
    """Reset daily counter at UTC midnight."""
    global _daily_total, _daily_reset
    from datetime import date
    today = date.today()
    if _daily_reset != today:
        _daily_reset  = today
        _daily_total  = 0


def reset_poll_budget():
    """Call this at the start of each poll cycle to reset per-poll counters."""
    _check_daily_reset()
    for k in _usage:
        _usage[k] = 0


def daily_cap_reached():
    """True if we've hit the daily hard cap across all slots."""
    _check_daily_reset()
    return _daily_total >= DAILY_CAP


def budget_remaining(slot):
    """Returns True if this slot still has budget and daily cap not reached."""
    if daily_cap_reached():
        return False
    return _usage.get(slot, 0) < BUDGET.get(slot, 0)


def _consume(slot):
    """Increment usage for a slot. Returns False if over budget."""
    global _daily_total
    if not budget_remaining(slot):
        return False
    _usage[slot]  = _usage.get(slot, 0) + 1
    _daily_total += 1
    return True


def budget_summary():
    pct = round(_daily_total / DAILY_CAP * 100)
    per_poll = ' | '.join(f"{k}: {_usage[k]}/{BUDGET[k]}" for k in BUDGET)
    return f"daily: {_daily_total}/{DAILY_CAP} ({pct}%) | {per_poll}" 


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
            _rate_limit_wait()  # proactive throttle — avoids 429s
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
                # Unexpected 429 despite rate limiting — back off and retry
                wait = 2 ** attempt
                print(f"  Groq 429 — waiting {wait}s before retry {attempt+1}/{retries}")
                _time.sleep(wait)
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
            _rate_limit_wait()  # proactive throttle
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
                _time.sleep(wait)
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
