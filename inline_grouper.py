"""
Inline causal grouper — runs synchronously at the end of each poll cycle.

Replaces the 30-minute async grouper process. By running inline, causal
links are attached to signal cards before they ever appear in the feed.

Architecture
------------
1. Pre-filter with cheap heuristics to eliminate pairs that Groq would
   obviously reject. Keeps Groq call count low even with 20+ signals.

2. For surviving pairs, ask Groq a category-aware prompt:
   - Sports/esports: "Are these the exact same real-world event?"
   - Causal categories: "Would a move on X logically affect Y?"
     The Haaland/Arsenal example works here because Groq understands
     that two top-of-table title contenders are causally linked even
     with zero shared keywords.

3. Confirmed pairs get their related_contracts updated in the DB
   and in the in-memory signal dicts so the API reflects it immediately.

Pre-filter rules (applied before any Groq call)
------------------------------------------------
- Same event_id → already grouped by the poller, skip
- Same category pair that is sports × causal → skip (no causal link)
- Both contracts have 0 keyword overlap AND both are sports → skip
  (two sports contracts from different sports are almost never related)
- Score sum < 100 → at least one signal is weak, probably not worth linking

Groq call budget per poll
--------------------------
n signals → up to n*(n-1)/2 pairs before pre-filter.
Pre-filter typically eliminates 70-90% of pairs.
For 10 signals: ~45 raw pairs → ~5-10 Groq calls.
For 20 signals: ~190 raw pairs → ~15-25 Groq calls.
At Groq free tier speeds (~100ms each) this adds 1-3s to the poll cycle,
which is negligible given the 5-minute poll interval.
"""

import json
import time
from groq_client import groq_yes_no, groq_available
from constants import CAUSAL_CATEGORIES, SAME_EVENT_CATEGORIES, SKIP_WORDS
from database import db


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _causal_prompt(q_a, q_b):
    """
    Used when either signal is in a causal category (macro, geo, political,
    crypto, commodities).

    The Haaland/Arsenal case works here: Groq knows both are title
    contenders and that a Haaland injury affects Arsenal's odds.
    We explicitly tell it NOT to say YES just because both involve
    the same broad topic (e.g. both involve the Premier League).
    """
    return (
        "You are analysing prediction market contracts for causal relationships.\n\n"
        f'Contract A: "{q_a}"\n'
        f'Contract B: "{q_b}"\n\n'
        "Would an unexpected major move on Contract A directly and specifically "
        "affect the probability of Contract B?\n\n"
        "Say YES only if there is a clear, specific causal or correlational link — "
        "for example: same political figure, same team's title race, same economic "
        "indicator, same geopolitical event, or a well-known market spillover "
        "(e.g. oil price → Iran sanctions contract).\n"
        "Say NO if the link is only superficial (both involve finance, both involve "
        "sport, both mention the same country in passing).\n"
        "Answer only YES or NO."
    )


def _same_event_prompt(q_a, event_a, q_b, event_b):
    """
    Used when both signals are sports/esports.
    Strict: must be literally the same match/tournament/player prop.
    """
    return (
        "Are these two prediction market questions about the EXACT SAME "
        "real-world sporting event at the SAME time?\n\n"
        f'Question A: "{q_a}" (event: "{event_a}")\n'
        f'Question B: "{q_b}" (event: "{event_b}")\n\n'
        "Answer only YES or NO."
    )


def _build_prompt(sig_a, sig_b):
    cat_a = sig_a.get('category', 'other')
    cat_b = sig_b.get('category', 'other')

    both_sports = (cat_a in SAME_EVENT_CATEGORIES and
                   cat_b in SAME_EVENT_CATEGORIES)

    if both_sports:
        return _same_event_prompt(
            sig_a['question'], sig_a.get('event_title', ''),
            sig_b['question'], sig_b.get('event_title', ''),
        ), 'same_event'
    else:
        return _causal_prompt(sig_a['question'], sig_b['question']), 'causal'


# ---------------------------------------------------------------------------
# Pre-filter
# ---------------------------------------------------------------------------

def _should_ask_groq(sig_a, sig_b):
    """
    Returns (should_ask, reason_if_not).
    Cheap heuristics to avoid burning Groq calls on obvious misses.
    """
    cat_a = sig_a.get('category', 'other')
    cat_b = sig_b.get('category', 'other')

    # Already grouped by event_id in the poller — nothing to do
    if sig_a.get('event_id') == sig_b.get('event_id'):
        return False, 'same_event_id'

    # Cross-category: sports × pure-causal → no plausible link
    # (a crypto contract is not causally linked to a football match)
    one_sports = (cat_a in SAME_EVENT_CATEGORIES) != (cat_b in SAME_EVENT_CATEGORIES)
    one_causal = (cat_a in CAUSAL_CATEGORIES) != (cat_b in CAUSAL_CATEGORIES)
    if one_sports and one_causal:
        return False, 'sports_x_causal_mismatch'

    # Both sports — only worth asking Groq if same event_id (already handled above)
    # or same team appears in both questions. Different matches are never related.
    words_a = set(sig_a['question'].lower().split()) - SKIP_WORDS
    words_b = set(sig_b['question'].lower().split()) - SKIP_WORDS
    common  = words_a & words_b
    both_sports = (cat_a in SAME_EVENT_CATEGORIES and
                   cat_b in SAME_EVENT_CATEGORIES)

    if both_sports:
        # For sports pairs, require at least one meaningful shared word
        # that isn't a generic match-format word.
        # "Leverkusen vs Augsburg" / "Leeds vs Wolves" share no team names
        # so common will be empty after removing vs/will/the etc.
        # Even if they share a word like a league name, different matches
        # are never causally related so skip entirely.
        sports_noise = {'vs', 'vs.', 'fc', 'afc', 'sc', 'utd', 'united',
                        'city', 'sport', 'sports', 'club', 'total', 'over',
                        'under', 'spread', 'winner', 'match', 'game'}
        meaningful_common = common - sports_noise
        if len(meaningful_common) == 0:
            return False, 'sports_different_matches'
        # Even with shared words (e.g. same team name in both), only ask
        # Groq if it's a same-event check — different matches of the same
        # team in the same poll are still not causally linked
        if sig_a.get('event_id') != sig_b.get('event_id'):
            return False, 'sports_different_events'

    # At least one signal must be reasonably strong
    if sig_a.get('score', 0) + sig_b.get('score', 0) < 100:
        return False, 'combined_score_too_low'

    return True, None


# ---------------------------------------------------------------------------
# DB update
# ---------------------------------------------------------------------------

def _update_related_contracts(signal_id, new_contract):
    """Append a confirmed related contract to a signal's related_contracts."""
    with db() as conn:
        rows = conn.run(
            "SELECT related_contracts FROM signals WHERE id = :id",
            id=signal_id,
        )
        if not rows:
            return

        try:
            existing = json.loads(rows[0][0] or '[]')
        except Exception:
            existing = []

        if any(c.get('question') == new_contract['question'] for c in existing):
            return  # already there

        existing.append(new_contract)
        conn.run(
            "UPDATE signals SET related_contracts = :rc, "
            "related_cross_event = related_cross_event + 1 "
            "WHERE id = :id",
            rc=json.dumps(existing),
            id=signal_id,
        )


def _update_signal_dict(signal, new_contract):
    """Update the in-memory signal dict so the current poll's output is fresh."""
    try:
        existing = json.loads(signal.get('related_contracts', '[]'))
    except Exception:
        existing = []
    if not any(c.get('question') == new_contract['question'] for c in existing):
        existing.append(new_contract)
        signal['related_contracts'] = json.dumps(existing)
        signal['related_cross_event'] = signal.get('related_cross_event', 0) + 1


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_inline_grouper(signals):
    """
    Called at the end of each poll cycle with the list of signals that fired.
    Mutates signal dicts in place (adding related_contracts) and updates the DB.

    Returns the number of confirmed causal links found.
    """
    if len(signals) < 2:
        return 0

    if not groq_available():
        print("  Inline grouper: GROQ_API_KEY not set — skipping causal linking")
        return 0

    n          = len(signals)
    pairs_raw  = n * (n - 1) // 2
    pairs_sent = 0
    confirmed  = 0

    print(f"  Inline grouper: {n} signals → {pairs_raw} raw pairs")

    for i, sig_a in enumerate(signals):
        for sig_b in signals[i + 1:]:

            should_ask, reason = _should_ask_groq(sig_a, sig_b)
            if not should_ask:
                continue

            prompt, rel_type = _build_prompt(sig_a, sig_b)
            pairs_sent += 1

            is_related = groq_yes_no(prompt)

            if is_related:
                confirmed += 1
                print(
                    f"  Groq [{rel_type}] LINKED: "
                    f"{sig_a['question'][:45]!r} ↔ {sig_b['question'][:45]!r}"
                )

                contract_b = {
                    'question':    sig_b['question'],
                    'odds':        sig_b['current_odds'],
                    'prev_odds':   sig_b['prev_odds'],
                    'platform':    sig_b['platform'],
                    'event_title': sig_b.get('event_title', ''),
                    'type':        rel_type,
                }
                contract_a = {
                    'question':    sig_a['question'],
                    'odds':        sig_a['current_odds'],
                    'prev_odds':   sig_a['prev_odds'],
                    'platform':    sig_a['platform'],
                    'event_title': sig_a.get('event_title', ''),
                    'type':        rel_type,
                }

                # Update DB
                if sig_a.get('db_id'):
                    _update_related_contracts(sig_a['db_id'], contract_b)
                if sig_b.get('db_id'):
                    _update_related_contracts(sig_b['db_id'], contract_a)

                # Update in-memory dicts so this poll's feed is already correct
                _update_signal_dict(sig_a, contract_b)
                _update_signal_dict(sig_b, contract_a)

            else:
                print(
                    f"  Groq [{rel_type}] unrelated: "
                    f"{sig_a['question'][:35]!r} / {sig_b['question'][:35]!r}"
                )

            # Brief pause to stay within Groq rate limits
            time.sleep(0.3)

    print(
        f"  Inline grouper: {pairs_sent}/{pairs_raw} pairs evaluated, "
        f"{confirmed} links confirmed"
    )
    return confirmed
