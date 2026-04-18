"""
Shared constants used across poller, grouper, and news modules.
Single source of truth — do not duplicate these elsewhere.
"""

# Words to ignore when comparing market questions for keyword overlap.
# Covers common English stop words + year strings that cause false matches.
SKIP_WORDS = {
    'will', 'the', 'a', 'an', 'be', 'is', 'are', 'by', 'in',
    'on', 'at', 'to', 'for', 'of', 'win', 'lose', 'before',
    'after', 'during', 'most', 'least', 'first', 'last', 'next',
    'have', 'has', 'had', 'does', 'did', 'when', 'what', 'which',
    'that', 'this', 'with', 'from', 'than', 'more', 'there',
    '2024', '2025', '2026', '2027', '2028',
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
    'q1', 'q2', 'q3', 'q4',
}

# Categories where causal cross-market relationships make sense.
# e.g. Fed cuts → Bitcoin rallies, Iran ceasefire → oil drops.
CAUSAL_CATEGORIES = {'political', 'macro', 'geopolitical', 'commodities', 'crypto'}

# Categories where only strict same-event grouping is appropriate.
# e.g. two contracts about the same match, same tournament, same player.
SAME_EVENT_CATEGORIES = {'sports', 'esports', 'other'}

# All valid category names.
ALL_CATEGORIES = sorted(CAUSAL_CATEGORIES | SAME_EVENT_CATEGORIES)

# Score thresholds
SCORE_STORE_MIN  = 50   # signals below this are discarded
SCORE_FEED_MIN   = 60   # feed shows 60+ by default
SCORE_HIGH       = 70   # "high confidence" label
SCORE_EXTREME    = 80   # "extreme" label

# Sports: contracts at these odds thresholds are likely match-complete noise
# e.g. a contract at 0% or 100% after a game ends should be filtered
SPORTS_TERMINAL_ODDS_LOW  = 0.02   # ≤ 2% — essentially resolved NO
SPORTS_TERMINAL_ODDS_HIGH = 0.98   # ≥ 98% — essentially resolved YES

# Minimum minutes elapsed for a sports signal to be interesting.
# Prevents catching the normal odds drift as a match concludes.
SPORTS_MIN_SIGNAL_MINS = 2

# Geopolitical contracts tend to move slowly and stay elevated for days.
# A 5% move in 30 mins on a geopolitical contract is more meaningful than
# a 5% move on a binary sports outcome where any small score changes it.
GEOPOLITICAL_MOVE_BONUS = 5   # extra score points for geo signals
