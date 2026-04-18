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


def groq_yes_no(prompt, timeout=8):
    """
    Ask Groq a YES/NO question.

    Returns True (YES) or False (NO).
    Returns False on any error — callers should treat False as
    "not confirmed" rather than "definitely not".

    If GROQ_API_KEY is not set, always returns False so the system
    degrades gracefully rather than crashing.
    """
    if not GROQ_API_KEY:
        return False

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
        response.raise_for_status()
        answer = (
            response.json()['choices'][0]['message']['content']
            .strip().upper()
        )
        return answer.startswith('YES')
    except Exception as e:
        print(f"  Groq error: {e}")
        return False


def groq_available():
    return bool(GROQ_API_KEY)
