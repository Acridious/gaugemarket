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


def groq_yes_no(prompt, timeout=8, retries=3):
    """
    Ask Groq a YES/NO question.

    Returns True (YES) or False (NO).
    Retries with exponential backoff on 429 rate limit errors.
    Returns False on persistent failure — callers treat False as
    "not confirmed" rather than "definitely not".

    If GROQ_API_KEY is not set, always returns False so the system
    degrades gracefully rather than crashing.
    """
    if not GROQ_API_KEY:
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


def groq_complete(prompt, max_tokens=150, temperature=0.3, timeout=10, retries=3):
    """
    Ask Groq for a text completion (used for AI summaries).
    Retries with exponential backoff on 429.
    Returns the response string or None on failure.
    """
    if not GROQ_API_KEY:
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
