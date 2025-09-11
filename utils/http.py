from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, Optional, Tuple

import requests


RETRY_STATUSES = {429, 500, 502, 503, 504}


async def get_json(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15.0,
    max_tries: int = 5,
    backoff_base: float = 0.8,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_tries + 1):
        try:
            resp = await asyncio.to_thread(requests.get, url, params=params, headers=headers, timeout=timeout)
            status = resp.status_code
            if status in RETRY_STATUSES:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
                else:
                    delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
                if attempt < max_tries:
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < max_tries:
                delay = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
                await asyncio.sleep(delay)
                continue
            break
    assert last_err is not None
    raise last_err


async def post_json(
    url: str,
    *,
    json_body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 15.0,
    max_tries: int = 5,
    backoff_base: float = 0.8,
) -> Tuple[int, str]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_tries + 1):
        try:
            resp = await asyncio.to_thread(requests.post, url, json=json_body, headers=headers, timeout=timeout)
            status = resp.status_code
            if status in RETRY_STATUSES and attempt < max_tries:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = backoff_base * (2 ** (attempt - 1))
                else:
                    delay = backoff_base * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
                continue
            return status, resp.text
        except Exception as e:
            last_err = e
            if attempt < max_tries:
                delay = backoff_base * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
                continue
            break
    assert last_err is not None
    raise last_err
