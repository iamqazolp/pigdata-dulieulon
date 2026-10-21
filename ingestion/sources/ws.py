from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Optional, Sequence

import websockets

log = logging.getLogger(__name__)

MAX_BACKOFF = 30.0


async def messages(urls: Sequence[str], subscribe: Optional[dict] = None) -> AsyncIterator[Any]:
    backoff = 1.0
    attempt = 0
    while True:
        url = urls[attempt % len(urls)]
        try:
            async with websockets.connect(url, open_timeout=10, ping_interval=20, max_size=2**22) as ws:
                log.info("connected to %s", url)
                if subscribe is not None:
                    await ws.send(json.dumps(subscribe))
                backoff = 1.0
                async for raw in ws:
                    yield json.loads(raw)
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            log.warning("connection to %s failed: %r; retrying in %.0fs", url, exc, backoff)
            attempt += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
