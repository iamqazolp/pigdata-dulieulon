from __future__ import annotations

import asyncio
import json
import logging
import ssl
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Sequence

import websockets

log = logging.getLogger(__name__)

MAX_BACKOFF = 30.0
MACOS_CERT_PEM = Path("/etc/ssl/cert.pem")


def _ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if not ctx.get_ca_certs() and MACOS_CERT_PEM.is_file():
        ctx.load_verify_locations(cafile=str(MACOS_CERT_PEM))
    return ctx


async def messages(urls: Sequence[str], subscribe: Optional[dict] = None) -> AsyncIterator[Any]:
    backoff = 1.0
    attempt = 0
    ssl_ctx = _ssl_context()
    while True:
        url = urls[attempt % len(urls)]
        try:
            async with websockets.connect(
                url, ssl=ssl_ctx if url.startswith("wss://") else None, open_timeout=10, ping_interval=20, max_size=2**22
            ) as ws:
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
