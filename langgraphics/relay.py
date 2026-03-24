import asyncio
import json
import threading
from typing import Any

from websockets.asyncio.client import connect as ws_connect


class PublisherRelay:
    """WebSocket client that connects to the server's /publish endpoint.

    Runs in a daemon thread and forwards graph topology and execution events
    from the instrumentation to the standalone server, which then broadcasts
    them to all connected browser viewers.
    """

    def __init__(self, topology: dict[str, Any], publish_url: str) -> None:
        self._topology_json = json.dumps(topology)
        self._publish_url = publish_url
        self.loop: asyncio.AbstractEventLoop | None = None
        self._ws: Any | None = None
        self._ready = threading.Event()
        threading.Thread(target=self._thread_main, daemon=True).start()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())

    async def _run(self) -> None:
        self.loop = asyncio.get_running_loop()
        while True:
            try:
                async with ws_connect(self._publish_url) as ws:
                    self._ws = ws
                    await ws.send(self._topology_json)
                    self._ready.set()
                    # Keep the connection alive; server never sends to publishers.
                    async for _ in ws:
                        pass
                return
            except Exception:
                self._ws = None
                await asyncio.sleep(0.05)

    async def send(self, message: str) -> None:
        loop = self.loop
        ws = self._ws
        if loop is None or ws is None:
            return
        try:
            await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(ws.send(message), loop)
            )
        except Exception:
            pass

    async def shutdown(self) -> None:
        loop = self.loop
        ws = self._ws
        if loop is None or ws is None:
            return
        try:
            await asyncio.wrap_future(
                asyncio.run_coroutine_threadsafe(ws.close(), loop)
            )
        except Exception:
            pass
        self.loop = None
        self._ws = None
