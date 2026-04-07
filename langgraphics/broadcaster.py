import asyncio
import json
from typing import Any

import websockets
from websockets.asyncio.server import Server


class Broadcaster:
    """Manages viewer WebSocket connections and relays events from publishers.

    Browser viewers connect to the root path (``/``) and receive the graph
    topology followed by a replay of the current run's events.

    Instrumentation publishers connect to ``/publish``, send the graph topology
    as their first message, and then stream execution events.  Every message
    received from a publisher is broadcast to all current viewers.
    """

    def __init__(self) -> None:
        self.connections: set[Any] = set()
        self.topology_json: str | None = None
        self.discovery_events: list[str] = []
        self.replay: list[str] = []
        self.loop: asyncio.AbstractEventLoop | None = None
        self.server: Server | None = None

    async def handler(self, websocket: Any) -> None:
        path = websocket.request.path
        if path == "/publish":
            await self._handle_publisher(websocket)
        else:
            await self._handle_viewer(websocket)

    async def _handle_viewer(self, websocket: Any) -> None:
        self.connections.add(websocket)
        try:
            if self.topology_json is not None:
                await websocket.send(self.topology_json)
            for message in self.discovery_events:
                await websocket.send(message)
            for message in self.replay:
                await websocket.send(message)
            async for _message in websocket:
                pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.connections.discard(websocket)

    async def _handle_publisher(self, websocket: Any) -> None:
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                if msg.get("type") == "graph":
                    self.topology_json = raw
                    self.replay = []
                    self.discovery_events = []
                else:
                    self.record(raw)
                await self.broadcast(raw)
        except websockets.exceptions.ConnectionClosed:
            pass

    def record(self, message: str) -> None:
        msg_type = json.loads(message).get("type")
        if msg_type in ("node_discovered", "edge_discovered"):
            self.discovery_events.append(message)
        elif msg_type == "run_start":
            self.replay = [message]
        elif msg_type in ("run_end", "error"):
            self.replay = []
        elif msg_type in ("edge_active", "node_output", "node_step"):
            self.replay.append(message)

    async def broadcast(self, message: str) -> None:
        if self.connections:
            await asyncio.gather(
                *[c.send(message) for c in self.connections],
                return_exceptions=True,
            )

    async def shutdown(self) -> None:
        loop = self.loop
        if loop is None:
            return

        async def _shutdown() -> None:
            if self.connections:
                await asyncio.gather(
                    *[c.close() for c in list(self.connections)],
                    return_exceptions=True,
                )
                self.connections.clear()
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(_shutdown(), loop))
        self.loop = None
