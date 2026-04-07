"""Server implementation"""

import asyncio
import threading
import time
from functools import partial
from http.server import SimpleHTTPRequestHandler
from pathlib import Path
from socketserver import TCPServer

from websockets.asyncio.server import serve

from .broadcaster import Broadcaster


def _start_http_server(host: str, port: int) -> TCPServer:
    static = Path(__file__).parent / "static"
    handler = partial(SimpleHTTPRequestHandler, directory=static)

    class _Server(TCPServer):
        allow_reuse_address = True

    server = _Server((host, port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _start_ws_server(manager: Broadcaster, host: str, port: int) -> None:
    async def run() -> None:
        manager.loop = asyncio.get_running_loop()
        manager.server = await serve(manager.handler, host, port)
        await manager.server.wait_closed()

    def thread_target() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    threading.Thread(target=thread_target, daemon=True).start()


class Server:
    """Standalone visualization server.

    Serves the frontend over HTTP and accepts two kinds of WebSocket connections:
    - Browser viewers connect to ``ws://<host>:<ws_port>/`` and receive the live graph.
    - Instrumentation publishers connect to ``ws://<host>:<ws_port>/publish`` and push
      graph topology and execution events that are broadcast to all viewers.
    """

    def __init__(
        self,
        broadcaster: Broadcaster,
        http_server: TCPServer,
        host: str,
        port: int,
        ws_port: int,
    ) -> None:
        self.broadcaster = broadcaster
        self._http_server = http_server
        self.host = host
        self.port = port
        self.ws_port = ws_port

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def ws_url(self) -> str:
        return f"ws://{self.host}:{self.ws_port}"

    @property
    def publish_url(self) -> str:
        return f"ws://{self.host}:{self.ws_port}/publish"

    async def shutdown(self) -> None:
        if self.broadcaster is not None:
            await self.broadcaster.shutdown()
        if self._http_server is not None:
            self._http_server.shutdown()


def connect_server(
    host: str = "localhost",
    port: int = 8764,
    ws_port: int = 8765,
) -> Server:
    """Return a handle to an already-running server without starting anything.

    Use this when the server was started independently (e.g. via the
    ``langgraphics-server`` CLI) and you just need to point ``watch()`` at it::

        server = connect_server()
        graph = watch(my_graph, server=server)
    """
    return Server(None, None, host, port, ws_port)


def start_server(
    host: str = "localhost",
    port: int = 8764,
    ws_port: int = 8765,
) -> Server:
    """Start the visualization server and return immediately.

    The HTTP server serves the bundled frontend and the WebSocket server
    handles both viewer and publisher connections.  Both run in daemon threads
    so they are automatically cleaned up when the main process exits.
    """
    broadcaster = Broadcaster()
    http_server = _start_http_server(host, port)
    _start_ws_server(broadcaster, host, ws_port)
    return Server(broadcaster, http_server, host, port, ws_port)


def main() -> None:
    """CLI entry point: ``langgraphics-server``."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="langgraphics-server",
        description="Start the LangGraphics visualization server.",
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8764, help="HTTP port")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket port")
    args = parser.parse_args()

    server = start_server(host=args.host, port=args.port, ws_port=args.ws_port)
    print("LangGraphics server running")
    print(f"  Frontend : {server.url}")
    print(f"  WebSocket: {server.ws_url}")
    print(f"  Publish  : {server.publish_url}")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
