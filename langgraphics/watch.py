import webbrowser
from typing import Any, Literal

from .relay import PublisherRelay
from .server import Server, start_server
from .streamer import Viewport
from .topology import extract


def watch(
    graph: Any,
    *,
    server: Server | None = None,
    host: str = "localhost",
    port: int = 8764,
    ws_port: int = 8765,
    open_browser: bool = True,
    direction: Literal["TB", "LR"] = "TB",
    mode: Literal["auto", "manual"] = "auto",
    inspect: Literal["off", "tree", "full"] = "off",
    theme: Literal["system", "dark", "light"] = "system",
) -> Viewport:
    """Wrap *graph* for live visualization.

    When *server* is ``None`` (default) a new :class:`~langgraphics.server.Server`
    is started automatically on *host*/*port*/*ws_port*.  Pass an existing
    :class:`~langgraphics.server.Server` to reuse an already-running standalone
    server instead.
    """
    topology = extract(graph)
    edge_lookup = {(e["source"], e["target"]): e["id"] for e in topology["edges"]}

    if server is None:
        server = start_server(host=host, port=port, ws_port=ws_port)
        
    if open_browser:
        defaults = (
            ("mode", mode, "auto"),
            ("theme", theme, "system"),
            ("inspect", inspect, "off"),
            ("direction", direction, "TB"),
        )
        params = [f"{k}={v}" for k, v, default in defaults if v != default]
        query = ("?" + "&".join(params)) if params else ""
        webbrowser.open(f"{server.url}{query}")

    relay = PublisherRelay(topology, server.publish_url)
    relay._ready.wait(5.0)

    return Viewport(graph, relay, edge_lookup)
