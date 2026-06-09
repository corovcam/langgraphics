import asyncio
from langchain_core.tracers.base import AsyncBaseTracer
from langgraph.graph.state import CompiledStateGraph
from contextlib import asynccontextmanager
import json
import webbrowser
from typing import Any, Literal, AsyncGenerator
import logging

from .relay import PublisherRelay
from .server import Server, start_server
from .streamer import Viewport
from .topology import extract
from .streamer import build_langgraphics_tracer


logger = logging.getLogger(__name__)


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
    edge_seeding = {(e["source"], e["target"]): e["id"] for e in topology["edges"]}

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

    async def broadcast_fn(message: dict[str, Any]) -> None:
        await relay.send(json.dumps(message))

    async def shutdown_fn() -> None:
        await relay.shutdown()

    return Viewport(graph, broadcast_fn, shutdown_fn, edge_seeding)


def create_langgraphics_watcher(
    builder_or_graph: Any,
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
) -> AsyncBaseTracer | None:
    relay: PublisherRelay | None = None
    
    async def broadcast_fn(message: dict[str, Any]) -> None:
        await relay.send(json.dumps(message))
    
    try:
        if isinstance(builder_or_graph, CompiledStateGraph):
            graph = builder_or_graph
        else:
            graph = builder_or_graph.compile(name="LangGraphics Watcher")
            
        topology = extract(graph)
        edge_seeding = {(e["source"], e["target"]): e["id"] for e in topology["edges"]}
        
        if server is None:
            logger.debug("Starting LangGraphics server...")
            server = start_server(host=host, port=port, ws_port=ws_port)
        
        relay = PublisherRelay(topology, server.publish_url)
        relay._ready.wait(5.0)

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

        tracer = build_langgraphics_tracer(
            broadcast_fn,
            edge_seeding,
        )
        
        return tracer
    except Exception as e:
        logger.exception("Error in LangGraphics Watcher:")
        return None
