import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

from langchain_core.tracers.base import AsyncBaseTracer
from langchain_core.tracers.schemas import Run

from .formatter import Formatter


class BroadcastingTracer(AsyncBaseTracer):
    """Intercepts LangChain/LangGraph callbacks and broadcasts execution events.

    Owns all event emission: run lifecycle, edge traversals, and node outputs.
    A fresh instance is created per invocation so all per-run state is isolated.
    """

    def __init__(
        self,
        broadcast_fn: Callable[[dict[str, Any]], Awaitable[None]],
        node_names: set[str],
        edge_seeding: dict[tuple[str, str], str] | None = None,
    ) -> None:
        """
        Args:
            broadcast_fn: Coroutine called with each event dict to send to viewers.
            node_names: Set of graph node names from static topology. Used to
                identify which chain runs are graph nodes vs. internal LangGraph
                operations or sub-calls (LLM, tool, etc.).
            edge_seeding: Optional ``(source, target) -> edge_id`` mapping from the
                static topology. Pre-seeding keeps dynamic IDs aligned with the IDs
                already present in the initial ``graph`` message so the frontend can
                match ``edge_active`` events to the correct visual edges.
        """
        super().__init__(_schema_format="original+chat")
        self._broadcast = broadcast_fn
        self.node_names = node_names
        self.states: dict[str, Any] = {}

        # Run lifecycle.
        self.root_run_id: str | None = None
        self.node_run_ids: set[str] = set()
        self.run_id: str | None = None

        # Dynamic edge inference: tracks the last completed node per parent scope.
        self.last_completed: dict[str, str] = {}  # parent_run_id -> last node name

        # Dynamic edge ID assignment — pre-seeded from static topology when provided.
        self.discovered_edges: dict[tuple[str, str], str] = dict(edge_seeding or {})
        self.edge_counter: int = len(self.discovered_edges)

        # Error tracking.
        self.current_node: str | None = None
        self.error_emitted: bool = False

    async def _persist_run(self, run: Run) -> None:
        pass

    def _get_or_create_edge(self, source: str, target: str) -> tuple[str, bool]:
        """Return (edge_id, is_new). Assigns a new ID the first time an edge is seen."""
        key = (source, target)
        if key not in self.discovered_edges:
            edge_id = f"e{self.edge_counter}"
            self.edge_counter += 1
            self.discovered_edges[key] = edge_id
            return edge_id, True
        return self.discovered_edges[key], False

    async def _emit_edge_traversal(self, source: str, target: str) -> None:
        """Emit edge_discovered (first time only) then edge_active for each traversal."""
        edge_id, is_new = self._get_or_create_edge(source, target)
        if is_new:
            await self._broadcast(
                {
                    "type": "edge_discovered",
                    "edge_id": edge_id,
                    "source": source,
                    "target": target,
                }
            )
        await self._broadcast(
            {
                "type": "edge_active",
                "source": source,
                "target": target,
                "edge_id": edge_id,
            }
        )

    async def _emit_node_output(self, run: Run) -> None:
        state = self.states.get(run.name)
        await self._broadcast(
            {
                "type": "node_output",
                "node_id": run.name,
                "run_id": str(run.id),
                "node_kind": run.run_type,
                "status": "error" if run.error else "ok",
                "input": Formatter.inputs(run),
                "output": Formatter.outputs(run),
                "metrics": Formatter.metrics(run),
                "state": json.dumps(
                    state,
                    ensure_ascii=False,
                    default=lambda x: x.__dict__,
                )
                if state
                else None,
            }
        )

    async def _emit_sub_output(self, run: Run) -> None:
        """Emit output for LLM, tool, retriever, or other sub-calls within a node."""
        node_run_id = str(run.parent_run_id) if run.parent_run_id else None
        if node_run_id is None:
            return
        state = self.states.get(run.name)
        await self._broadcast(
            {
                "type": "node_output",
                "run_id": str(run.id),
                "parent_run_id": node_run_id,
                "node_id": run.name,
                "node_kind": run.run_type,
                "status": "error" if run.error else "ok",
                "input": Formatter.inputs(run),
                "output": Formatter.outputs(run),
                "metrics": Formatter.metrics(run),
                "state": json.dumps(
                    state,
                    ensure_ascii=False,
                    default=lambda x: x.__dict__,
                )
                if state
                else None,
            }
        )

    async def _on_chain_start(self, run: Run) -> None:
        self.states[run.name] = run.inputs

        if self.root_run_id is None:
            # First chain run is the graph root.
            self.root_run_id = str(run.id)
            self.run_id = uuid.uuid4().hex[:8]
            await self._broadcast({"type": "run_start", "run_id": self.run_id})
            return

        if str(run.parent_run_id) == self.root_run_id and run.name in self.node_names:
            # Direct child of root with a known name = a graph node.
            self.node_run_ids.add(str(run.id))
            self.current_node = run.name

            # Infer predecessor from the last completed node in this scope.
            predecessor = self.last_completed.get(self.root_run_id) or "__start__"
            await self._emit_edge_traversal(predecessor, run.name)
            await self._broadcast(
                {
                    "type": "node_discovered",
                    "node_id": run.name,
                    "node_kind": run.run_type,
                    "run_id": self.run_id,
                    "parent_node_id": None,
                }
            )

    async def _on_chain_end(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            last = self.last_completed.get(self.root_run_id) or "__start__"
            await self._emit_edge_traversal(last, "__end__")
            await self._broadcast({"type": "run_end", "run_id": self.run_id})
            return

        if str(run.id) in self.node_run_ids:
            await self._emit_node_output(run)
            self.last_completed[self.root_run_id] = run.name
        else:
            await self._emit_sub_output(run)

    async def _on_chain_error(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            if not self.error_emitted and self.current_node is not None:
                predecessor = self.last_completed.get(self.root_run_id) or "__start__"
                edge_id = self.discovered_edges.get((predecessor, self.current_node))
                await self._broadcast(
                    {
                        "type": "error",
                        "edge_id": edge_id,
                        "source": predecessor,
                        "target": self.current_node,
                    }
                )
            return

        if str(run.id) in self.node_run_ids:
            await self._emit_node_output(run)
            if not self.error_emitted:
                predecessor = self.last_completed.get(self.root_run_id) or "__start__"
                edge_id = self.discovered_edges.get((predecessor, run.name))
                await self._broadcast(
                    {
                        "type": "error",
                        "edge_id": edge_id,
                        "source": predecessor,
                        "target": run.name,
                    }
                )
                self.error_emitted = True
        else:
            await self._emit_sub_output(run)

    async def _on_llm_end(self, run: Run) -> None:
        await self._emit_sub_output(run)

    async def _on_llm_error(self, run: Run) -> None:
        await self._emit_sub_output(run)

    async def _on_tool_end(self, run: Run) -> None:
        await self._emit_sub_output(run)

    async def _on_tool_error(self, run: Run) -> None:
        await self._emit_sub_output(run)

    async def _on_retriever_end(self, run: Run) -> None:
        await self._emit_sub_output(run)

    async def _on_retriever_error(self, run: Run) -> None:
        await self._emit_sub_output(run)


class Viewport:
    def __init__(
        self,
        graph: Any,
        broadcast_fn: Callable[[dict[str, Any]], Awaitable[None]],
        shutdown_fn: Callable[[], Awaitable[None]],
        node_names: set[str],
        edge_seeding: dict[tuple[str, str], str] | None = None,
    ) -> None:
        self.graph = graph
        self._broadcast_fn = broadcast_fn
        self._shutdown_fn = shutdown_fn
        self._node_names = node_names
        self._edge_seeding = edge_seeding

    def __getattr__(self, name: str) -> Any:
        return getattr(self.graph, name)

    def _make_config(self, config: Any) -> dict[str, Any]:
        tracer = BroadcastingTracer(
            self._broadcast_fn, self._node_names, self._edge_seeding
        )
        merged: dict[str, Any] = dict(config or {})
        merged["callbacks"] = list(merged.get("callbacks") or []) + [tracer]
        return merged

    async def shutdown(self) -> None:
        await self._shutdown_fn()

    async def ainvoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        return await self.graph.ainvoke(
            input, config=self._make_config(config), **kwargs
        )

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        return asyncio.run(self.ainvoke(input, config=config, **kwargs))

    async def astream(
        self, input: Any, config: Any = None, **kwargs: Any
    ) -> AsyncIterator:
        async for chunk in self.graph.astream(
            input, config=self._make_config(config), **kwargs
        ):
            yield chunk

    def stream(self, input: Any, config: Any = None, **kwargs: Any) -> Iterator:
        loop = asyncio.new_event_loop()
        ait = self.astream(input, config=config, **kwargs).__aiter__()
        try:
            while True:
                try:
                    yield loop.run_until_complete(ait.__anext__())
                except StopAsyncIteration:
                    break
        finally:
            loop.close()
