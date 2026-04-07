import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

from langchain_core.tracers.base import AsyncBaseTracer
from langchain_core.tracers.schemas import Run

from .formatter import Formatter
from .relay import PublisherRelay


class BroadcastingTracer(AsyncBaseTracer):
    """Intercepts LangChain/LangGraph callbacks and broadcasts execution events.

    Owns all event emission: run lifecycle, edge traversals, and node outputs.
    A fresh instance is created per invocation so generation/linked state never
    bleeds across runs.
    """

    def __init__(
        self,
        viewport: "Viewport",
        edge_lookup: dict[tuple[str, str], str],
    ) -> None:
        """
        Args:
            viewport: The Viewport that owns this tracer. Used solely to call
                ``viewport.broadcast()`` when emitting events.
            edge_lookup: Mapping of ``(source_node, target_node)`` pairs to their
                edge IDs as defined in the static topology. Used to resolve edge IDs
                when emitting ``edge_active`` events and to identify which chain runs
                correspond to graph nodes vs. sub-calls (LLM, tool, etc.).
        """
        super().__init__(_schema_format="original+chat")
        self.viewport = viewport
        self.states: dict[str, Any] = {}

        # Static topology — used for edge inference and node identity (Phase 1).
        self.edge_lookup = edge_lookup
        self.node_names: set[str] = set()
        self.predecessors: dict[str, set[str]] = {}
        for src, tgt in edge_lookup:
            self.node_names.add(src)
            self.node_names.add(tgt)
            self.predecessors.setdefault(tgt, set()).add(src)
        self.node_names -= {"__start__", "__end__"}

        # Per-run state (reset automatically since a new tracer is created each call).
        self.generation: dict[str, int] = {"__start__": 0}
        self.linked: set[tuple[str, int, str]] = set()

        # Run tracking.
        self.root_run_id: str | None = None
        self.node_run_ids: set[str] = set()
        self.run_id: str | None = None
        self.current_node: str | None = None  # last graph node that started
        self.last_completed_node: str = "__start__"
        self.error_emitted: bool = False

    async def _persist_run(self, run: Run) -> None:
        pass

    async def _emit_edge(self, target: str) -> None:
        for source in self.predecessors.get(target, set()):
            if (src_gen := self.generation.get(source)) is None:
                continue
            if (key := (source, src_gen, target)) in self.linked:
                continue
            self.linked.add(key)
            if edge_id := self.edge_lookup.get((source, target)):
                await self.viewport.broadcast(
                    {
                        "type": "edge_active",
                        "source": source,
                        "target": target,
                        "edge_id": edge_id,
                    }
                )
        self.generation[target] = self.generation.get(target, -1) + 1

    async def _emit_node_output(self, run: Run) -> None:
        state = self.states.get(run.name)
        await self.viewport.broadcast(
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
        await self.viewport.broadcast(
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

    async def _emit_graph_error(self) -> None:
        if self.current_node is None:
            return
        edge_id = self.edge_lookup.get((self.last_completed_node, self.current_node))
        await self.viewport.broadcast(
            {
                "type": "error",
                "edge_id": edge_id,
                "source": self.last_completed_node,
                "target": self.current_node,
            }
        )

    async def _on_chain_start(self, run: Run) -> None:
        self.states[run.name] = run.inputs

        if self.root_run_id is None:
            # First chain run is the graph root.
            self.root_run_id = str(run.id)
            self.run_id = uuid.uuid4().hex[:8]
            await self.viewport.broadcast({"type": "run_start", "run_id": self.run_id})
            return

        if str(run.parent_run_id) == self.root_run_id and run.name in self.node_names:
            # Direct child of root with a known name = a graph node.
            self.node_run_ids.add(str(run.id))
            self.current_node = run.name

    async def _on_chain_end(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            await self._emit_edge("__end__")
            await self.viewport.broadcast({"type": "run_end", "run_id": self.run_id})
            return

        if str(run.id) in self.node_run_ids:
            await self._emit_node_output(run)
            await self._emit_edge(run.name)
            self.last_completed_node = run.name
        else:
            await self._emit_sub_output(run)

    async def _on_chain_error(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            # Root errored — emit graph error if a node error hasn't already done it.
            if not self.error_emitted:
                await self._emit_graph_error()
            return

        if str(run.id) in self.node_run_ids:
            await self._emit_node_output(run)
            if not self.error_emitted:
                await self._emit_graph_error()
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
        relay: PublisherRelay,
        edge_lookup: dict[tuple[str, str], str],
    ) -> None:
        self.relay = relay
        self.graph = graph
        self._edge_lookup = edge_lookup

    def __getattr__(self, name: str) -> Any:
        return getattr(self.graph, name)

    async def broadcast(self, message: dict[str, Any]) -> None:
        await self.relay.send(json.dumps(message))

    def _make_config(self, config: Any) -> dict[str, Any]:
        tracer = BroadcastingTracer(self, self._edge_lookup)
        merged: dict[str, Any] = dict(config or {})
        merged["callbacks"] = list(merged.get("callbacks") or []) + [tracer]
        return merged

    async def shutdown(self) -> None:
        await self.relay.shutdown()

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
