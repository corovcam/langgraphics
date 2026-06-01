import asyncio
import json
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

from langchain_core.tracers.base import AsyncBaseTracer
from langchain_core.tracers.schemas import Run
from langgraph.graph.state import CompiledStateGraph

from .formatter import Formatter, _json_default

_GRAPH_STEP_TAG = "graph:step:"


def _is_graph_node(run: Run) -> bool:
    """True for any real LangGraph node — top-level or nested subgraph.

    LangGraph sets a ``graph:step:N`` tag on every user-defined node callback,
    distinguishing it from the graph-root chain and internal wrapper chains.
    """
    return any(t.startswith(_GRAPH_STEP_TAG) for t in (run.tags or []))


class BroadcastingTracer(AsyncBaseTracer):
    """Intercepts LangChain/LangGraph callbacks and broadcasts execution events.

    Owns all event emission: run lifecycle, edge traversals, and node outputs.
    A fresh instance is created per invocation so all per-run state is isolated.
    """

    def __init__(
        self,
        broadcast_fn: Callable[[dict[str, Any]], Awaitable[None]],
        edge_seeding: dict[tuple[str, str], str] | None = None,
    ) -> None:
        """
        Args:
            broadcast_fn: Coroutine called with each event dict to send to viewers.
            edge_seeding: Optional ``(source, target) -> edge_id`` mapping from the
                static topology. Pre-seeding keeps dynamic IDs aligned with the IDs
                already present in the initial ``graph`` message so the frontend can
                match ``edge_active`` events to the correct visual edges.
        """
        super().__init__(_schema_format="original+chat")
        self._broadcast = broadcast_fn
        self.states: dict[str, Any] = {}

        # Run lifecycle.
        self.root_run_id: str | None = None
        self.run_id: str | None = None

        # Tracks real graph nodes (top-level and subgraph) by their run ID.
        self.node_run_ids: set[str] = set()
        # Maps run_id -> display node name for real nodes; used to find the logical parent.
        self.run_to_node: dict[str, str] = {}

        # Multi-instance node support (Send dispatch): tracks how many times each
        # node name has started so repeated invocations get a unique display suffix.
        # e.g. first "researcher" stays "researcher"; second becomes "researcher[2]".
        self.node_instance_count: dict[str, int] = {}
        # How many instances of each canonical node are currently executing.
        # Used to distinguish parallel (Send) from sequential (loop) reuse:
        #   > 0 when new instance starts → parallel → assign new display name
        #   == 0 when new instance starts → sequential loop → reuse canonical name
        self.active_canonical_count: dict[str, int] = {}
        # Maps run_id -> display name (e.g. "researcher[2]").
        self.run_to_display: dict[str, str] = {}
        # Maps display name -> canonical name for instances > 1.
        # e.g. "researcher[2]" -> "researcher". Used to resolve fan-in edges.
        self.alias_of: dict[str, str] = {}

        # Dynamic edge inference: last completed node per scope (parent_run_id key).
        # Used as fallback for __end__ traversal and sequential subgraph edges.
        self.last_completed: dict[str, str] = {}
        # All nodes that have completed at each scope, used to emit all fan-in edges.
        self.all_completed_at_scope: dict[str, set[str]] = {}

        # Dynamic edge ID assignment — pre-seeded from static topology when provided.
        self.discovered_edges: dict[tuple[str, str], str] = dict(edge_seeding or {})
        self.edge_counter: int = len(self.discovered_edges)

        # Intermediate subgraph invocation chains (non-graph-node chains directly
        # under a graph node). Emitted as trace entries to give InspectPanel a
        # grouping level between the parent node and its inner graph nodes.
        self.subgraph_root_run_ids: set[str] = set()

        # Error tracking.
        self.current_node: str | None = None
        self.error_emitted: bool = False

    async def _persist_run(self, run: Run) -> None:
        pass

    def _find_parent_node_id(self, run: Run) -> str | None:
        """Walk up the run tree to find the nearest ancestor that is a graph node."""
        pid = str(run.parent_run_id) if run.parent_run_id else None
        while pid:
            if pid in self.run_to_node:
                return self.run_to_node[pid]
            ancestor = self.run_map.get(pid)
            if ancestor is None:
                break
            pid = str(ancestor.parent_run_id) if ancestor.parent_run_id else None
        return None

    def _find_parent_node_run_id(self, run: Run) -> str | None:
        """Return the run_id of the nearest ancestor that is a graph node or subgraph root.

        This gives InspectPanel the correct parent for trace nesting: inner graph
        nodes nest under their subgraph root entry, which in turn nests under the
        top-level parent node.
        """
        pid = str(run.parent_run_id) if run.parent_run_id else None
        while pid:
            if pid in self.node_run_ids or pid in self.subgraph_root_run_ids:
                return pid
            ancestor = self.run_map.get(pid)
            if ancestor is None:
                break
            pid = str(ancestor.parent_run_id) if ancestor.parent_run_id else None
        return None

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
        display_name = self.run_to_display.get(str(run.id), run.name)
        state = self.states.get(str(run.id))
        await self._broadcast(
            {
                "type": "node_output",
                "node_id": display_name,
                "run_id": str(run.id),
                "parent_run_id": self._find_parent_node_run_id(run),
                "node_kind": run.run_type,
                "status": "error" if run.error else "ok",
                "input": Formatter.inputs(run),
                "output": Formatter.outputs(run),
                "metrics": Formatter.metrics(run),
                "state": json.dumps(
                    state,
                    ensure_ascii=False,
                    default=_json_default,
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
        state = self.states.get(str(run.id))
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
                    default=_json_default,
                )
                if state
                else None,
            }
        )

    async def _emit_subgraph_root_start(self, run: Run) -> None:
        """Emit a preliminary node_output for a subgraph root that is still executing.

        Sent immediately when the subgraph root chain starts so InspectPanel can
        show its inner nodes nested under it before the subgraph completes.
        Updated in _emit_subgraph_root_output when the chain finishes.
        """
        await self._broadcast(
            {
                "type": "node_output",
                "run_id": str(run.id),
                "parent_run_id": self._find_parent_node_run_id(run),
                "node_id": run.name,
                "node_kind": "chain",
                "status": "running",
                "input": Formatter.inputs(run),
                "output": None,
                "metrics": None,
                "state": None,
            }
        )

    async def _emit_subgraph_root_output(self, run: Run) -> None:
        """Emit the final node_output for a completed subgraph root chain."""
        await self._broadcast(
            {
                "type": "node_output",
                "run_id": str(run.id),
                "parent_run_id": self._find_parent_node_run_id(run),
                "node_id": run.name,
                "node_kind": "chain",
                "status": "error" if run.error else "ok",
                "input": Formatter.inputs(run),
                "output": Formatter.outputs(run),
                "metrics": Formatter.metrics(run),
                "state": None,
            }
        )

    async def _on_chain_start(self, run: Run) -> None:
        self.states[str(run.id)] = run.inputs

        if self.root_run_id is None:
            # First chain run is the graph root.
            self.root_run_id = str(run.id)
            self.run_id = uuid.uuid4().hex[:8]
            await self._broadcast({"type": "run_start", "run_id": self.run_id})
            return

        # Detect subgraph root: a non-graph-node chain whose immediate parent is a
        # real graph node or another subgraph root. These act as grouping containers
        # in the InspectPanel trace tree.
        if not _is_graph_node(run) and run.parent_run_id is not None:
            parent_id = str(run.parent_run_id)
            if parent_id in self.node_run_ids or parent_id in self.subgraph_root_run_ids:
                self.subgraph_root_run_ids.add(str(run.id))
                await self._emit_subgraph_root_start(run)
                return

        if not _is_graph_node(run):
            return

        # Determine the node's scope and parent before computing its display name.
        scope = str(run.parent_run_id) if run.parent_run_id else self.root_run_id
        parent_node_id = self._find_parent_node_id(run)

        if scope == self.root_run_id:
            # Only create a new instance name if another instance of this node is
            # currently running (parallel Send dispatch). Sequential loop re-runs
            # reuse the canonical name so the node is not duplicated.
            currently_active = self.active_canonical_count.get(run.name, 0)
            if currently_active > 0:
                # Parallel invocation — assign a new unique display name.
                count = self.node_instance_count.get(run.name, 0) + 1
                self.node_instance_count[run.name] = count
                display_name = run.name if count == 1 else f"{run.name}[{count}]"
                if count > 1:
                    self.alias_of[display_name] = run.name
            else:
                # Sequential / loop reuse — keep the canonical name.
                display_name = run.name
                # Ensure counter is in sync if this is the first-ever start.
                self.node_instance_count.setdefault(run.name, 1)
            self.active_canonical_count[run.name] = currently_active + 1
        else:
            # Inner / subgraph node: inherit the parent's instance suffix so that
            # search[2] always stays with researcher[2], regardless of execution order.
            # e.g. parent "researcher[2]" → suffix "[2]" → display "search[2]".
            parent_suffix = ""
            if parent_node_id:
                m = re.search(r'(\[\d+\])$', parent_node_id)
                if m:
                    parent_suffix = m.group(1)
            display_name = run.name + parent_suffix

        self.run_to_display[str(run.id)] = display_name
        self.node_run_ids.add(str(run.id))
        self.run_to_node[str(run.id)] = display_name
        self.current_node = display_name

        if scope == self.root_run_id:
            completed = self.all_completed_at_scope.get(scope, set())
            canonical = self.alias_of.get(display_name, display_name)

            predecessors: list[str] = []
            for (src, tgt) in self.discovered_edges:
                if tgt not in (display_name, canonical):
                    continue
                if src not in completed:
                    continue
                # Detect self-edges (loops): source's canonical == this node's canonical.
                src_canonical = self.alias_of.get(src, src)
                if src_canonical == canonical:
                    # Loop self-edge — replace canonical with the most recent instance
                    # to chain process→process[2]→process[3], not process→process[3].
                    most_recent = self.last_completed.get(scope)
                    if most_recent and most_recent != display_name and most_recent not in predecessors:
                        predecessors.append(most_recent)
                else:
                    # Regular edge — include this predecessor and all its completed aliases
                    # so that parallel Send instances all get edges to the fan-in node.
                    if src not in predecessors:
                        predecessors.append(src)
                    for alias, canon in self.alias_of.items():
                        if canon == src and alias in completed and alias not in predecessors:
                            predecessors.append(alias)

            if predecessors:
                for pred in predecessors:
                    await self._emit_edge_traversal(pred, display_name)
            else:
                predecessor = self.last_completed.get(scope) or "__start__"
                await self._emit_edge_traversal(predecessor, display_name)
        else:
            # Subgraph node: only emit an edge if there's a real predecessor;
            # the first subgraph node has no explicit __start__ edge in the view.
            predecessor = self.last_completed.get(scope)
            if predecessor:
                await self._emit_edge_traversal(predecessor, display_name)

        await self._broadcast(
            {
                "type": "node_discovered",
                "node_id": display_name,
                "node_kind": run.run_type,
                "run_id": self.run_id,
                "parent_node_id": parent_node_id,
            }
        )

    async def _on_chain_end(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            # Emit __end__ traversal for every terminal node with a known edge to __end__.
            # For loop nodes, replace the canonical name with the most recent instance.
            completed = self.all_completed_at_scope.get(self.root_run_id, set())
            raw_terminals = [src for (src, tgt) in self.discovered_edges if tgt == "__end__" and src in completed]
            terminals: list[str] = []
            for src in raw_terminals:
                src_canonical = self.alias_of.get(src, src)
                # If this source has completed aliases (loop), use the most recent one.
                aliases_done = [a for a, c in self.alias_of.items() if c == src_canonical and a in completed]
                if aliases_done:
                    most_recent = self.last_completed.get(self.root_run_id)
                    chosen = most_recent if most_recent in aliases_done else aliases_done[-1]
                    if chosen not in terminals:
                        terminals.append(chosen)
                elif src not in terminals:
                    terminals.append(src)
            if terminals:
                for term in terminals:
                    await self._emit_edge_traversal(term, "__end__")
            else:
                last = self.last_completed.get(self.root_run_id) or "__start__"
                await self._emit_edge_traversal(last, "__end__")
            await self._broadcast({"type": "run_end", "run_id": self.run_id})
            return

        if str(run.id) in self.node_run_ids:
            await self._emit_node_output(run)
            scope = str(run.parent_run_id) if run.parent_run_id else self.root_run_id
            display_name = self.run_to_display.get(str(run.id), run.name)
            self.last_completed[scope] = display_name
            self.all_completed_at_scope.setdefault(scope, set()).add(display_name)
            # Decrement the active count so sequential loop re-runs don't get new
            # display-name suffixes (they reuse the canonical name instead).
            if scope == self.root_run_id:
                canonical = self.alias_of.get(display_name, display_name)
                if canonical in self.active_canonical_count:
                    self.active_canonical_count[canonical] = max(
                        0, self.active_canonical_count[canonical] - 1
                    )
        elif str(run.id) in self.subgraph_root_run_ids:
            await self._emit_subgraph_root_output(run)
        else:
            await self._emit_sub_output(run)

    async def _on_chain_error(self, run: Run) -> None:
        if str(run.id) == self.root_run_id:
            if not self.error_emitted and self.current_node is not None:
                scope = self.root_run_id
                predecessor = self.last_completed.get(scope) or "__start__"
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
            display_name = self.run_to_display.get(str(run.id), run.name)
            scope = str(run.parent_run_id) if run.parent_run_id else self.root_run_id
            if scope == self.root_run_id:
                canonical = self.alias_of.get(display_name, display_name)
                if canonical in self.active_canonical_count:
                    self.active_canonical_count[canonical] = max(
                        0, self.active_canonical_count[canonical] - 1
                    )
            if not self.error_emitted:
                predecessor = self.last_completed.get(scope) or "__start__"
                edge_id = self.discovered_edges.get((predecessor, display_name))
                await self._broadcast(
                    {
                        "type": "error",
                        "edge_id": edge_id,
                        "source": predecessor,
                        "target": display_name,
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


# class Viewport(CompiledStateGraph[StateT, ContextT, InputT, OutputT]):
class Viewport:
    def __init__(
        self,
        graph: Any,
        broadcast_fn: Callable[[dict[str, Any]], Awaitable[None]],
        shutdown_fn: Callable[[], Awaitable[None]],
        edge_seeding: dict[tuple[str, str], str] | None = None,
        **kwargs: Any,
    ) -> None:
        # super().__init__(**kwargs)
        self.graph = graph
        self._broadcast_fn = broadcast_fn
        self._shutdown_fn = shutdown_fn
        self._edge_seeding = edge_seeding

    def __getattr__(self, name: str) -> Any:
        return getattr(self.graph, name)

    def _make_config(self, config: Any) -> dict[str, Any]:
        tracer = build_langgraphics_tracer(
            self._broadcast_fn,
            self._edge_seeding,
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

def build_langgraphics_tracer(
    broadcast_fn: Callable[[dict[str, Any]], Awaitable[None]],
    edge_seeding: dict[tuple[str, str], str] | None = None,
) -> BroadcastingTracer:
    return BroadcastingTracer(broadcast_fn, edge_seeding)
