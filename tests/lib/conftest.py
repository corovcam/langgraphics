import asyncio
import json
import operator
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, TypedDict

import pytest
import websockets
from langgraph.graph import END, START, StateGraph


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class SimpleState(TypedDict):
    value: str


class CounterState(TypedDict):
    value: str
    counter: int


@pytest.fixture
def simple_graph() -> StateGraph:
    def step_a(state: SimpleState) -> dict:
        return {"value": state["value"] + "_a"}

    def step_b(state: SimpleState) -> dict:
        return {"value": state["value"] + "_b"}

    builder = StateGraph(SimpleState)
    builder.add_node("step_a", step_a)
    builder.add_node("step_b", step_b)
    builder.set_entry_point("step_a")
    builder.add_edge("step_a", "step_b")
    builder.add_edge("step_b", END)
    return builder.compile()


@pytest.fixture
def branching_graph() -> StateGraph:
    def process(state: CounterState) -> dict:
        return {"value": state["value"] + "_p", "counter": state["counter"] + 1}

    def should_continue(state: CounterState) -> str:
        if state["counter"] >= 3:
            return END
        return "process"

    builder = StateGraph(CounterState)
    builder.add_node("process", process)
    builder.set_entry_point("process")
    builder.add_conditional_edges(
        "process",
        should_continue,
        path_map={END: END, "process": "process"},
    )
    return builder.compile()


@pytest.fixture
def error_graph() -> StateGraph:
    def good_node(state: SimpleState) -> dict:
        return {"value": state["value"] + "_good"}

    def failing_node(state: SimpleState) -> dict:
        raise ValueError("intentional test error")

    builder = StateGraph(SimpleState)
    builder.add_node("good_node", good_node)
    builder.add_node("failing_node", failing_node)
    builder.set_entry_point("good_node")
    builder.add_edge("good_node", "failing_node")
    builder.add_edge("failing_node", END)
    return builder.compile()


class SubgraphState(TypedDict):
    value: str
    summary: str


@pytest.fixture
def subgraph_graph() -> StateGraph:
    """Graph where one top-level node invokes a compiled subgraph."""

    def inner_step(state: SubgraphState) -> dict:
        return {"summary": state["value"] + "_summarised"}

    inner = StateGraph(SubgraphState)
    inner.add_node("inner_step", inner_step)
    inner.add_edge(START, "inner_step")
    inner.add_edge("inner_step", END)
    compiled_inner = inner.compile()

    def outer_node(state: SimpleState) -> dict:
        result = compiled_inner.invoke({"value": state["value"], "summary": ""})
        return {"value": result["summary"]}

    def final_node(state: SimpleState) -> dict:
        return {"value": state["value"] + "_done"}

    builder = StateGraph(SimpleState)
    builder.add_node("outer_node", outer_node)
    builder.add_node("final_node", final_node)
    builder.set_entry_point("outer_node")
    builder.add_edge("outer_node", "final_node")
    builder.add_edge("final_node", END)
    return builder.compile()


class FanoutState(TypedDict):
    value: str
    results: Annotated[list[str], operator.add]


@pytest.fixture
def fanout_graph() -> StateGraph:
    """Graph with a fan-out (one → three parallel) then fan-in (three → one)."""

    def source(state: FanoutState) -> dict:
        return {"value": state["value"] + "_source"}

    def branch_a(state: FanoutState) -> dict:
        return {"results": ["a"]}

    def branch_b(state: FanoutState) -> dict:
        return {"results": ["b"]}

    def branch_c(state: FanoutState) -> dict:
        return {"results": ["c"]}

    def sink(state: FanoutState) -> dict:
        return {"value": ",".join(state["results"])}

    builder = StateGraph(FanoutState)
    builder.add_node("source", source)
    builder.add_node("branch_a", branch_a)
    builder.add_node("branch_b", branch_b)
    builder.add_node("branch_c", branch_c)
    builder.add_node("sink", sink)
    builder.add_edge(START, "source")
    builder.add_edge("source", "branch_a")
    builder.add_edge("source", "branch_b")
    builder.add_edge("source", "branch_c")
    builder.add_edge("branch_a", "sink")
    builder.add_edge("branch_b", "sink")
    builder.add_edge("branch_c", "sink")
    builder.add_edge("sink", END)
    return builder.compile()


@asynccontextmanager
async def ws_collect(
    ws_port: int, timeout: float = 15.0
) -> AsyncIterator[tuple[list[dict], asyncio.Event]]:
    messages: list[dict] = []
    done = asyncio.Event()
    connected = asyncio.Event()

    async def _collect() -> None:
        ws = None
        for _ in range(20):
            try:
                ws = await websockets.connect(f"ws://localhost:{ws_port}")
                break
            except (OSError, ConnectionRefusedError):
                await asyncio.sleep(0.1)

        if ws is None:
            raise RuntimeError(f"Could not connect to ws://localhost:{ws_port}")

        try:
            async for raw in ws:
                msg = json.loads(raw)
                messages.append(msg)
                if msg["type"] == "graph":
                    connected.set()
                if msg["type"] in ("run_end", "error"):
                    done.set()
        except websockets.ConnectionClosed:
            if not done.is_set():
                done.set()

    task = asyncio.create_task(_collect())
    try:
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        yield messages, done
        await asyncio.wait_for(done.wait(), timeout=timeout)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def safe_ainvoke(viewport: Any, input: dict, **kwargs: Any) -> Any:
    try:
        return await viewport.ainvoke(input, **kwargs)
    except TimeoutError:
        pass
