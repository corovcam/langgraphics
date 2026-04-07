import asyncio
import json

import pytest
import websockets

from langgraphics import watch
from langgraphics.topology import extract
from tests.lib.conftest import find_free_port, safe_ainvoke, ws_collect


async def test_linear_message_sequence(simple_graph):
    ws_port = find_free_port()
    viewport = watch(
        simple_graph, port=find_free_port(), ws_port=ws_port, open_browser=False
    )

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test"})

    assert messages[0]["type"] == "graph"

    assert messages[1]["type"] == "run_start"
    assert "run_id" in messages[1]

    edge_events = [m for m in messages if m["type"] == "edge_active"]

    assert len(edge_events) == 3

    assert edge_events[0]["source"] == "__start__"
    assert edge_events[0]["target"] == "step_a"

    assert edge_events[1]["source"] == "step_a"
    assert edge_events[1]["target"] == "step_b"

    assert edge_events[2]["source"] == "step_b"
    assert edge_events[2]["target"] == "__end__"

    assert messages[-1]["type"] == "run_end"


async def test_branching_message_sequence(branching_graph):
    ws_port = find_free_port()
    viewport = watch(
        branching_graph, port=find_free_port(), ws_port=ws_port, open_browser=False
    )

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test", "counter": 0})

    edge_events = [m for m in messages if m["type"] == "edge_active"]

    assert len(edge_events) == 4, (
        f"Expected 4 edge_active events, got {len(edge_events)}. "
        f"All messages: {[m['type'] for m in messages]}"
    )

    assert edge_events[0]["source"] == "__start__"
    assert edge_events[0]["target"] == "process"

    assert edge_events[1]["source"] == "process"
    assert edge_events[1]["target"] == "process"
    assert edge_events[2]["source"] == "process"
    assert edge_events[2]["target"] == "process"

    assert edge_events[-1]["source"] == "process"
    assert edge_events[-1]["target"] == "__end__"


async def test_error_emits_error_message(error_graph):
    ws_port = find_free_port()
    viewport = watch(
        error_graph, port=find_free_port(), ws_port=ws_port, open_browser=False
    )

    async with ws_collect(ws_port) as (messages, done):
        with pytest.raises((ValueError, TimeoutError)):
            await viewport.ainvoke({"value": "test"})

    assert messages[0]["type"] == "graph"
    assert messages[1]["type"] == "run_start"

    assert len([m for m in messages if m["type"] == "error"]) == 1

    error_msg = next(m for m in messages if m["type"] == "error")
    assert "source" in error_msg
    assert "target" in error_msg


async def test_topology_ws_matches_extract(simple_graph):
    ws_port = find_free_port()
    viewport = watch(
        simple_graph, port=find_free_port(), ws_port=ws_port, open_browser=False
    )

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test"})

    assert messages[0] == extract(simple_graph)


async def test_all_edge_ids_exist_in_topology(simple_graph):
    ws_port = find_free_port()
    viewport = watch(
        simple_graph, port=find_free_port(), ws_port=ws_port, open_browser=False
    )

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test"})

    # Edge IDs are now assigned dynamically; they appear in edge_discovered messages.
    # All edge_active events must reference an ID that was introduced via edge_discovered.
    valid_edge_ids = {e["id"] for e in messages[0]["edges"]}
    valid_edge_ids |= {m["edge_id"] for m in messages if m["type"] == "edge_discovered"}

    for event in (m for m in messages if m["type"] == "edge_active"):
        assert event["edge_id"] in valid_edge_ids, (
            f"edge_id '{event['edge_id']}' not found in topology or discovered edges {valid_edge_ids}"
        )


async def test_node_discovered_fires_before_node_output(simple_graph):
    """node_discovered is emitted for each graph node before its node_output.

    This ensures the UI can mark a node as active as soon as it starts executing,
    rather than only after it completes.
    """
    ws_port = find_free_port()
    viewport = watch(simple_graph, port=find_free_port(), ws_port=ws_port, open_browser=False)

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test"})

    discovered_ids = {m["node_id"] for m in messages if m["type"] == "node_discovered"}
    assert discovered_ids == {"step_a", "step_b"}

    for node_id in ("step_a", "step_b"):
        disc_idx = next(
            i for i, m in enumerate(messages)
            if m["type"] == "node_discovered" and m["node_id"] == node_id
        )
        out_idx = next(
            i for i, m in enumerate(messages)
            if m["type"] == "node_output" and m["node_id"] == node_id
        )
        assert disc_idx < out_idx, (
            f"node_discovered for {node_id} (index {disc_idx}) must precede "
            f"node_output (index {out_idx})"
        )


async def test_edge_active_ids_match_static_topology(branching_graph):
    """edge_active events use the same edge ID as the static topology for the same
    source->target pair.

    Regression: dynamic edge IDs were previously assigned in traversal order rather
    than definition order, causing mismatches for conditional/looping graphs.
    """
    ws_port = find_free_port()
    viewport = watch(branching_graph, port=find_free_port(), ws_port=ws_port, open_browser=False)

    async with ws_collect(ws_port) as (messages, done):
        await safe_ainvoke(viewport, {"value": "test", "counter": 0})

    topology_ids = {(e["source"], e["target"]): e["id"] for e in messages[0]["edges"]}

    for event in (m for m in messages if m["type"] == "edge_active"):
        pair = (event["source"], event["target"])
        assert pair in topology_ids, f"Unexpected edge pair {pair} not in static topology"
        assert event["edge_id"] == topology_ids[pair], (
            f"edge_active for {pair}: expected id '{topology_ids[pair]}', got '{event['edge_id']}'"
        )


async def test_late_viewer_receives_node_discovery_events(simple_graph):
    """A viewer connecting after a completed run receives node_discovered events
    from the discovery buffer, but not run lifecycle events (replay is cleared on run_end).
    """
    ws_port = find_free_port()
    viewport = watch(simple_graph, port=find_free_port(), ws_port=ws_port, open_browser=False)

    async with ws_collect(ws_port) as (_, done):
        await safe_ainvoke(viewport, {"value": "test"})

    late_messages: list[dict] = []

    async def collect() -> None:
        async with websockets.connect(f"ws://localhost:{ws_port}") as ws:
            async for raw in ws:
                late_messages.append(json.loads(raw))

    try:
        await asyncio.wait_for(collect(), timeout=2.0)
    except asyncio.TimeoutError:
        pass

    assert late_messages[0]["type"] == "graph"
    assert any(m["type"] == "node_discovered" for m in late_messages), (
        "Late viewer should receive node_discovered events from discovery buffer"
    )
    # Replay buffer is cleared on run_end, so no run lifecycle events replayed.
    assert not any(m["type"] == "run_start" for m in late_messages)
