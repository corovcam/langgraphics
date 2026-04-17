# LangGraphics – Copilot Instructions

LangGraphics is a live browser-based visualization tool for LangGraph agent execution. A single `watch(graph)` call wraps any compiled LangGraph and streams real-time graph topology and execution events to a React frontend over WebSockets.

## Commands

### Python backend

```bash
uv sync                          # install all deps including dev
pytest                           # run all tests
pytest tests/lib/test_watching.py::test_linear_message_sequence  # single test
pytest -v -s -k "linear"        # filter tests by name
tox                              # full matrix (Python 3.10–3.14 × LangGraph 1.0.0/1.0.5/1.0.8)
tox -e py312-lg108               # specific environment
tox -e py311-lg105 -- tests/lib/test_watching.py  # specific file in specific env
ruff check langgraphics/         # lint
ruff check --fix                 # auto-fix
ruff format langgraphics/        # format
```

### Examples

```bash
uv run examples/basic_agent.py         # sequential workflow
uv run examples/react_agent.py         # ReAct + tools
uv run examples/deep_agent.py          # nested subgraphs
uv run examples/fanout_agent.py        # parallel execution
uv run examples/error_agent.py         # error handling
uv run examples/server_basic_agent.py  # standalone server mode (see below)
```

In **standalone mode** the server is started separately from the agent process. Start the server once, then connect multiple agents to it:

```bash
# Terminal 1 — start the server (stays running)
uv run langgraphics-server             # default: HTTP 8764, WS 8765
uv run langgraphics-server --port 9000 --ws-port 9001  # custom ports

# Terminal 2 — run the agent, connecting to the already-running server
uv run examples/server_basic_agent.py
```

In code, use `connect_server()` instead of letting `watch()` start its own server:

```python
from langgraphics import watch, connect_server

server = connect_server()          # points to default host/ports
graph = watch(my_graph, server=server)
graph.invoke(...)
```

### Web frontend (run from `langgraphics-web/`)

```bash
npm test          # run tests
npm run build     # TypeScript compile + Vite build
npm run dev       # dev server at localhost:5173
```

## Architecture

The backend has seven focused modules:

| Module | Responsibility |
|---|---|
| `watch.py` | Public entry point; builds `Viewport` and starts server |
| `streamer.py` | `Viewport` wraps a compiled graph; `BroadcastingTracer` intercepts LangChain callbacks |
| `server.py` | `Server` manages HTTP (port 8764) + WebSocket (port 8765) servers in daemon threads |
| `broadcaster.py` | `Broadcaster` routes WebSocket connections: `/publish` for writers, `/` for viewers; stores replay buffer |
| `relay.py` | `PublisherRelay` daemon thread forwards Viewport events to `/publish` |
| `formatter.py` | Transforms `langchain_core.Run` objects into UI-friendly JSON; reads pricing from `metadata/models.json` |
| `topology.py` | Extracts nodes/edges from a compiled graph into the wire format |

**Execution flow:**
```
watch(graph) → extract topology → start Server → start PublisherRelay thread → return Viewport

viewport.ainvoke() → emit run_start → inject BroadcastingTracer → graph.astream(updates)
  → _emit_edge() per transition → BroadcastingTracer emits node_output → emit run_end
  → PublisherRelay → Broadcaster → all connected viewers
```

The frontend (`langgraphics-web/src/`) uses `@xyflow/react` for rendering and `@dagrejs/dagre` for layout. Key pieces: `useGraphState` hook owns all state, `useWebSocket` handles the connection, `GraphCanvas` renders the graph, `InspectPanel` shows node details.

## WebSocket message protocol

All messages are JSON. The topology message is sent once on connect; everything else is streamed during execution.

```jsonc
// Topology (sent on connect + replayed to late joiners)
{"type": "graph", "nodes": [...], "edges": [...]}

// Lifecycle
{"type": "run_start", "run_id": "abc123de"}
{"type": "run_end",   "run_id": "abc123de"}

// Node transition
{"type": "edge_active", "source": "nodeA", "target": "nodeB", "edge_id": "e0"}

// Node completed
{
  "type": "node_output",
  "node_id": "process",
  "run_id": "abc123de",
  "node_kind": "chain",   // "chain" | "llm" | "tool" | "retriever"
  "status": "ok",
  "input": "...",         // JSON string
  "output": "...",        // JSON string
  "metrics": {"latency": "250ms", "costs": {...}, "tokens": {...}},
  "state": "{...}"        // JSON string of full graph state
}

// Error
{"type": "error", "edge_id": "e3", "source": "nodeA", "target": "nodeB"}
```

Types for all messages are defined in `langgraphics-web/src/types.ts`.

## Key conventions

**Async/threading:** The backend is async-first; `asyncio.wrap_future()` bridges the main event loop to daemon threads. `PublisherRelay` and both server threads are daemon threads—they die with the main process.

**Loop detection:** `Viewport.generation` (a dict) tracks how many times each node has executed. Self-edges (node → same node) increment the generation counter to differentiate repeated activations.

**Replay buffer:** `Broadcaster` clears its event buffer on `run_start` and records all subsequent events. New viewers receive the stored topology first, then the buffered events—no explicit replay request needed.

**Test isolation:** Each test spins up a fresh server on a random port (`find_free_port()`). Use `ws_collect()` context manager to capture WebSocket messages and `safe_ainvoke()` to run the graph. `pytest-asyncio` is configured with `asyncio_mode = "auto"` so async test functions work without decorators.

**Special graph nodes:** `__start__` and `__end__` are the built-in LangGraph sentinel nodes; topology extraction converts them to `type: "start"` / `type: "end"`.

**Run IDs:** 8-character hex prefix of `uuid.uuid4().hex`.

**No persistence:** Everything is in-memory. Closing a viewer loses history.
