"""Send example — dynamic parallel sub-agent dispatch.

A router node reads the input and dynamically decides how many researcher
sub-agents to spawn (1-3) using LangGraph's Send construct. Each researcher
runs its own inner subgraph (search → analyse). Results are merged by a
synthesiser node.

Outer graph:
  __start__ → plan → [Send → researcher x N] → synthesise → __end__

Each researcher subgraph:
  __start__ → search → analyse → __end__

Run:
    uv run examples/send_agent.py

Open http://localhost:8764 (or ?debug=1) while it runs.
"""

import asyncio
import operator
import random
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from langgraphics import watch


# ---------------------------------------------------------------------------
# Shared state types
# ---------------------------------------------------------------------------

class ResearcherState(TypedDict):
    topic: str
    findings: str


class MainState(TypedDict):
    query: str
    topics: list[str]
    findings: Annotated[list[str], operator.add]
    answer: str


# ---------------------------------------------------------------------------
# Researcher subgraph  (search → analyse)
# ---------------------------------------------------------------------------

async def search(state: ResearcherState) -> ResearcherState:
    await asyncio.sleep(random.uniform(2.0, 3.5))
    return {"findings": f"[search:{state['topic']}] raw data found"}


async def analyse(state: ResearcherState) -> ResearcherState:
    await asyncio.sleep(random.uniform(2.5, 4.0))
    return {"findings": f"[analysis:{state['topic']}] insight extracted"}


researcher_builder = StateGraph(ResearcherState)
researcher_builder.add_node("search", search)
researcher_builder.add_node("analyse", analyse)
researcher_builder.add_edge(START, "search")
researcher_builder.add_edge("search", "analyse")
researcher_builder.add_edge("analyse", END)
researcher_graph = researcher_builder.compile()


# ---------------------------------------------------------------------------
# Outer graph nodes
# ---------------------------------------------------------------------------

async def plan(state: MainState) -> MainState:
    """Decide which topics to research (1–3, chosen dynamically)."""
    await asyncio.sleep(3.5)
    all_topics = ["climate", "economy", "technology", "health", "politics"]
    n = random.randint(3, 5)
    topics = random.sample(all_topics, n)
    print(f"  → dispatching {n} researcher(s): {topics}")
    return {"topics": topics}


def route_researchers(state: MainState) -> list[Send]:
    """Fan-out: one Send per topic, each routed to the 'researcher' node."""
    return [Send("researcher", {"topic": t, "findings": ""}) for t in state["topics"]]


async def researcher(state: ResearcherState) -> dict:
    """Outer-graph node that runs the full researcher subgraph for one topic."""
    result = await researcher_graph.ainvoke(state)
    # Return to the reducer — findings accumulate across all researchers.
    return {"findings": [result["findings"]]}


async def synthesise(state: MainState) -> MainState:
    """Merge all researchers' findings into a final answer."""
    await asyncio.sleep(2.0)
    answer = "Summary: " + " | ".join(state["findings"])
    return {"answer": answer}


# ---------------------------------------------------------------------------
# Build the outer graph
# ---------------------------------------------------------------------------

builder = StateGraph(MainState)
builder.add_node("plan", plan)
builder.add_node("researcher", researcher)
builder.add_node("synthesise", synthesise)

builder.add_edge(START, "plan")
builder.add_conditional_edges("plan", route_researchers, ["researcher"])
builder.add_edge("researcher", "synthesise")
builder.add_edge("synthesise", END)

graph = builder.compile()
graph = watch(graph)


async def main() -> None:
    result = await graph.ainvoke({
        "query": "What are the latest global trends?",
        "topics": [],
        "findings": [],
        "answer": "",
    })
    print("Answer:", result["answer"])
    # delay = 30
    # print(f"Open http://localhost:8764 — server stays up for {delay} seconds.")
    # await asyncio.sleep(delay)


asyncio.run(main())
