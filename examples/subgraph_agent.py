"""Subgraph example — demonstrates nested node rendering in the UI.

The outer graph has three top-level nodes:
  __start__ → preprocess → summarise_runner → postprocess → __end__

`summarise_runner` delegates to an inner subgraph:
  __start__ → clean → summarise → __end__

Run:
    uv run examples/subgraph_agent.py

Then open http://localhost:8764 in your browser.
The `summarise_runner` node should expand to show `clean` and `summarise` as children.
"""

import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from langgraphics import watch


# --- Inner subgraph -----------------------------------------------------------

class InnerState(TypedDict):
    text: str


def clean(state: InnerState) -> InnerState:
    time.sleep(3)
    return {"text": state["text"].strip().lower()}


def summarise(state: InnerState) -> InnerState:
    time.sleep(3)
    words = state["text"].split()
    return {"text": " ".join(words[:5]) + ("..." if len(words) > 5 else "")}


inner_builder = StateGraph(InnerState)
inner_builder.add_edge(START, "clean")
inner_builder.add_node("clean", clean)
inner_builder.add_node("summarise", summarise)

inner_builder.add_edge("clean", "summarise")
inner_builder.add_edge("summarise", END)

inner_graph = inner_builder.compile()


# --- Outer graph --------------------------------------------------------------

class OuterState(TypedDict):
    text: str


def preprocess(state: OuterState) -> OuterState:
    time.sleep(5)
    return {"text": f"  {state['text']}  "}   # add whitespace for clean() to strip


def summarise_runner(state: OuterState) -> OuterState:
    result = inner_graph.invoke({"text": state["text"]})
    return {"text": result["text"]}


def postprocess(state: OuterState) -> OuterState:
    time.sleep(5)
    return {"text": state["text"].capitalize()}


outer_builder = StateGraph(OuterState)
outer_builder.add_node("preprocess", preprocess)
outer_builder.add_node("summarise_runner", summarise_runner)
outer_builder.add_node("summarizer", inner_graph)
outer_builder.add_node("postprocess", postprocess)
outer_builder.add_edge(START, "preprocess")
outer_builder.add_edge("preprocess", "summarise_runner")
outer_builder.add_edge("summarizer", "summarise_runner")
outer_builder.add_edge("summarise_runner", "postprocess")
outer_builder.add_edge("postprocess", END)

graph = outer_builder.compile()
graph = watch(graph)

result = graph.invoke({"text": "The quick brown fox jumps over the lazy dog near the river"})
print("Result:", result["text"])
# wait = 10
# print(f"Open http://localhost:8764 to inspect — server stays up for {wait} seconds.")
# time.sleep(wait)
