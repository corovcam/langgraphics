"""Dual-subgraph example — one top-level node invokes two inner subgraphs in parallel.

Outer graph:
  __start__ → preprocess → dual_processor → postprocess → __end__

dual_processor fans out to both inner subgraphs concurrently via asyncio.gather:

  Subgraph A (summarise pipeline):
    __start__ → clean → summarise → __end__

  Subgraph B (translate pipeline):
    __start__ → expand → translate → __end__

All four inner nodes should appear as children of dual_processor in the UI,
with clean/expand running at the same time and summarise/translate running at
the same time.

Run:
    uv run examples/dual_subgraph_agent.py

Then open http://localhost:8764 (or http://localhost:8764?debug=1) in your browser.
"""

import asyncio
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from langgraphics import watch


# --- Subgraph A: summarise pipeline ------------------------------------------

class SummariseState(TypedDict):
    text: str


async def clean(state: SummariseState) -> SummariseState:
    await asyncio.sleep(1.5)
    return {"text": state["text"].strip().lower()}


async def summarise(state: SummariseState) -> SummariseState:
    await asyncio.sleep(3.0)
    words = state["text"].split()
    return {"text": " ".join(words[:5]) + ("..." if len(words) > 5 else "")}


summarise_builder = StateGraph(SummariseState)
summarise_builder.add_node("clean", clean)
summarise_builder.add_node("summarise", summarise)
summarise_builder.add_edge(START, "clean")
summarise_builder.add_edge("clean", "summarise")
summarise_builder.add_edge("summarise", END)
summarise_graph = summarise_builder.compile()


# --- Subgraph B: translate pipeline ------------------------------------------

class TranslateState(TypedDict):
    text: str


async def expand(state: TranslateState) -> TranslateState:
    await asyncio.sleep(2.0)
    return {"text": state["text"] + " [expanded]"}


async def translate(state: TranslateState) -> TranslateState:
    await asyncio.sleep(2.0)
    return {"text": f"[FR] {state['text']}"}


translate_builder = StateGraph(TranslateState)
translate_builder.add_node("expand", expand)
translate_builder.add_node("translate", translate)
translate_builder.add_edge(START, "expand")
translate_builder.add_edge("expand", "translate")
translate_builder.add_edge("translate", END)
translate_graph = translate_builder.compile()


# --- Outer graph -------------------------------------------------------------

class OuterState(TypedDict):
    text: str


async def preprocess(state: OuterState) -> OuterState:
    await asyncio.sleep(2.0)
    return {"text": state["text"].strip()}


async def dual_processor(state: OuterState) -> OuterState:
    """Runs summarise and translate pipelines concurrently."""
    await asyncio.sleep(1.5)
    result_a, result_b = await asyncio.gather(
        summarise_graph.ainvoke({"text": state["text"]}),
        translate_graph.ainvoke({"text": state["text"]}),
    )
    return {"text": result_a["text"] + " | " + result_b["text"]}


async def postprocess(state: OuterState) -> OuterState:
    await asyncio.sleep(2.0)
    return {"text": state["text"].capitalize()}


outer_builder = StateGraph(OuterState)
outer_builder.add_node("preprocess", preprocess)
outer_builder.add_node("dual_processor", dual_processor)
outer_builder.add_node("postprocess", postprocess)
outer_builder.add_edge(START, "preprocess")
outer_builder.add_edge("preprocess", "dual_processor")
outer_builder.add_edge("dual_processor", "postprocess")
outer_builder.add_edge("postprocess", END)

graph = outer_builder.compile()
graph = watch(graph)


async def main() -> None:
    """Main function - run the example"""
    result = await graph.ainvoke({"text": "The quick brown fox jumps over the lazy dog near the river"})
    print("Result:", result["text"])
    # delay = 10
    # print(f"Open http://localhost:8764 to inspect — server stays up for {delay} seconds.")
    # await asyncio.sleep(delay)


asyncio.run(main())
