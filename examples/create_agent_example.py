"""ReAct agent built with langchain.agents.create_agent.

Four tools with a deliberate dependency chain:
  1. get_coordinates  — look up lat/lon for a city (prerequisite for others)
  2. get_weather      — get weather using coordinates from step 1
  3. get_air_quality  — get air quality index using coordinates from step 1
  4. get_travel_tips  — summarise travel advice using weather + air quality from steps 2–3

The system prompt instructs the model to call them in dependency order so the
visualization clearly shows sequential tool activations.

Each tool sleeps 2–3 seconds to make the execution visible in the UI.

Set OPENAI_API_KEY in the environment before running:
    uv run examples/create_agent_example.py
"""

import asyncio
import random

from langchain.agents import create_agent
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

from langgraphics import watch


@tool
async def get_coordinates(city: str) -> str:
    """Look up the geographic coordinates (latitude and longitude) for a city.
    Always call this first before any other location-based tools."""
    await asyncio.sleep(random.uniform(2, 3))
    coords = {
        "paris": (48.8566, 2.3522),
        "tokyo": (35.6762, 139.6503),
        "new york": (40.7128, -74.0060),
    }
    lat, lon = coords.get(city.lower(), (51.5074, -0.1278))
    return f"Coordinates for {city}: lat={lat}, lon={lon}"


@tool
async def get_weather(lat: float, lon: float) -> str:
    """Get the current weather conditions for a location given its latitude and longitude.
    Requires coordinates from get_coordinates."""
    await asyncio.sleep(random.uniform(2, 3))
    return f"Weather at ({lat}, {lon}): Partly cloudy, 18°C, humidity 65%, wind 12 km/h NW."


@tool
async def get_air_quality(lat: float, lon: float) -> str:
    """Get the current air quality index (AQI) for a location given its latitude and longitude.
    Requires coordinates from get_coordinates."""
    await asyncio.sleep(random.uniform(2, 3))
    return f"Air quality at ({lat}, {lon}): AQI 42 (Good). PM2.5: 8 µg/m³, O3: 55 µg/m³."


@tool
async def get_travel_tips(weather_summary: str, air_quality_summary: str) -> str:
    """Generate practical travel tips based on weather and air quality summaries.
    Requires results from both get_weather and get_air_quality."""
    await asyncio.sleep(random.uniform(2, 3))
    return (
        f"Travel tips based on conditions ({weather_summary[:40]}… / {air_quality_summary[:40]}…): "
        "Dress in light layers, bring a compact umbrella. Air quality is good — outdoor activities are fine. "
        "Best time to visit outdoor attractions: late morning."
    )


llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

agent = create_agent(
    llm,
    [get_coordinates, get_weather, get_air_quality, get_travel_tips],
    system_prompt=(
        "You are a travel assistant. Follow this exact sequence of tool calls:\n"
        "1. Call get_coordinates to get the lat/lon for the requested city.\n"
        "2. Call get_weather with the lat/lon from step 1.\n"
        "3. Call get_air_quality with the lat/lon from step 1.\n"
        "4. Call get_travel_tips with the weather and air quality summaries from steps 2 and 3.\n"
        "Only after all four tool calls, provide a concise final answer."
    ),
)

agent = watch(agent)


async def main() -> None:
    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": "I'm planning to visit Paris. What should I know?"}]}
    )
    print(result["messages"][-1].content)


if __name__ == "__main__":
    asyncio.run(main())
