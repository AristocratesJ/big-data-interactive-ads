"""Twitter Advanced Search Query Scenarios

Defines progressive test scenarios for Twitter API advanced search,
ranging from simple queries to complex combinations with:
- Keywords (OR groups)
- Geographic filters (near/within)
- Language constraints
- Time windows (since)

Each scenario builds on the previous one, adding complexity incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Callable


@dataclass(frozen=True)
class Scenario:
    description: str
    build_params: Callable[[], dict[str, str]]


class ScenarioId(StrEnum):
    BASIC = "01_basic"
    KEYWORDS = "02_keywords"
    LOCATION = "03_location"
    KEYWORDS_LOCATION = "04_keywords_location"
    KEYWORDS_LOCATION_SINCE = "05_keywords_location_since"
    KEYWORDS_LOCATION_TOP = "06_keywords_location_top"
    KEYWORDS_LOCATION_SINCE_TOP = "07_keywords_location_since_top"


def _or_keywords(keywords: list[str]) -> str:
    # If a keyword contains spaces, wrap it in quotes.
    safe = [f'"{k}"' if " " in k else k for k in keywords]
    return "(" + " OR ".join(safe) + ")"


NEGATIVE_PL_KEYWORDS = [
    "zmęczony",
    "zmeczony",
    "korek",
    "smog",
    "zimno",
    "nienawidzę",
    "depresja",
]


def _format_since_utc(hours: int) -> str:
    # Docs example: since:2021-12-31_23:59:59_UTC
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    return since.strftime("%Y-%m-%d_%H:%M:%S_UTC")


def scenario_01_basic() -> dict[str, str]:
    # Minimal request: simple Warsaw query
    return {
        "queryType": "Latest",
        "query": "Warsaw",
    }


def scenario_02_keywords() -> dict[str, str]:
    # Keywords only (OR-grouping), no geo constraints
    return {
        "queryType": "Latest",
        "query": _or_keywords(NEGATIVE_PL_KEYWORDS),
    }


def scenario_03_location() -> dict[str, str]:
    # Location only: geo filter near Warsaw
    return {
        "queryType": "Latest",
        "query": "geocode:52.2297,21.0122,20km",
    }


def scenario_04_keywords_location() -> dict[str, str]:
    # Keywords + location (production baseline)
    query = f"{_or_keywords(NEGATIVE_PL_KEYWORDS)} geocode:52.2297,21.0122,20km"
    return {
        "queryType": "Latest",
        "query": query,
    }


def scenario_05_keywords_location_since() -> dict[str, str]:
    # Keywords + location + time window (last 24h)
    since = _format_since_utc(hours=24)
    query = f"{_or_keywords(NEGATIVE_PL_KEYWORDS)} geocode:52.2297,21.0122,20km since:{since}"
    return {
        "queryType": "Latest",
        "query": query,
    }


def scenario_06_keywords_location_top() -> dict[str, str]:
    # Keywords + location + Top queryType (most relevant/popular)
    query = f"{_or_keywords(NEGATIVE_PL_KEYWORDS)} geocode:52.2297,21.0122,20km"
    return {
        "queryType": "Top",
        "query": query,
    }


def scenario_07_keywords_location_since_top() -> dict[str, str]:
    # Keywords + location + time window + Top queryType
    since = _format_since_utc(hours=24)
    query = f"{_or_keywords(NEGATIVE_PL_KEYWORDS)} geocode:52.2297,21.0122,20km since:{since}"
    return {
        "queryType": "Top",
        "query": query,
    }


SCENARIOS: dict[ScenarioId, Scenario] = {
    ScenarioId.BASIC: Scenario(
        description="Basic: query=Warsaw, queryType=Latest",
        build_params=scenario_01_basic,
    ),
    ScenarioId.KEYWORDS: Scenario(
        description="Keywords only (OR group), no location filter",
        build_params=scenario_02_keywords,
    ),
    ScenarioId.LOCATION: Scenario(
        description="Location only: near:Warsaw",
        build_params=scenario_03_location,
    ),
    ScenarioId.KEYWORDS_LOCATION: Scenario(
        description="Keywords + Location (production baseline)",
        build_params=scenario_04_keywords_location,
    ),
    ScenarioId.KEYWORDS_LOCATION_SINCE: Scenario(
        description="Keywords + Location + Since (last 24h)",
        build_params=scenario_05_keywords_location_since,
    ),
    ScenarioId.KEYWORDS_LOCATION_TOP: Scenario(
        description="Keywords + Location + Top queryType",
        build_params=scenario_06_keywords_location_top,
    ),
    ScenarioId.KEYWORDS_LOCATION_SINCE_TOP: Scenario(
        description="Keywords + Location + Since + Top queryType",
        build_params=scenario_07_keywords_location_since_top,
    ),
}

# Validate that all enum values have corresponding scenarios
assert set(SCENARIOS.keys()) == set(ScenarioId), (
    "SCENARIOS registry must contain all ScenarioId enum values"
)


def list_scenarios() -> list[str]:
    """Return list of scenario IDs as strings."""
    return [s.value for s in ScenarioId]


def get_scenario(scenario_id: ScenarioId | str) -> Scenario:
    """Get scenario by ID with validation."""
    if isinstance(scenario_id, str):
        scenario_id = ScenarioId(scenario_id)
    return SCENARIOS[scenario_id]
