"""Twitter API Advanced Search Test Runner

Tests Twitter API with different query scenarios ranging from basic to advanced.
Each scenario tests different query parameters (keywords, geo filters, language, time).

Usage:
    # Run with default scenario (03_keywords_warsaw_20km)
    python twitter_api.py

    # List available scenarios
    python twitter_api.py --list

    # Run specific scenario
    python twitter_api.py --scenario 01_basic_latest
    python twitter_api.py -s 02_keywords_latest

    # Or via environment variable
    set TWITTER_SCENARIO=01_basic_latest
    python twitter_api.py
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from twitter_search_scenarios import SCENARIOS, ScenarioId, list_scenarios

TWITTER_BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"


REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=REPO_ROOT / ".env")

API_KEY = os.getenv("TWITTER_API_KEY")
if not API_KEY:
    raise SystemExit("Missing TWITTER_API_KEY. Add it to .env (see .env.example).")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Test Twitter API Advanced Search with different scenarios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-s",
        "--scenario",
        type=str,
        default=os.getenv("TWITTER_SCENARIO", ScenarioId.KEYWORDS_LOCATION.value),
        help=f"Scenario ID to run (default: {ScenarioId.KEYWORDS_LOCATION.value})",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List all available scenarios and exit",
    )
    parser.add_argument(
        "-c",
        "--cursor",
        type=str,
        default="",
        help="Pagination cursor (use next_cursor from previous response)",
    )
    return parser.parse_args()


def print_available_scenarios():
    print("Available Twitter Search Scenarios:\n")
    for scenario_id in ScenarioId:
        scenario = SCENARIOS[scenario_id]
        print(f"  {scenario_id.value}")
        print(f"    {scenario.description}")
        print()


args = parse_args()

if args.list:
    print_available_scenarios()
    raise SystemExit(0)

# Scenario selection
try:
    SELECTED_SCENARIO = ScenarioId(args.scenario)
except ValueError:
    available = ", ".join(list_scenarios())
    raise SystemExit(
        f"Unknown scenario '{args.scenario}'. Available: {available}\n"
        f"Use --list to see detailed descriptions."
    )

scenario = SCENARIOS[SELECTED_SCENARIO]
cursor = args.cursor

querystring = scenario.build_params()
if cursor:
    querystring["cursor"] = cursor

headers = {"X-API-Key": API_KEY}

print(f"SCENARIO: {SELECTED_SCENARIO.value}")
print(f"DESC: {scenario.description}")
print(f"DEBUG queryType: {querystring.get('queryType')}")
print(f"DEBUG query: {querystring.get('query')}")

try:
    req = requests.Request("GET", TWITTER_BASE_URL, headers=headers, params=querystring)
    prepared = req.prepare()
    print(f"\nREQUEST URL: {prepared.url}\n")

    response = requests.get(
        TWITTER_BASE_URL,
        headers=headers,
        params=querystring,
        timeout=30,
    )

    # Throw on HTTP errors (gives clearer failures than silent else-branch)
    response.raise_for_status()
    data = response.json()

    # Check if there are any tweets
    tweets = data.get("tweets") or []
    if tweets:
        print(f"Success! Retrieved {len(tweets)} tweets.\n")

        processed_data = []
        for tweet in tweets:
            # Extract data according to API documentation
            tweet_id = tweet.get("id", "") or tweet.get("id_str", "")
            created_at = tweet.get("createdAt", "") or tweet.get("created_at", "")

            # Author handling (API field: author)
            author_data = tweet.get("author") or tweet.get("user") or {}
            author_name = (
                author_data.get("name")
                or author_data.get("userName")
                or author_data.get("screen_name")
                or "Unknown"
            )

            text = tweet.get("text", "") or tweet.get("full_text", "")

            # Hashtags
            entities = tweet.get("entities", {})
            hashtags_list = entities.get("hashtags", [])
            hashtags_str = ",".join(
                [f"#{h.get('text', '')}" for h in hashtags_list if h.get("text")]
            )

            # Data acquisition date
            data_acquisition_date = datetime.now().strftime("%Y-%m-%d")

            row = {
                "tweet_id": tweet_id,
                "created_at": created_at,
                "author_name": author_name,
                "text": text,
                "hashtags": hashtags_str,
                "data_acquisition_date": data_acquisition_date,
            }
            processed_data.append(row)

        # --- RESULTS PRESENTATION ---
        print("Data sample (first 5 records):")
        for row in processed_data[:5]:
            print(row)

        # Pagination hints
        if data.get("has_next_page"):
            print("\nThere is another page of results.")
            print("Set cursor = next_cursor and run again:")
            print("next_cursor:", data.get("next_cursor"))

            # Optional CSV export
            # df.to_csv("twitter_test_data.csv", index=False, quotechar='"')

    else:
        print(
            "Query valid, but no results (0 tweets). Try changing keywords or increasing km range."
        )
        print("Raw response:", data)

except Exception as e:
    print(f"Critical error occurred: {e}")
