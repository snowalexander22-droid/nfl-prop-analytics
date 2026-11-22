#!/usr/bin/env python3
"""
NFL Player Props & Game Totals Automated Analytics & Self-Learning Pipeline
100% Free-Tier Stack:
- ESPN Public JSON APIs (Scoreboard, Game Summaries, Live Numeric Team Rosters,
  Game-Specific Injury Reports, 2026 Season Leaders & Recent Real Box Scores)
- Open-Meteo API (Real-Time Stadium Weather Forecasts)
- Polymarket Gamma API (Live Binary Market Implied Probabilities)
- Public NFL RSS Feeds (ESPN, ProFootballTalk, CBS Sports, Yahoo Sports)
- Google AI Studio Gemini API (gemini-3.8-flash / 3.6-flash / 3.5-flash-lite)
- Firebase Firestore (Real-time Database + Auto-Purge + Post-Game Autopsy Loop)
"""

import os
import re
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set

import requests
import feedparser
from pydantic import BaseModel, Field

import firebase_admin
from firebase_admin import credentials, firestore

from google import genai
from google.genai import types

# Force unbuffered live logging in GitHub Actions
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nfl_pipeline")

# Active Free-Tier Gemini Models on Google AI Studio (separate quota buckets)
GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

# ESPN Public JSON Endpoints
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
ESPN_SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"
ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries"
ESPN_TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"

# Polymarket Gamma & Open-Meteo Endpoints
POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Free Public NFL News & Beat RSS Feeds
RSS_FEEDS = [
    ("ESPN NFL", "https://www.espn.com/espn/rss/nfl/news"),
    ("ProFootballTalk", "https://profootballtalk.nbcsports.com/feed/"),
    ("CBS Sports NFL", "https://www.cbssports.com/rss/headlines/nfl/"),
    ("Yahoo Sports NFL", "https://sports.yahoo.com/nfl/rss.xml"),
]

# Complete 32-Team NFL Stadium Metadata (Coordinates & Roof Status)
NFL_STADIUMS: Dict[str, Dict[str, Any]] = {
    "ARI": {"stadium": "State Farm Stadium", "lat": 33.5276, "lon": -112.2626, "dome": True},
    "ATL": {"stadium": "Mercedes-Benz Stadium", "lat": 33.7554, "lon": -84.4009, "dome": True},
    "BAL": {"stadium": "M&T Bank Stadium", "lat": 39.2780, "lon": -76.6227, "dome": False},
    "BUF": {"stadium": "Highmark Stadium", "lat": 42.7738, "lon": -78.7870, "dome": False},
    "CAR": {"stadium": "Bank of America Stadium", "lat": 35.2258, "lon": -80.8528, "dome": False},
    "CHI": {"stadium": "Soldier Field", "lat": 41.8623, "lon": -87.6167, "dome": False},
    "CIN": {"stadium": "Paycor Stadium", "lat": 39.0955, "lon": -84.5161, "dome": False},
    "CLE": {"stadium": "Huntington Bank Field", "lat": 41.5061, "lon": -81.6995, "dome": False},
    "DAL": {"stadium": "AT&T Stadium", "lat": 32.7473, "lon": -97.0945, "dome": True},
    "DEN": {"stadium": "Empower Field at Mile High", "lat": 39.7439, "lon": -105.0201, "dome": False},
    "DET": {"stadium": "Ford Field", "lat": 42.3400, "lon": -83.0456, "dome": True},
    "GB":  {"stadium": "Lambeau Field", "lat": 44.5013, "lon": -88.0622, "dome": False},
    "HOU": {"stadium": "NRG Stadium", "lat": 29.6847, "lon": -95.4107, "dome": True},
    "IND": {"stadium": "Lucas Oil Stadium", "lat": 39.7601, "lon": -86.1639, "dome": True},
    "JAX": {"stadium": "EverBank Stadium", "lat": 30.3239, "lon": -81.6373, "dome": False},
    "KC":  {"stadium": "GEHA Field at Arrowhead Stadium", "lat": 39.0489, "lon": -94.4839, "dome": False},
    "LV":  {"stadium": "Allegiant Stadium", "lat": 36.0909, "lon": -115.1833, "dome": True},
    "LAC": {"stadium": "SoFi Stadium", "lat": 33.9535, "lon": -118.3392, "dome": True},
    "LAR": {"stadium": "SoFi Stadium", "lat": 33.9535, "lon": -118.3392, "dome": True},
    "MIA": {"stadium": "Hard Rock Stadium", "lat": 25.9580, "lon": -80.2389, "dome": False},
    "MIN": {"stadium": "U.S. Bank Stadium", "lat": 44.9736, "lon": -93.2575, "dome": True},
    "NE":  {"stadium": "Gillette Stadium", "lat": 42.0909, "lon": -71.2643, "dome": False},
    "NO":  {"stadium": "Caesars Superdome", "lat": 29.9511, "lon": -90.0812, "dome": True},
    "NYG": {"stadium": "MetLife Stadium", "lat": 40.8128, "lon": -74.0742, "dome": False},
    "NYJ": {"stadium": "MetLife Stadium", "lat": 40.8128, "lon": -74.0742, "dome": False},
    "PHI": {"stadium": "Lincoln Financial Field", "lat": 39.9008, "lon": -75.1675, "dome": False},
    "PIT": {"stadium": "Acrisure Stadium", "lat": 40.4468, "lon": -80.0158, "dome": False},
    "SF":  {"stadium": "Levi's Stadium", "lat": 37.4033, "lon": -121.9694, "dome": False},
    "SEA": {"stadium": "Lumen Field", "lat": 47.5952, "lon": -122.3316, "dome": False},
    "TB":  {"stadium": "Raymond James Stadium", "lat": 27.9759, "lon": -82.5033, "dome": False},
    "TEN": {"stadium": "Nissan Stadium", "lat": 36.1665, "lon": -86.7713, "dome": False},
    "WSH": {"stadium": "Northwest Stadium", "lat": 38.9076, "lon": -76.8645, "dome": False},
    "WAS": {"stadium": "Northwest Stadium", "lat": 38.9076, "lon": -76.8645, "dome": False},
}

RULED_OUT_KEYWORDS = (
    "out",
    "doubtful",
    "injured reserve",
    "ir",
    "pup",
    "nfi",
    "suspended",
    "non-football",
    "reserve",
    "practice",
    "waived",
)


# =====================================================================
# PYDANTIC SCHEMAS FOR GEMINI STRUCTURED JSON OUTPUT
# =====================================================================

class GameTotalMarketSchema(BaseModel):
    market_id: str = Field(description="Unique ID, e.g., BUF_KC_FULL_OVER or BUF_TEAM_OVER")
    market_type: str = Field(description="Strictly 'FULL_GAME_TOTAL', 'AWAY_TEAM_TOTAL', or 'HOME_TEAM_TOTAL'")
    target_team: str = Field(description="Both team abbrs for full game (e.g. 'BUF @ KC') or single team abbr")
    direction: str = Field(description="'OVER' or 'UNDER'")
    line: float = Field(description="Numerical points line, e.g., 47.5 or 24.5")
    true_prob: float = Field(description="Model true hit probability between 0.05 and 0.95")
    polymarket_prob: float = Field(description="Polymarket implied probability between 0.05 and 0.95")
    ev_edge_pct: float = Field(description="True probability minus Polymarket probability in percentage points")
    tier: int = Field(description="1 (Green Elite), 2 (Yellow Strong), 3 (Orange Neutral), or 4 (Red High Risk)")
    tier_label: str = Field(description="'Tier 1 (Green)', 'Tier 2 (Yellow)', 'Tier 3 (Orange)', or 'Tier 4 (Red)'")
    intel_badges: List[str] = Field(description="2 concise, specific analytical badges explaining why")


class PlayerPropSchema(BaseModel):
    prop_id: str = Field(description="Unique slug ID, e.g., josh_allen_pass_yds_over")
    player_name: str = Field(description="EXACT full player name copied verbatim from verified_active_players")
    team: str = Field(description="Player team abbreviation matching verified_active_players")
    opponent: str = Field(description="Opponent team abbreviation")
    position: str = Field(description="Strictly 'QB', 'RB', 'WR', or 'TE'")
    stat_category: str = Field(description="e.g., 'Passing Yards', 'Rushing Yards', 'Receiving Yards', 'Receptions', 'Passing TDs'")
    direction: str = Field(description="'OVER' or 'UNDER'")
    line: float = Field(description="Prop numerical line, e.g., 245.5")
    season_2026_hit_rate: float = Field(description="2026 season hit rate as percentage 0 to 100")
    rolling_17g_hit_rate: float = Field(description="Rolling 17-game hit rate as percentage 0 to 100")
    active_over_streak: int = Field(description="Consecutive games hitting the Over on this line")
    opp_def_rank_vs_pos: int = Field(description="Opponent defensive rank vs this position (1 = toughest, 32 = softest)")
    true_prob: float = Field(description="AI synthesized true probability between 0.05 and 0.95")
    polymarket_prob: float = Field(description="Polymarket implied probability between 0.05 and 0.95")
    ev_edge_pct: float = Field(description="True probability minus Polymarket probability in percentage points")
    tier: int = Field(description="1 (Green Elite), 2 (Yellow Strong), 3 (Orange Neutral), or 4 (Red High Risk)")
    tier_label: str = Field(description="'Tier 1 (Green)', 'Tier 2 (Yellow)', 'Tier 3 (Orange)', or 'Tier 4 (Red)'")
    intel_badges: List[str] = Field(description="2 actionable bullet notes grounded in recent stats, weather, defense, or injury status")


class BatchMatchupItemSchema(BaseModel):
    game_id: str = Field(description="Exact ESPN game_id provided in the input object")
    game_totals: List[GameTotalMarketSchema] = Field(
        description="Strictly 3 items: FULL_GAME_TOTAL, AWAY_TEAM_TOTAL, and HOME_TEAM_TOTAL"
    )
    player_props: List[PlayerPropSchema] = Field(
        description="6 high-value QB, RB, WR, TE player props strictly chosen from verified_active_players"
    )
    matchup_headline: str = Field(description="1-sentence sharp analytical summary of the game script and weather")


class BatchMatchupSynthesisSchema(BaseModel):
    matchups: List[BatchMatchupItemSchema]


class ParlayLegSchema(BaseModel):
    leg_id: str
    game_id: str
    matchup_label: str
    leg_type: str = Field(description="'PLAYER_PROP', 'FULL_GAME_TOTAL', or 'TEAM_TOTAL'")
    selection_label: str = Field(description="e.g., 'Josh Allen (BUF) OVER 245.5 Passing Yards'")
    team: str
    true_prob: float
    polymarket_prob: float
    ev_edge_pct: float
    tier: int
    rationale: str


class SuggestedParlaySchema(BaseModel):
    parlay_rank: int = Field(description="1 to 5")
    parlay_title: str = Field(description="Sharp descriptive name for this parlay")
    legs: List[ParlayLegSchema] = Field(description="3 to 6 legs with zero negative correlation")
    combined_true_prob: float = Field(description="Product of leg true probabilities (0.01 to 0.90)")
    combined_polymarket_prob: float = Field(description="Product of leg Polymarket probabilities")
    net_ev_edge_pct: float = Field(description="Combined edge percentage")
    correlation_safety_note: str = Field(description="Verification of non-crossover and positive/neutral game script alignment")


class FinalFiveParlaysResponseSchema(BaseModel):
    parlays: List[SuggestedParlaySchema] = Field(description="Strictly 5 non-crossover parlays ranked 1 to 5")


class PostMortemAutopsySchema(BaseModel):
    failure_headline: str = Field(description="Concise 4-8 word failure cause")
    root_cause_category: str = Field(
        description="One of: 'GAME_SCRIPT_BLOWOUT', 'IN_GAME_INJURY', 'WEATHER_VARIANCE', 'DEFENSIVE_SCHEME_SHADOW', 'USAGE_COMMITTEE_SHIFT', 'RED_ZONE_VARIANCE'"
    )
    detailed_autopsy: str = Field(description="2-3 sentences analyzing the final box score, weather, and play-by-play script")
    learned_adjustment_rule: str = Field(description="Actionable rule for future Gemini prompts")
    parameter_weight_delta: str = Field(description="Specific weighting tweak")


# =====================================================================
# INITIALIZATION & FAST GEMINI HELPERS
# =====================================================================

def init_firestore() -> firestore.Client:
    if not firebase_admin._apps:
        sa_raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
        if not sa_raw:
            raise RuntimeError("Missing FIREBASE_SERVICE_ACCOUNT environment variable.")
        cred = credentials.Certificate(json.loads(sa_raw))
        firebase_admin.initialize_app(cred)
    return firestore.client()


def init_gemini_client() -> genai.Client:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")
    return genai.Client(api_key=api_key)


def _build_gen_config(
    schema: Any,
    system_instruction: str,
    temperature: float,
) -> types.GenerateContentConfig:
    """Builds GenerateContentConfig with low thinking latency for fast structured JSON."""
    base_kwargs: Dict[str, Any] = {
        "system_instruction": system_instruction,
        "response_mime_type": "application/json",
        "response_schema": schema,
        "temperature": temperature,
        "max_output_tokens": 16384,
    }
    try:
        base_kwargs["thinking_config"] = types.ThinkingConfig(thinking_level="low")
        return types.GenerateContentConfig(**base_kwargs)
    except Exception:
        base_kwargs.pop("thinking_config", None)
        return types.GenerateContentConfig(**base_kwargs)


def call_gemini_structured(
    client: genai.Client,
    prompt: str,
    schema: Any,
    system_instruction: str,
    temperature: float = 0.2,
) -> Any:
    """
    Calls free-tier Gemini models with low thinking latency.
    Immediately jumps to the next free-tier model on 429/quota errors to prevent timeouts.
    """
    config = _build_gen_config(schema, system_instruction, temperature)
    last_error: Optional[Exception] = None

    for model_name in GEMINI_MODELS:
        for attempt in range(2):
            t0 = time.time()
            try:
                logger.info(f"Calling {model_name} (attempt {attempt + 1})...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=config,
                )
                elapsed = round(time.time() - t0, 1)
                if response and response.text:
                    parsed = json.loads(response.text)
                    logger.info(f"Success from {model_name} in {elapsed}s")
                    return parsed
                raise ValueError("Empty response text from Gemini.")
            except Exception as exc:
                last_error = exc
                err_str = str(exc)
                logger.warning(f"Gemini '{model_name}' attempt {attempt + 1} failed ({round(time.time() - t0, 1)}s): {err_str[:220]}")
                # If quota/rate-limited or model unavailable, jump immediately to next model
                if any(code in err_str for code in ("429", "RESOURCE_EXHAUSTED", "404", "NOT_FOUND")):
                    time.sleep(2)
                    break
                time.sleep(4)

    raise RuntimeError(f"All Gemini model calls failed. Last error: {last_error}")


# =====================================================================
# FREE ESPN, WEATHER, POLYMARKET & RSS DATA INGESTION
# =====================================================================

def normalize_name(name: str) -> str:
    """Normalizes player names for exact cross-referencing across ESPN endpoints."""
    cleaned = re.sub(r"\b(jr\.?|sr\.?|ii|iii|iv|v)\b", "", name.lower())
    cleaned = re.sub(r"[^a-z\s]", "", cleaned)
    return " ".join(cleaned.split())


def is_ruled_out_status(status_text: str) -> bool:
    s = (status_text or "").strip().lower()
    if not s:
        return False
    return any(re.search(rf"\b{re.escape(kw)}\b", s) or (kw in s and len(kw) > 3) for kw in RULED_OUT_KEYWORDS)


def fetch_espn_scoreboard(week: Optional[int] = None, season_year: Optional[int] = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if week is not None:
        params["week"] = int(week)
        params["seasontype"] = 2
    if season_year is not None:
        params["dates"] = str(season_year)
    resp = requests.get(ESPN_SCOREBOARD_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_espn_game_summary(event_id: str) -> Dict[str, Any]:
    try:
        resp = requests.get(ESPN_SUMMARY_URL, params={"event": event_id}, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning(f"Could not fetch summary for event {event_id}: {exc}")
        return {}


def fetch_espn_league_injuries() -> Dict[str, List[Dict[str, str]]]:
    """Fetches league-wide NFL injury report grouped by team abbreviation."""
    injuries_by_team: Dict[str, List[Dict[str, str]]] = {}
    try:
        resp = requests.get(ESPN_INJURIES_URL, timeout=20)
        if resp.status_code != 200:
            return injuries_by_team
        data = resp.json()
        for team_entry in data.get("injuries", []):
            abbr = team_entry.get("team", {}).get("abbreviation", "UNK").upper()
            team_list = []
            for item in team_entry.get("injuries", []):
                athlete = item.get("athlete", {}).get("displayName", "Unknown").strip()
                pos = item.get("athlete", {}).get("position", {}).get("abbreviation", "")
                status = item.get("status", "").strip()
                detail = item.get("details", {}).get("type", "").strip()
                team_list.append({
                    "player_name": athlete,
                    "position": pos,
                    "status": status,
                    "detail": detail,
                    "label": f"{athlete} ({pos}) - {status}" + (f" [{detail}]" if detail else ""),
                })
            injuries_by_team[abbr] = team_list
    except Exception as exc:
        logger.warning(f"League injury fetch warning: {exc}")
    return injuries_by_team


def extract_game_summary_injuries(summary_data: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    """Extracts the official game-specific injury report directly from ESPN /summary."""
    out: Dict[str, List[Dict[str, str]]] = {}
    for team_block in summary_data.get("injuries", []):
        abbr = team_block.get("team", {}).get("abbreviation", "UNK").upper()
        items = []
        for inj in team_block.get("injuries", []):
            athlete_obj = inj.get("athlete", {})
            name = athlete_obj.get("displayName", "").strip()
            if not name:
                continue
            pos = athlete_obj.get("position", {}).get("abbreviation", "")
            status = inj.get("status", "").strip()
            detail = inj.get("details", {}).get("type", "") or inj.get("type", {}).get("description", "")
            items.append({
                "player_name": name,
                "position": pos,
                "status": status,
                "detail": detail.strip(),
                "label": f"{name} ({pos}) - {status}" + (f" [{detail.strip()}]" if detail.strip() else ""),
            })
        out[abbr] = items
    return out


def extract_game_summary_leaders(summary_data: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Extracts 2026 season statistical leaders per team from ESPN /summary."""
    leaders_by_team: Dict[str, Dict[str, str]] = {}
    for team_block in summary_data.get("leaders", []):
        abbr = team_block.get("team", {}).get("abbreviation", "UNK").upper()
        team_map: Dict[str, str] = {}
        for cat_block in team_block.get("leaders", []):
            cat_name = cat_block.get("displayName", cat_block.get("name", ""))
            for entry in cat_block.get("leaders", []):
                name = entry.get("athlete", {}).get("displayName", "").strip()
                stat_val = entry.get("displayValue", "").strip()
                if name:
                    norm = normalize_name(name)
                    prev = team_map.get(norm, "")
                    entry_str = f"Season Leader ({cat_name}: {stat_val})"
                    team_map[norm] = f"{prev}; {entry_str}".strip("; ")
        leaders_by_team[abbr] = team_map
    return leaders_by_team


def extract_final_box_score_stats(summary_data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """Parses ESPN final box score into player_name -> {stat_category: value}."""
    player_stats: Dict[str, Dict[str, float]] = {}
    boxscore = summary_data.get("boxscore", {})
    for team_block in boxscore.get("players", []):
        for stat_group in team_block.get("statistics", []):
            group_name = stat_group.get("name", "").lower()
            labels = stat_group.get("labels", [])
            for athlete_row in stat_group.get("athletes", []):
                a_name = athlete_row.get("athlete", {}).get("displayName", "").strip().lower()
                if not a_name:
                    continue
                stats_arr = athlete_row.get("stats", [])
                if a_name not in player_stats:
                    player_stats[a_name] = {}

                for lbl, val in zip(labels, stats_arr):
                    try:
                        if group_name == "passing":
                            if lbl == "YDS":
                                player_stats[a_name]["Passing Yards"] = float(val)
                            elif lbl == "TD":
                                player_stats[a_name]["Passing TDs"] = float(val)
                        elif group_name == "rushing":
                            if lbl == "YDS":
                                player_stats[a_name]["Rushing Yards"] = float(val)
                            elif lbl == "CAR":
                                player_stats[a_name]["Rushing Attempts"] = float(val)
                        elif group_name == "receiving":
                            if lbl == "YDS":
                                player_stats[a_name]["Receiving Yards"] = float(val)
                            elif lbl == "REC":
                                player_stats[a_name]["Receptions"] = float(val)
                            elif lbl == "TGTS":
                                player_stats[a_name]["Targets"] = float(val)
                    except Exception:
                        continue
    return player_stats


def fetch_recent_onfield_producers(
    current_week: int,
    season_year: int,
) -> tuple[Dict[str, Dict[str, Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Fetches completed games from Week (current_week - 1) and (current_week - 2)
    so we know with 100% certainty WHICH players actually played and recorded
    passing/rushing/receiving production for every NFL team in the last 14 days.
    Also returns last week's completed events so pending parlays get graded automatically.
    """
    team_recent_players: Dict[str, Dict[str, Dict[str, Any]]] = {}
    prev_week_events: List[Dict[str, Any]] = []

    weeks_to_scan = [w for w in [current_week - 1, current_week - 2] if w >= 1]
    seen_teams: Set[str] = set()

    for w in weeks_to_scan:
        try:
            sb = fetch_espn_scoreboard(week=w, season_year=season_year)
            w_events = sb.get("events", [])
            if w == current_week - 1:
                prev_week_events = w_events

            for ev in w_events:
                status_name = ev.get("status", {}).get("type", {}).get("name", "")
                if status_name != "STATUS_FINAL":
                    continue

                comp = ev.get("competitions", [{}])[0]
                comp_abbrs = [
                    c.get("team", {}).get("abbreviation", "").upper()
                    for c in comp.get("competitors", [])
                ]
                # Skip fetching box score if we already captured both teams' most recent game
                if all(abbr in seen_teams for abbr in comp_abbrs if abbr):
                    continue

                summary = fetch_espn_game_summary(str(ev.get("id", "")))
                boxscore = summary.get("boxscore", {})

                for team_block in boxscore.get("players", []):
                    t_abbr = team_block.get("team", {}).get("abbreviation", "").upper()
                    if not t_abbr or t_abbr in seen_teams:
                        continue

                    t_players: Dict[str, Dict[str, Any]] = {}
                    for stat_group in team_block.get("statistics", []):
                        gname = stat_group.get("name", "").lower()
                        if gname not in ("passing", "rushing", "receiving"):
                            continue
                        labels = stat_group.get("labels", [])
                        for row in stat_group.get("athletes", []):
                            a_obj = row.get("athlete", {})
                            display_name = a_obj.get("displayName", "").strip()
                            if not display_name:
                                continue
                            norm = normalize_name(display_name)
                            if norm not in t_players:
                                t_players[norm] = {
                                    "name": display_name,
                                    "week": w,
                                    "score": 0.0,
                                    "stats": {},
                                }
                            stats_arr = row.get("stats", [])
                            for lbl, val in zip(labels, stats_arr):
                                try:
                                    if gname == "passing" and lbl in ("C/ATT", "YDS", "TD"):
                                        t_players[norm]["stats"][f"Pass {lbl}"] = val
                                        if lbl == "YDS":
                                            t_players[norm]["score"] += float(val) * 0.5
                                    elif gname == "rushing" and lbl in ("CAR", "YDS", "TD"):
                                        t_players[norm]["stats"][f"Rush {lbl}"] = val
                                        if lbl == "YDS":
                                            t_players[norm]["score"] += float(val) * 1.2
                                        elif lbl == "CAR":
                                            t_players[norm]["score"] += float(val) * 4.0
                                    elif gname == "receiving" and lbl in ("REC", "YDS", "TD", "TGTS"):
                                        t_players[norm]["stats"][f"Rec {lbl}"] = val
                                        if lbl == "YDS":
                                            t_players[norm]["score"] += float(val) * 1.2
                                        elif lbl == "REC":
                                            t_players[norm]["score"] += float(val) * 6.0
                                except Exception:
                                    continue

                    if t_players:
                        team_recent_players[t_abbr] = t_players
                        seen_teams.add(t_abbr)
        except Exception as exc:
            logger.warning(f"Recent box score scan warning for Week {w}: {exc}")

    logger.info(f"Verified recent on-field box score producers for {len(team_recent_players)} NFL teams.")
    return team_recent_players, prev_week_events


def fetch_verified_team_starters(
    team_id: str,
    team_abbr: str,
    combined_injuries: List[Dict[str, str]],
    recent_box_map: Dict[str, Dict[str, Any]],
    season_leaders_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Builds a 100% verified list of active QB, RB, WR, TE players for THIS team right now:
    1. Fetches ESPN's live roster using numeric team_id (never fails on abbr mismatches).
    2. Strictly requires status.type == 'active' inside the 'offense' group (excludes IR/PUP/Practice Squad).
    3. Excludes anyone listed as Out, Doubtful, IR, PUP, or Suspended on game or league injury reports.
    4. Ranks starters by ACTUAL recent 2026 game production + 2026 season leaders (never by age/experience).
    """
    ruled_out_norms: Set[str] = set()
    questionable_details: Dict[str, str] = {}

    for inj in combined_injuries:
        norm = normalize_name(inj.get("player_name", ""))
        if not norm:
            continue
        status = inj.get("status", "")
        if is_ruled_out_status(status):
            ruled_out_norms.add(norm)
        elif "questionable" in status.lower():
            questionable_details[norm] = f"Questionable ({inj.get('detail', 'injury')})"

    url = f"{ESPN_TEAMS_URL}/{team_id}/roster"
    active_offense: List[Dict[str, Any]] = []

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for group in data.get("athletes", []):
            group_pos = str(group.get("position", "")).lower()
            # Explicitly ignore injuredReserveOrOut, practiceSquad, defense, specialTeam
            if group_pos != "offense":
                continue

            for athlete in group.get("items", []):
                pos = athlete.get("position", {}).get("abbreviation", "").upper()
                if pos not in ("QB", "RB", "WR", "TE"):
                    continue

                full_name = athlete.get("displayName", "").strip()
                if not full_name:
                    continue

                status_obj = athlete.get("status", {})
                status_type = str(status_obj.get("type", "")).lower()
                status_name = str(status_obj.get("name", "")).lower()
                if status_type != "active" or is_ruled_out_status(status_name):
                    continue

                norm = normalize_name(full_name)
                if norm in ruled_out_norms:
                    continue

                recent_entry = recent_box_map.get(norm)
                leader_note = season_leaders_map.get(norm, "")

                # Calculate priority score driven by ACTUAL recent 2026 playing time & production
                priority_score = 0.0
                recent_stat_str = "Active 53-man roster (no touches last game)"
                if recent_entry:
                    priority_score += 100.0 + float(recent_entry.get("score", 0.0))
                    stat_parts = [f"{k}: {v}" for k, v in recent_entry.get("stats", {}).items()]
                    recent_stat_str = f"Wk {recent_entry['week']} Actual: " + ", ".join(stat_parts)
                if leader_note:
                    priority_score += 80.0

                active_offense.append({
                    "name": full_name,
                    "position": pos,
                    "team": team_abbr.upper(),
                    "injury_tag": questionable_details.get(norm, "Healthy / Active"),
                    "recent_production": recent_stat_str,
                    "season_leader_note": leader_note,
                    "_priority": priority_score,
                })
    except Exception as exc:
        logger.warning(f"Numeric roster fetch error for {team_abbr} (id={team_id}): {exc}")

    # Select top active starters per skill position ordered by real 2026 production
    pos_caps = {"QB": 1, "RB": 2, "WR": 3, "TE": 2}
    selected: List[Dict[str, Any]] = []
    for pos_code, cap in pos_caps.items():
        group_list = [p for p in active_offense if p["position"] == pos_code]
        group_list.sort(key=lambda x: x["_priority"], reverse=True)
        for p in group_list[:cap]:
            item = {k: v for k, v in p.items() if k != "_priority"}
            if not item["season_leader_note"]:
                item.pop("season_leader_note", None)
            selected.append(item)

    return selected


def fetch_stadium_weather(home_team_abbr: str, kickoff_iso: str) -> Dict[str, Any]:
    meta = NFL_STADIUMS.get(home_team_abbr.upper(), {
        "stadium": f"{home_team_abbr} Stadium",
        "lat": 39.8283,
        "lon": -98.5795,
        "dome": False,
    })
    if meta["dome"]:
        return {
            "stadium": meta["stadium"],
            "is_dome": True,
            "temp_f": 72.0,
            "wind_mph": 0.0,
            "precip_prob": 0,
            "summary": f"Indoors / Dome ({meta['stadium']}) - Controlled 72°F, 0 mph wind",
        }

    try:
        params = {
            "latitude": meta["lat"],
            "longitude": meta["lon"],
            "current": "temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m",
            "hourly": "temperature_2m,precipitation_probability,wind_speed_10m",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "forecast_days": 7,
        }
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        resp.raise_for_status()
        wdata = resp.json()
        current = wdata.get("current", {})
        temp_f = float(current.get("temperature_2m", 58.0))
        wind_mph = float(current.get("wind_speed_10m", 8.0))
        gusts_mph = float(current.get("wind_gusts_10m", wind_mph))
        precip_prob = 0

        hourly = wdata.get("hourly", {})
        times = hourly.get("time", [])
        target_prefix = kickoff_iso[:13] if kickoff_iso else ""
        for idx, t_str in enumerate(times):
            if target_prefix and t_str.startswith(target_prefix):
                temp_f = float(hourly.get("temperature_2m", [temp_f])[idx])
                wind_mph = float(hourly.get("wind_speed_10m", [wind_mph])[idx])
                precip_prob = int(hourly.get("precipitation_probability", [0])[idx])
                break

        summary = (
            f"Outdoor ({meta['stadium']}) | {round(temp_f)}°F | "
            f"Wind {round(wind_mph)} mph (Gusts {round(gusts_mph)} mph) | "
            f"Precip {precip_prob}%"
        )
        return {
            "stadium": meta["stadium"],
            "is_dome": False,
            "temp_f": round(temp_f, 1),
            "wind_mph": round(wind_mph, 1),
            "precip_prob": precip_prob,
            "summary": summary,
        }
    except Exception as exc:
        logger.warning(f"Open-Meteo fallback for {home_team_abbr}: {exc}")
        return {
            "stadium": meta["stadium"],
            "is_dome": False,
            "temp_f": 55.0,
            "wind_mph": 7.0,
            "precip_prob": 10,
            "summary": f"Outdoor ({meta['stadium']}) | ~55°F | Wind 7 mph",
        }


def fetch_polymarket_nfl_markets() -> List[Dict[str, Any]]:
    markets_out: List[Dict[str, Any]] = []
    try:
        params = {"tag_slug": "nfl", "active": "true", "closed": "false", "limit": 100}
        resp = requests.get(POLYMARKET_EVENTS_URL, params=params, timeout=20)
        if resp.status_code != 200:
            return markets_out
        events = resp.json()
        if not isinstance(events, list):
            return markets_out

        for ev in events:
            title = ev.get("title", "")
            for mkt in ev.get("markets", []):
                question = mkt.get("question", title)
                outcomes_raw = mkt.get("outcomes", "[]")
                prices_raw = mkt.get("outcomePrices", "[]")
                try:
                    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                except Exception:
                    outcomes, prices = [], []

                outcome_map = {}
                for o_label, p_val in zip(outcomes, prices):
                    try:
                        outcome_map[str(o_label)] = round(float(p_val), 4)
                    except Exception:
                        pass

                if outcome_map:
                    markets_out.append({
                        "event_title": title,
                        "question": question,
                        "implied_probabilities": outcome_map,
                        "volume": mkt.get("volume", 0),
                    })
    except Exception as exc:
        logger.warning(f"Polymarket fetch warning: {exc}")
    return markets_out


def fetch_nfl_rss_intel() -> List[Dict[str, str]]:
    articles: List[Dict[str, str]] = []
    for source_name, feed_url in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:15]:
                title = entry.get("title", "").strip()
                summary = re.sub(r"<[^>]+>", "", entry.get("summary", "")).strip()
                if title:
                    articles.append({
                        "source": source_name,
                        "title": title,
                        "summary": summary[:220],
                    })
        except Exception as exc:
            logger.warning(f"RSS error on {source_name}: {exc}")
    return articles


# =====================================================================
# POST-GAME GRADING & SELF-LEARNING AUTOPSY ENGINE
# =====================================================================

def evaluate_single_prop_outcome(
    direction: str,
    line: float,
    actual_value: Optional[float],
) -> Optional[str]:
    if actual_value is None:
        return None
    if direction.upper() == "OVER":
        return "WON" if actual_value > float(line) else "LOST"
    return "WON" if actual_value < float(line) else "LOST"


def grade_completed_games_and_run_autopsies(
    db: firestore.Client,
    gemini_client: genai.Client,
    events: List[Dict[str, Any]],
) -> int:
    """Grades completed games from both current week and prior week + runs failure autopsies."""
    autopsies_created = 0
    final_games_map: Dict[str, Dict[str, Any]] = {}

    for ev in events:
        status_name = ev.get("status", {}).get("type", {}).get("name", "")
        if status_name != "STATUS_FINAL":
            continue

        game_id = str(ev.get("id", ""))
        if game_id in final_games_map:
            continue

        comp = ev.get("competitions", [{}])[0]
        competitors = comp.get("competitors", [])
        home_team, away_team = "", ""
        home_score, away_score = 0.0, 0.0

        for c in competitors:
            abbr = c.get("team", {}).get("abbreviation", "")
            score = float(c.get("score", 0))
            if c.get("homeAway") == "home":
                home_team, home_score = abbr, score
            else:
                away_team, away_score = abbr, score

        summary_json = fetch_espn_game_summary(game_id)
        box_stats = extract_final_box_score_stats(summary_json)

        final_games_map[game_id] = {
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "full_total": home_score + away_score,
            "box_stats": box_stats,
        }

        matchup_ref = db.collection("weekly_matchups").document(game_id)
        matchup_snap = matchup_ref.get()
        if matchup_snap.exists:
            mdata = matchup_snap.to_dict() or {}
            if mdata.get("graded") is not True:
                graded_totals = []
                for gt in mdata.get("game_totals", []):
                    mtype = gt.get("market_type", "")
                    if mtype == "FULL_GAME_TOTAL":
                        actual = home_score + away_score
                    elif mtype == "HOME_TEAM_TOTAL":
                        actual = home_score
                    else:
                        actual = away_score
                    res = evaluate_single_prop_outcome(gt.get("direction", "OVER"), float(gt.get("line", 0)), actual)
                    gt["actual_value"] = actual
                    gt["status"] = res or "GRADED"
                    graded_totals.append(gt)

                graded_props = []
                for pp in mdata.get("player_props", []):
                    p_key = pp.get("player_name", "").strip().lower()
                    cat = pp.get("stat_category", "")
                    actual_val = box_stats.get(p_key, {}).get(cat)
                    res = evaluate_single_prop_outcome(pp.get("direction", "OVER"), float(pp.get("line", 0)), actual_val)
                    pp["actual_value"] = actual_val
                    pp["status"] = res or "VOID"
                    graded_props.append(pp)

                    if actual_val is not None:
                        streak_id = re.sub(r"[^a-z0-9_]", "_", f"{p_key}_{cat.lower()}")
                        s_ref = db.collection("player_streaks").document(streak_id)
                        s_doc = s_ref.get()
                        prev_streak = (s_doc.to_dict() or {}).get("active_over_streak", pp.get("active_over_streak", 0))
                        hit_over = actual_val > float(pp.get("line", 0))
                        new_streak = (int(prev_streak) + 1) if hit_over else 0
                        s_ref.set({
                            "player_name": pp.get("player_name"),
                            "team": pp.get("team"),
                            "stat_category": cat,
                            "active_over_streak": new_streak,
                            "cold_flag": not hit_over,
                            "last_actual": actual_val,
                            "last_line": pp.get("line"),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }, merge=True)

                matchup_ref.set({
                    "status": "STATUS_FINAL",
                    "home_score": int(home_score),
                    "away_score": int(away_score),
                    "game_totals": graded_totals,
                    "player_props": graded_props,
                    "graded": True,
                    "graded_at": datetime.now(timezone.utc).isoformat(),
                }, merge=True)

    if not final_games_map:
        return 0

    for collection_name in ["ai_parlays", "user_parlays"]:
        pending_docs = db.collection(collection_name).where("status", "==", "PENDING").stream()
        for p_doc in pending_docs:
            pdata = p_doc.to_dict() or {}
            legs = pdata.get("legs", [])
            if not legs:
                continue

            all_legs_finished = True
            any_leg_lost = False
            lost_legs_details = []

            for leg in legs:
                gid = str(leg.get("game_id", ""))
                if gid not in final_games_map:
                    all_legs_finished = False
                    continue

                fgame = final_games_map[gid]
                sel = leg.get("selection_label", "")
                direction = "OVER" if "OVER" in sel.upper() else "UNDER"
                line_match = re.search(r"(\d+\.?\d*)", sel)
                line_val = float(line_match.group(1)) if line_match else 0.0

                actual_val: Optional[float] = None
                if leg.get("leg_type") == "FULL_GAME_TOTAL":
                    actual_val = fgame["full_total"]
                elif leg.get("leg_type") == "TEAM_TOTAL":
                    actual_val = fgame["home_score"] if leg.get("team") == fgame["home_team"] else fgame["away_score"]
                else:
                    for p_name, stats_dict in fgame["box_stats"].items():
                        if p_name in sel.lower():
                            for s_cat, s_val in stats_dict.items():
                                if s_cat.lower() in sel.lower():
                                    actual_val = s_val
                                    break

                if actual_val is not None:
                    leg_res = evaluate_single_prop_outcome(direction, line_val, actual_val)
                    leg["status"] = leg_res
                    leg["actual_value"] = actual_val
                    if leg_res == "LOST":
                        any_leg_lost = True
                        lost_legs_details.append(
                            f"{sel} (Actual: {actual_val}, Final Score: {fgame['away_team']} {int(fgame['away_score'])} @ {fgame['home_team']} {int(fgame['home_score'])})"
                        )

            if all_legs_finished:
                final_parlay_status = "LOST" if any_leg_lost else "WON"
                update_payload: Dict[str, Any] = {
                    "status": final_parlay_status,
                    "legs": legs,
                    "graded_at": datetime.now(timezone.utc).isoformat(),
                }

                if final_parlay_status == "LOST" and lost_legs_details:
                    try:
                        autopsy_prompt = (
                            f"Analyze this LOST NFL Parlay ({pdata.get('parlay_title') or pdata.get('name')}):\n"
                            f"Missed Legs:\n- " + "\n- ".join(lost_legs_details) + "\n\n"
                            "Diagnose the root cause from the final score and box score context, and formulate a concrete "
                            "self-learning adjustment rule."
                        )
                        autopsy_res = call_gemini_structured(
                            client=gemini_client,
                            prompt=autopsy_prompt,
                            schema=PostMortemAutopsySchema,
                            system_instruction=(
                                "You are a Principal Quantitative NFL Post-Mortem Analyst. Diagnose why high-probability "
                                "parlay legs missed and produce a strict mathematical/qualitative adjustment rule."
                            ),
                        )
                        update_payload["autopsy"] = autopsy_res
                        db.collection("model_learnings").add({
                            "parlay_name": pdata.get("parlay_title") or pdata.get("name", "Saved Parlay"),
                            "missed_legs": lost_legs_details,
                            "failure_headline": autopsy_res.get("failure_headline", ""),
                            "root_cause_category": autopsy_res.get("root_cause_category", ""),
                            "detailed_autopsy": autopsy_res.get("detailed_autopsy", ""),
                            "learned_adjustment_rule": autopsy_res.get("learned_adjustment_rule", ""),
                            "parameter_weight_delta": autopsy_res.get("parameter_weight_delta", ""),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        })
                        autopsies_created += 1
                    except Exception as exc:
                        logger.warning(f"Failed to generate autopsy for {p_doc.id}: {exc}")

                p_doc.reference.set(update_payload, merge=True)

    return autopsies_created


def purge_stale_weekly_matchups(db: firestore.Client, current_game_ids: Set[str]) -> int:
    """Removes previous weeks' matchup docs from weekly_matchups so only the current week shows."""
    deleted = 0
    for doc in db.collection("weekly_matchups").stream():
        if doc.id not in current_game_ids:
            doc.reference.delete()
            deleted += 1
    if deleted:
        logger.info(f"Purged {deleted} stale matchup document(s) from prior week(s).")
    return deleted


def load_recent_model_learnings(db: firestore.Client, limit: int = 10) -> List[Dict[str, Any]]:
    learnings: List[Dict[str, Any]] = []
    try:
        docs = (
            db.collection("model_learnings")
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
            .stream()
        )
        for d in docs:
            learnings.append(d.to_dict() or {})
    except Exception:
        pass

    if not learnings:
        default_rules = [
            {
                "failure_headline": "Outdoor Crosswind Passing Efficiency Drop",
                "root_cause_category": "WEATHER_VARIANCE",
                "detailed_autopsy": "Sustained crosswinds above 17 mph reduced deep air-yard completion rate by 19% and depressed full-game scoring.",
                "learned_adjustment_rule": "When outdoor sustained wind exceeds 17 mph, penalize QB Passing Yards Overs and Full Game Total Overs by -6.5% true probability.",
                "parameter_weight_delta": "Outdoor Wind Penalty >= 17mph: -6.5% Over Prob",
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            {
                "failure_headline": "4th Quarter Blowout Starter Withdrawal",
                "root_cause_category": "GAME_SCRIPT_BLOWOUT",
                "detailed_autopsy": "Heavy favorites leading by 17+ points in Q4 pulled primary WR1/QB usage in favor of backup clock-killing carries.",
                "learned_adjustment_rule": "Discount WR1 4th-quarter volume projections in games with spread > 9.5; favor RB1 Rushing Attempts Over instead.",
                "parameter_weight_delta": "Spread > 9.5 WR1 Volume Discount: -4.0% Over Prob",
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        ]
        for rule in default_rules:
            db.collection("model_learnings").add(rule)
        learnings = default_rules

    return learnings


def load_player_streaks_map(db: firestore.Client) -> Dict[str, Dict[str, Any]]:
    streaks: Dict[str, Dict[str, Any]] = {}
    try:
        for doc in db.collection("player_streaks").stream():
            streaks[doc.id] = doc.to_dict() or {}
    except Exception:
        pass
    return streaks


# =====================================================================
# MAIN PIPELINE EXECUTION
# =====================================================================

def main() -> None:
    start_time = time.time()
    logger.info("Starting NFL Player Props & Game Totals AI Analytics Pipeline...")
    db = init_firestore()
    gemini_client = init_gemini_client()

    force_week_raw = os.environ.get("FORCE_WEEK", "").strip()
    force_week = int(force_week_raw) if force_week_raw.isdigit() else None

    scoreboard = fetch_espn_scoreboard(week=force_week)
    season_year = int(scoreboard.get("season", {}).get("year", 2026))
    week_num = int(scoreboard.get("week", {}).get("number", 1))
    events = scoreboard.get("events", [])
    current_game_ids = {str(ev.get("id", "")) for ev in events if ev.get("id")}
    logger.info(f"Loaded ESPN Scoreboard: Season {season_year}, Week {week_num} ({len(events)} games)")

    if not events:
        raise RuntimeError("ESPN Scoreboard returned 0 games. Aborting to protect Firestore state.")

    # Step 1: Scan last 2 weeks of REAL completed NFL box scores (ground truth on who is playing!)
    recent_producers_by_team, prev_week_events = fetch_recent_onfield_producers(week_num, season_year)

    # Step 2: Grade completed games from BOTH prior week and current week (e.g., TNF)
    autopsies_count = grade_completed_games_and_run_autopsies(db, gemini_client, prev_week_events + events)
    logger.info(f"Post-game grading complete. New autopsies generated: {autopsies_count}")

    # Step 3: Load active AI self-learning rules, streaks, injuries, Polymarket, and RSS news
    learnings = load_recent_model_learnings(db, limit=10)
    learnings_text = "\n".join(
        [f"- [{L.get('root_cause_category')}] {L.get('learned_adjustment_rule')} ({L.get('parameter_weight_delta')})" for L in learnings]
    )
    streaks_map = load_player_streaks_map(db)
    league_injuries = fetch_espn_league_injuries()
    polymarket_markets = fetch_polymarket_nfl_markets()
    rss_articles = fetch_nfl_rss_intel()

    system_instruction = (
        "You are an elite Quantitative NFL Sportsbook Oddsmaker & Analytics Engine.\n"
        "CRITICAL NON-NEGOTIABLE RULES:\n"
        "1. Game Props MUST be strictly limited to: FULL_GAME_TOTAL (Over/Under), AWAY_TEAM_TOTAL (Over/Under), and HOME_TEAM_TOTAL (Over/Under).\n"
        "2. 100% ROSTER GROUNDING: Every player in `player_props` MUST be copied VERBATIM (`name`, `team`, `position`) from that matchup's `verified_active_players` list. "
        "NEVER pull players from memory, prior seasons, other teams, or Injured Reserve. Prioritize players who show actual `Wk X Actual` production in `verified_active_players`.\n"
        "3. Apply these ACTIVE SELF-LEARNING MODEL ADJUSTMENTS:\n"
        f"{learnings_text}\n"
        "4. If a player has `injury_tag` = 'Questionable' or a cold streak flag, reflect the risk in their intel badges and tier rating."
    )

    # Step 4: Build verified game packets using numeric ESPN team_id + game-specific injuries + real recent stats
    prepared_games: List[Dict[str, Any]] = []
    for ev in events:
        game_id = str(ev.get("id", ""))
        status_type = ev.get("status", {}).get("type", {}).get("name", "STATUS_SCHEDULED")
        kickoff_iso = ev.get("date", "")
        comp = ev.get("competitions", [{}])[0]
        competitors = comp.get("competitors", [])

        home_id, away_id = "", ""
        home_team, away_team = "HOME", "AWAY"
        home_name, away_name = "Home Team", "Away Team"
        home_record, away_record = "0-0", "0-0"
        home_score, away_score = 0, 0

        for c in competitors:
            t_obj = c.get("team", {})
            tid = str(t_obj.get("id", ""))
            abbr = t_obj.get("abbreviation", "UNK").upper()
            dname = t_obj.get("displayName", abbr)
            rec_list = c.get("records", [])
            rec_str = rec_list[0].get("summary", "0-0") if rec_list else "0-0"
            scr = int(float(c.get("score", 0)))
            if c.get("homeAway") == "home":
                home_id, home_team, home_name, home_record, home_score = tid, abbr, dname, rec_str, scr
            else:
                away_id, away_team, away_name, away_record, away_score = tid, abbr, dname, rec_str, scr

        weather_info = fetch_stadium_weather(home_team, kickoff_iso)
        summary_data = fetch_espn_game_summary(game_id)

        pickcenter = summary_data.get("pickcenter", [])
        espn_ou = float(pickcenter[0].get("overUnder", 45.5) or 45.5) if pickcenter else 45.5
        espn_spread = float(pickcenter[0].get("spread", -2.5) or -2.5) if pickcenter else -2.5

        # Merge game-specific official injury report + league injury report
        game_injuries = extract_game_summary_injuries(summary_data)
        home_inj_objs = game_injuries.get(home_team, []) + league_injuries.get(home_team, [])
        away_inj_objs = game_injuries.get(away_team, []) + league_injuries.get(away_team, [])

        # Deduplicate injury labels for display
        home_inj_labels = list(dict.fromkeys([x["label"] for x in home_inj_objs]))[:6]
        away_inj_labels = list(dict.fromkeys([x["label"] for x in away_inj_objs]))[:6]

        # Extract 2026 season leaders from summary
        leaders_by_team = extract_game_summary_leaders(summary_data)

        # Build verified active starters strictly from numeric team_id roster + recent box scores + injury exclusions
        home_verified = fetch_verified_team_starters(
            team_id=home_id,
            team_abbr=home_team,
            combined_injuries=home_inj_objs,
            recent_box_map=recent_producers_by_team.get(home_team, {}),
            season_leaders_map=leaders_by_team.get(home_team, {}),
        )
        away_verified = fetch_verified_team_starters(
            team_id=away_id,
            team_abbr=away_team,
            combined_injuries=away_inj_objs,
            recent_box_map=recent_producers_by_team.get(away_team, {}),
            season_leaders_map=leaders_by_team.get(away_team, {}),
        )

        logger.info(
            f"Prepared {away_team} @ {home_team} ({status_type}): "
            f"{len(away_verified)} {away_team} + {len(home_verified)} {home_team} verified active starters"
        )

        relevant_poly = [
            m for m in polymarket_markets
            if home_team.lower() in m["event_title"].lower()
            or away_team.lower() in m["event_title"].lower()
            or home_name.split()[-1].lower() in m["event_title"].lower()
            or away_name.split()[-1].lower() in m["event_title"].lower()
        ][:5]

        relevant_rss = [
            a["title"] for a in rss_articles
            if home_name.split()[-1].lower() in (a["title"] + " " + a["summary"]).lower()
            or away_name.split()[-1].lower() in (a["title"] + " " + a["summary"]).lower()
        ][:4]

        team_streaks = [s for s in streaks_map.values() if s.get("team") in (home_team, away_team)][:8]

        prepared_games.append({
            "game_id": game_id,
            "status": status_type,
            "kickoff_iso": kickoff_iso,
            "away_team": away_team,
            "away_name": away_name,
            "away_record": away_record,
            "away_score": away_score,
            "home_team": home_team,
            "home_name": home_name,
            "home_record": home_record,
            "home_score": home_score,
            "baseline_ou": espn_ou,
            "home_spread": espn_spread,
            "weather": weather_info,
            "away_injuries": away_inj_labels,
            "home_injuries": home_inj_labels,
            "verified_active_players": away_verified + home_verified,
            "rss_headlines": relevant_rss,
            "team_streaks": team_streaks,
            "polymarket_context": relevant_poly,
        })

    # Step 5: Fast Batched Synthesis (4 games per batch -> ~6-10s per call, zero timeouts!)
    all_candidate_legs: List[Dict[str, Any]] = []
    processed_games = 0
    batch_size = 4

    for i in range(0, len(prepared_games), batch_size):
        chunk = prepared_games[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(prepared_games) + batch_size - 1) // batch_size
        logger.info(f"Synthesizing Batch {batch_num}/{total_batches} ({len(chunk)} games)...")

        batch_prompt = (
            f"Analyze these {len(chunk)} NFL Season {season_year} Week {week_num} matchups.\n"
            "For EACH matchup (using its exact `game_id`), generate:\n"
            "1. Exactly 3 `game_totals` entries: FULL_GAME_TOTAL, AWAY_TEAM_TOTAL, and HOME_TEAM_TOTAL.\n"
            "2. Exactly 6 `player_props` entries (3 away + 3 home) covering top QB, RB, WR, TE skill players "
            "COPIED VERBATIM from that game's `verified_active_players` list (do NOT invent or include any player outside that list!). "
            "Base lines and intel badges directly on each player's `recent_production`, `injury_tag`, weather, and opponent matchup.\n\n"
            f"Matchup Data:\n{json.dumps(chunk)}"
        )

        try:
            batch_res = call_gemini_structured(
                client=gemini_client,
                prompt=batch_prompt,
                schema=BatchMatchupSynthesisSchema,
                system_instruction=system_instruction,
            )
            synthesized_map = {str(m["game_id"]): m for m in batch_res.get("matchups", [])}

            for g in chunk:
                gid = g["game_id"]
                syn = synthesized_map.get(gid)
                if not syn:
                    logger.warning(f"Missing synthesis for {g['away_team']} @ {g['home_team']} ({gid})")
                    continue

                # Hard Python Anti-Hallucination Firewall
                verified_lookup = {
                    normalize_name(p["name"]): p
                    for p in g["verified_active_players"]
                }

                verified_props = []
                for pp in syn.get("player_props", []):
                    p_norm = normalize_name(pp.get("player_name", ""))
                    matched_player = verified_lookup.get(p_norm)
                    if matched_player:
                        # Enforce exact verified team & position from ESPN live roster
                        pp["player_name"] = matched_player["name"]
                        pp["team"] = matched_player["team"]
                        pp["position"] = matched_player["position"]
                        verified_props.append(pp)
                    else:
                        logger.info(
                            f"BLOCKED unverified/IR/out-of-date player prop: "
                            f"{pp.get('player_name')} ({pp.get('team')}) in {g['away_team']} @ {g['home_team']}"
                        )

                matchup_doc = {
                    "game_id": gid,
                    "season": season_year,
                    "week": week_num,
                    "status": g["status"],
                    "kickoff_iso": g["kickoff_iso"],
                    "away_team": g["away_team"],
                    "away_name": g["away_name"],
                    "away_record": g["away_record"],
                    "away_score": g["away_score"],
                    "home_team": g["home_team"],
                    "home_name": g["home_name"],
                    "home_record": g["home_record"],
                    "home_score": g["home_score"],
                    "baseline_ou": g["baseline_ou"],
                    "weather": g["weather"],
                    "away_injuries": g["away_injuries"],
                    "home_injuries": g["home_injuries"],
                    "rss_headlines": g["rss_headlines"],
                    "matchup_headline": syn.get("matchup_headline", ""),
                    "game_totals": syn.get("game_totals", []),
                    "player_props": verified_props,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                db.collection("weekly_matchups").document(gid).set(matchup_doc)
                processed_games += 1

                # Only include upcoming/unstarted games in Final 5 Parlays
                if g["status"] != "STATUS_FINAL":
                    for gt in syn.get("game_totals", []):
                        if int(gt.get("tier", 3)) <= 2:
                            all_candidate_legs.append({
                                "leg_id": gt.get("market_id"),
                                "game_id": gid,
                                "matchup_label": f"{g['away_team']} @ {g['home_team']}",
                                "leg_type": gt.get("market_type"),
                                "selection_label": f"{gt.get('target_team')} {gt.get('direction')} {gt.get('line')} ({gt.get('market_type').replace('_', ' ')})",
                                "team": g["home_team"] if "HOME" in gt.get("market_type", "") else g["away_team"],
                                "true_prob": gt.get("true_prob"),
                                "polymarket_prob": gt.get("polymarket_prob"),
                                "ev_edge_pct": gt.get("ev_edge_pct"),
                                "tier": gt.get("tier"),
                                "intel": gt.get("intel_badges", []),
                            })

                    for pp in verified_props:
                        if int(pp.get("tier", 3)) <= 2:
                            all_candidate_legs.append({
                                "leg_id": pp.get("prop_id"),
                                "game_id": gid,
                                "matchup_label": f"{g['away_team']} @ {g['home_team']}",
                                "leg_type": "PLAYER_PROP",
                                "selection_label": f"{pp.get('player_name')} ({pp.get('team')}) {pp.get('direction')} {pp.get('line')} {pp.get('stat_category')}",
                                "team": pp.get("team"),
                                "true_prob": pp.get("true_prob"),
                                "polymarket_prob": pp.get("polymarket_prob"),
                                "ev_edge_pct": pp.get("ev_edge_pct"),
                                "tier": pp.get("tier"),
                                "intel": pp.get("intel_badges", []),
                            })

            if i + batch_size < len(prepared_games):
                time.sleep(4)
        except Exception as exc:
            logger.error(f"Batch {batch_num} failed: {exc}")

    # Fail loudly if zero games succeeded so GitHub Actions alerts you instead of silently succeeding
    if processed_games == 0:
        raise RuntimeError("Pipeline processed 0 games. Check Gemini API key/quotas above.")

    # Step 6: Purge stale games from prior weeks so ONLY this week's slate appears on the board
    purge_stale_weekly_matchups(db, current_game_ids)

    # Step 7: Construct the Auto-Generated "Final 5" Non-Crossover Parlays (upcoming games only)
    if len(all_candidate_legs) >= 10:
        logger.info(f"Building Final 5 Non-Crossover Parlays from {len(all_candidate_legs)} verified upcoming legs...")
        parlay_prompt = (
            f"Candidate Verified High-Confidence Upcoming Legs for NFL Week {week_num}:\n"
            f"{json.dumps(all_candidate_legs)}\n\n"
            "Construct strictly 5 optimal parlays (3 to 5 legs each) ranked 1 to 5 by combined hit probability and +EV edge.\n"
            "STRICT RULES:\n"
            "1. NON-CROSSOVER RULE: No single leg_id may appear in more than one of the 5 parlays.\n"
            "2. CORRELATION RULE: Never combine negatively correlated legs from the same game."
        )
        try:
            parlay_res = call_gemini_structured(
                client=gemini_client,
                prompt=parlay_prompt,
                schema=FinalFiveParlaysResponseSchema,
                system_instruction=system_instruction,
            )
            for p_item in parlay_res.get("parlays", []):
                doc_id = f"w{week_num}_parlay_{p_item.get('parlay_rank', 1)}"
                db.collection("ai_parlays").document(doc_id).set({
                    "week": week_num,
                    "season": season_year,
                    "parlay_rank": p_item.get("parlay_rank"),
                    "parlay_title": p_item.get("parlay_title"),
                    "legs": p_item.get("legs", []),
                    "combined_true_prob": p_item.get("combined_true_prob"),
                    "combined_polymarket_prob": p_item.get("combined_polymarket_prob"),
                    "net_ev_edge_pct": p_item.get("net_ev_edge_pct"),
                    "correlation_safety_note": p_item.get("correlation_safety_note"),
                    "status": "PENDING",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }, merge=True)
        except Exception as exc:
            logger.error(f"Failed to build Final 5 Parlays: {exc}")

    # Step 8: Update System Meta Status
    elapsed_total = round(time.time() - start_time, 1)
    db.collection("system_meta").document("pipeline_status").set({
        "last_run_utc": datetime.now(timezone.utc).isoformat(),
        "season": season_year,
        "current_week": week_num,
        "games_processed": processed_games,
        "active_learnings_count": len(learnings),
        "model_used": GEMINI_MODELS[0],
        "runtime_seconds": elapsed_total,
    }, merge=True)

    logger.info(f"Pipeline finished in {elapsed_total}s! Processed {processed_games}/{len(events)} Week {week_num} games.")


if __name__ == "__main__":
    main()
