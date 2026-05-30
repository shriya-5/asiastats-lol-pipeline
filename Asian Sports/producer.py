"""
producer.py — AsiaStats Live Kafka Producer
Polls Pandascore API every 30s for real LoL match events
and streams them to Kafka topic: lol-match-events
"""

import json
import time
import uuid
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "lol-match-events"
API_KEY       = "ygsPj9hioceu6V8Npsyhvqy2Q3wwcdzITKVyN1PJFrtLlQALyRo"
  # ← paste your key here
POLL_INTERVAL = 30   # seconds between API polls

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ── Kafka producer ────────────────────────────────────────────────────
def make_producer() -> KafkaProducer:
    retries = 0
    while retries < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            retries += 1
            log.warning("Kafka not ready, retrying in 3s... (%d/10)", retries)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka. Is Docker running?")


# ── API fetch ─────────────────────────────────────────────────────────
def fetch_matches(status: str) -> list:
    try:
        r = requests.get(
            "https://api.pandascore.co/lol/matches",
            headers={"Authorization": f"Bearer {API_KEY}"},
            params={
                "filter[status]": status,
                "sort": "-begin_at",
                "per_page": 100,
            },
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
        log.warning("API returned %d for status=%s", r.status_code, status)
    except requests.RequestException as e:
        log.error("API request failed: %s", e)
    return []


# ── Event builder ─────────────────────────────────────────────────────
def build_event(match: dict, event_type: str) -> dict:
    opponents = match.get("opponents", [])
    teams     = [o.get("opponent", {}) for o in opponents if isinstance(o, dict)]
    results   = match.get("results", [])

    return {
        "event_id":             str(uuid.uuid4()),
        "event_type":           event_type,
        "match_id":             match.get("id"),
        "match_name":           match.get("name"),
        "match_status":         match.get("status"),
        "match_type":           match.get("match_type"),
        "number_of_games":      match.get("number_of_games"),
        "begin_at":             match.get("begin_at"),
        "end_at":               match.get("end_at"),
        "league_name":          match.get("league", {}).get("name"),
        "league_id":            match.get("league", {}).get("id"),
        "tournament_name":      match.get("tournament", {}).get("name"),
        "tournament_tier":      match.get("tournament", {}).get("tier"),
        "tournament_region":    match.get("tournament", {}).get("region"),
        "serie_name":           match.get("serie", {}).get("full_name"),
        "game_title":           "LoL",
        "teams": [
            {"id": t.get("id"), "name": t.get("name"), "acronym": t.get("acronym")}
            for t in teams
        ],
        "results":              results,
        "games_count":          len(match.get("games", [])),
        "avg_game_length_mins": None,   # populated after match finishes
        "simulated":            False,
        "_source":              "LIVE",
        "_ingested_at":         datetime.now(timezone.utc).isoformat(),
    }


# ── Main loop ─────────────────────────────────────────────────────────
def run():
    producer  = make_producer()
    seen_ids  = set()   # track finished matches already sent

    log.info("Producer running — polling every %ds | topic: %s", POLL_INTERVAL, TOPIC)

    while True:
        total = 0

        # Running matches — always emit (state changes every poll)
        for match in fetch_matches("running"):
            event = build_event(match, "match_started")
            key   = str(match.get("id", uuid.uuid4()))
            producer.send(TOPIC, key=key, value=event)
            log.info("LIVE [running]  → %s | league: %s | tier: %s",
                     event["match_name"], event["league_name"], event["tournament_tier"])
            total += 1

        # Finished matches — emit once per match
        for match in fetch_matches("finished"):
            mid = match.get("id")
            if mid in seen_ids:
                continue
            event = build_event(match, "match_finished")
            key   = str(mid)
            producer.send(TOPIC, key=key, value=event)
            log.info("LIVE [finished] → %s | league: %s | tier: %s",
                     event["match_name"], event["league_name"], event["tournament_tier"])
            seen_ids.add(mid)
            total += 1

        producer.flush()
        log.info("Poll complete — %d events sent | sleeping %ds", total, POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Stopped.")