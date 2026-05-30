"""
consumer.py — verify events are flowing through Kafka
Run this in a second terminal while producer.py is running.
"""

import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

TOPIC         = "lol-match-events"
KAFKA_BROKER  = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",       # only new messages
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="asiastats-consumer-dev",
)

log.info("Listening on topic: %s  (Ctrl+C to stop)", TOPIC)
log.info("%-8s %-12s %-30s %-15s %-6s", "SOURCE", "EVENT", "MATCH", "LEAGUE", "TIER")
log.info("─" * 80)

for msg in consumer:
    e = msg.value
    print(
        f"[{e.get('_source','?'):<4}] "
        f"{e.get('event_type','?'):<22} | "
        f"{e.get('match_name','?'):<28} | "
        f"{e.get('league_name','?'):<10} | "
        f"tier={e.get('tournament_tier','?')}"
    )
