from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer


def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def build_producer(bootstrap_servers: str | None = None) -> KafkaProducer:
	return KafkaProducer(
		bootstrap_servers=bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		acks="all",
		retries=10,
		linger_ms=10,
		enable_idempotence=True,
		value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
		key_serializer=lambda value: value.encode("utf-8") if isinstance(value, str) else value,
	)


def log_interval_status(topic: str, sent_count: int) -> None:
	print(f"Sent {sent_count} items to {topic}", flush=True)
