from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import feedparser

from kafka import KafkaProducer

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
	sys.path.insert(0, str(ROOT_DIR))

from producer_common import build_producer, log_interval_status as log_topic_status, utc_now_iso


RSS_FEEDS = [
	os.getenv("RSS_FEED_PRIMARY", "https://rss.bisnis.com/feed/rss2/financial-market"),
	os.getenv("RSS_FEED_BACKUP", "https://www.cnnindonesia.com/ekonomi/rss"),
]

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("RSS_TOPIC", "saham-rss")
POLL_INTERVAL_SECONDS = int(os.getenv("RSS_POLL_INTERVAL_SECONDS", "300"))
STATE_FILE = os.getenv("RSS_STATE_FILE", ".rss_seen_ids.json")


def load_seen_ids() -> set[str]:
	if not os.path.exists(STATE_FILE):
		return set()
	try:
		with open(STATE_FILE, "r", encoding="utf-8") as handle:
			return set(json.load(handle))
	except Exception:
		return set()


def save_seen_ids(seen_ids: set[str]) -> None:
	with open(STATE_FILE, "w", encoding="utf-8") as handle:
		json.dump(sorted(seen_ids), handle, ensure_ascii=False, indent=2)


def event_key_from_url(url: str) -> str:
	return hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]


def extract_entries() -> list[dict[str, str]]:
	items: list[dict[str, str]] = []
	for feed_url in RSS_FEEDS:
		parsed = feedparser.parse(feed_url)
		for entry in parsed.entries:
			url = entry.get("link") or entry.get("id") or ""
			if not url:
				continue
			items.append(
				{
					"title": entry.get("title", ""),
					"link": url,
					"summary": entry.get("summary", ""),
					"published": entry.get("published", entry.get("updated", "")),
					"source": parsed.feed.get("title", feed_url),
				}
			)
	return items


def build_payload(entry: dict[str, str]) -> dict[str, str]:
	return {
		"title": entry["title"],
		"link": entry["link"],
		"summary": entry["summary"],
		"published": entry["published"],
		"source": entry["source"],
		"item_id": event_key_from_url(entry["link"]),
		"timestamp": utc_now_iso(),
	}


def send_snapshot(producer: KafkaProducer, seen_ids: set[str]) -> int:
	new_items = 0
	try:
		entries = extract_entries()
		for entry in entries:
			payload = build_payload(entry)
			dedupe_key = payload["item_id"]
			if dedupe_key in seen_ids:
				continue
			seen_ids.add(dedupe_key)
			producer.send(DEFAULT_TOPIC, key=dedupe_key, value=payload)
			new_items += 1
		if new_items:
			producer.flush()
			save_seen_ids(seen_ids)
	except Exception:
		fallback_link = "https://www.idx.co.id/"
		payload = {
			"title": "Pasar modal bergerak aktif menjelang penutupan",
			"link": fallback_link,
			"summary": "Simulator RSS aktif saat membaca feed gagal.",
			"published": utc_now_iso(),
			"source": "simulator",
			"item_id": event_key_from_url(fallback_link),
			"timestamp": utc_now_iso(),
		}
		if payload["item_id"] not in seen_ids:
			seen_ids.add(payload["item_id"])
			producer.send(DEFAULT_TOPIC, key=payload["item_id"], value=payload)
			producer.flush()
			save_seen_ids(seen_ids)
			new_items = 1
	return new_items


def log_interval_status(new_items: int) -> None:
	log_topic_status(DEFAULT_TOPIC, new_items)


def main() -> None:
	producer = build_producer()
	seen_ids = load_seen_ids()
	print(f"Sending RSS data to {DEFAULT_TOPIC} via {DEFAULT_BOOTSTRAP}")

	while True:
		new_items = send_snapshot(producer, seen_ids)
		log_interval_status(new_items)
		time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
	main()
