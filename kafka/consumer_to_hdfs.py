from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any

from kafka import KafkaConsumer

try:
	from hdfs import InsecureClient
except Exception:  # pragma: no cover - optional dependency fallback
	InsecureClient = None


API_TOPIC          = "saham-api"
RSS_TOPIC          = "saham-rss"
BOOTSTRAP          = "localhost:9092"
GROUP_ID           = "saham-to-hdfs"
FLUSH_SECONDS      = 1
HDFS_BASE          = "/data/saham"
HDFS_NAMENODE_HOST = "localhost"
HDFS_WEB_PORT      = 9870
HDFS_USER          = "hadoop"
HDFS_NAMENODE_PORT = 8020
ENABLE_HDFS_REMOTE = True

LOCAL_DATA_DIR = Path("dashboard/data")
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

_HDFS_REMOTE_DISABLED = False
_HDFS_DIRS_ATTEMPTED  = False


def disable_hdfs_remote() -> None:
	global _HDFS_REMOTE_DISABLED
	_HDFS_REMOTE_DISABLED = True


def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def timestamp_label() -> str:
	return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def resolved_hdfs_web_url() -> str:
	return f"http://{HDFS_NAMENODE_HOST}:{HDFS_WEB_PORT}"


def build_consumer(topic: str, group_suffix: str) -> KafkaConsumer:
	return KafkaConsumer(
		topic,
		bootstrap_servers=BOOTSTRAP,
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
		key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
		group_id=f"{GROUP_ID}-{group_suffix}",
		fetch_max_wait_ms=1000,
	)


def ensure_hdfs_dirs() -> None:
	global _HDFS_DIRS_ATTEMPTED
	if not ENABLE_HDFS_REMOTE or _HDFS_REMOTE_DISABLED or _HDFS_DIRS_ATTEMPTED:
		return
	client = hdfs_client()
	if client is None:
		return
	for suffix in ("api", "rss", "hasil"):
		target = f"{HDFS_BASE}/{suffix}"
		try:
			client.makedirs(target)
		except Exception as exc:
			disable_hdfs_remote()
			print(f"Warning: HDFS makedirs failed ({exc}); using local fallback only.", flush=True)
			return
	_HDFS_DIRS_ATTEMPTED = True


def hdfs_client() -> InsecureClient | None: # type: ignore
	global _HDFS_REMOTE_DISABLED
	if _HDFS_REMOTE_DISABLED:
		return None
	if InsecureClient is None:
		return None
	try:
		return InsecureClient(resolved_hdfs_web_url(), user=HDFS_USER)
	except Exception:
		_HDFS_REMOTE_DISABLED = True
		return None


def write_local_snapshot(topic_suffix: str, payload: list[dict[str, Any]]) -> Path:
	file_path = LOCAL_DATA_DIR / f"live_{topic_suffix}.json"
	with open(file_path, "w", encoding="utf-8") as handle:
		json.dump(payload, handle, ensure_ascii=False, indent=2)
	return file_path


def upload_to_hdfs(topic_suffix: str, payload: list[dict[str, Any]]) -> None:
	global _HDFS_REMOTE_DISABLED
	if not ENABLE_HDFS_REMOTE:
		return
	ensure_hdfs_dirs()
	hdfs_target_dir = f"{HDFS_BASE}/{topic_suffix}"
	if _HDFS_REMOTE_DISABLED:
		return

	client = hdfs_client()
	if client is None:
		return

	snapshot_name = f"{timestamp_label()}.json"
	hdfs_target_path = f"{hdfs_target_dir}/{snapshot_name}"
	print(
		f"Attempting HDFS upload for {topic_suffix}: {hdfs_target_path} ({len(payload)} records)",
		flush=True,
	)
	try:
		client.makedirs(hdfs_target_dir)
		client.write(
			hdfs_target_path,
			data=json.dumps(payload, ensure_ascii=False, indent=2),
			overwrite=True,
			encoding="utf-8",
		)
		print(f"Uploaded {topic_suffix} snapshot to HDFS: {hdfs_target_path}", flush=True)
	except Exception as exc:
		_HDFS_REMOTE_DISABLED = True
		print(
			f"Warning: HDFS upload failed for {topic_suffix} ({hdfs_target_path}): {exc}; using local fallback only.",
			flush=True,
		)


def consume_topic(topic: str, topic_suffix: str, queue: Queue[dict[str, Any]]) -> None:
	consumer = build_consumer(topic, topic_suffix)
	for message in consumer:
		record = dict(message.value)
		record["topic"] = topic_suffix
		record["key"] = message.key
		record["consumed_at"] = utc_now_iso()
		queue.put(
			{
				"topic": topic_suffix,
				"record": record,
			}
		)


def flush_buffers(buffers: dict[str, list[dict[str, Any]]]) -> None:
	for suffix, items in list(buffers.items()):
		if not items:
			continue
		payload = list(items)
		buffers[suffix].clear()
		write_local_snapshot(suffix, payload)
		upload_to_hdfs(suffix, payload)


def main() -> None:
	print(
		"HDFS mode: "
		f"{'enabled' if ENABLE_HDFS_REMOTE else 'disabled'}; "
		f"endpoint={resolved_hdfs_web_url() if ENABLE_HDFS_REMOTE else 'local fallback only'}; "
		f"namenode={HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}",
		flush=True,
	)
	ensure_hdfs_dirs()
	queue: Queue[dict[str, Any]] = Queue()
	buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)

	threads = [
		threading.Thread(target=consume_topic, args=(API_TOPIC, "api", queue), daemon=True),
		threading.Thread(target=consume_topic, args=(RSS_TOPIC, "rss", queue), daemon=True),
	]
	for thread in threads:
		thread.start()

	print(f"Consuming {API_TOPIC} and {RSS_TOPIC} from {BOOTSTRAP}")
	last_flush = time.time()

	while True:
		try:
			item = queue.get(timeout=1)
			buffers[item["topic"]].append(item["record"])
			write_local_snapshot(item["topic"], buffers[item["topic"]])
			if len(buffers[item["topic"]]) >= 50:
				payload = list(buffers[item["topic"]])
				buffers[item["topic"]].clear()
				write_local_snapshot(item["topic"], payload)
				upload_to_hdfs(item["topic"], payload)
		except Empty:
			pass

		if time.time() - last_flush >= FLUSH_SECONDS:
			flush_buffers(buffers)
			last_flush = time.time()


if __name__ == "__main__":
	main()
