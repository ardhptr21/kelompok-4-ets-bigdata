from __future__ import annotations

import json
import os
import subprocess
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


API_TOPIC = os.getenv("API_TOPIC", "saham-api")
RSS_TOPIC = os.getenv("RSS_TOPIC", "saham-rss")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "saham-to-hdfs")
FLUSH_SECONDS = int(os.getenv("CONSUMER_FLUSH_SECONDS", "10"))
HDFS_BASE = os.getenv("HDFS_BASE_PATH", "/data/saham")
HDFS_WEB_URL = os.getenv("HDFS_WEB_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_NAMENODE_HOST = os.getenv("HDFS_NAMENODE_HOST", "localhost")
HDFS_NAMENODE_PORT = os.getenv("HDFS_NAMENODE_PORT", "8020")
ENABLE_HDFS_REMOTE = os.getenv("ENABLE_HDFS_REMOTE", "").lower() in {"1", "true", "yes"}
LOCAL_DATA_DIR = Path(os.getenv("LOCAL_DATA_DIR", "dashboard/data"))
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

_HDFS_REMOTE_DISABLED = False
_HDFS_DIRS_ATTEMPTED = False


def disable_hdfs_remote() -> None:
	global _HDFS_REMOTE_DISABLED
	_HDFS_REMOTE_DISABLED = True


def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def timestamp_label() -> str:
	return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def build_consumer(topic: str, group_suffix: str) -> KafkaConsumer:
	return KafkaConsumer(
		topic,
		bootstrap_servers=BOOTSTRAP,
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
		key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
		group_id=f"{GROUP_ID}-{group_suffix}",
		consumer_timeout_ms=1000,
		fetch_max_wait_ms=1000,
	)


def ensure_hdfs_dirs() -> None:
	global _HDFS_DIRS_ATTEMPTED
	if not ENABLE_HDFS_REMOTE or _HDFS_REMOTE_DISABLED or _HDFS_DIRS_ATTEMPTED:
		return
	for suffix in ("api", "rss", "hasil"):
		target = f"{HDFS_BASE}/{suffix}"
		full_target = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}{target}"
		try:
			# prefer the Hadoop-style wrapper `hdfs dfs -mkdir -p`
			proc = subprocess.run(["hdfs", "dfs", "-mkdir", "-p", full_target], capture_output=True, text=True)
			stderr = (proc.stderr or "").lower()
			if proc.returncode != 0 and "unknown command: dfs" in stderr:
				# busybox-style `hdfs` uses `mkdir -p` directly
				proc = subprocess.run(["hdfs", "mkdir", "-p", full_target], capture_output=True, text=True)
				stderr = (proc.stderr or "").lower()
			if proc.returncode != 0:
				if "permission denied" in stderr:
					disable_hdfs_remote()
				return
		except FileNotFoundError:
			# `hdfs` command not available on host (dev/demo). Allow local fallback.
			return
	_HDFS_DIRS_ATTEMPTED = True


def hdfs_client() -> InsecureClient | None:
	global _HDFS_REMOTE_DISABLED
	if _HDFS_REMOTE_DISABLED:
		return None
	if InsecureClient is None:
		return None
	try:
		return InsecureClient(HDFS_WEB_URL, user=HDFS_USER)
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
	snapshot_name = f"{timestamp_label()}.json"
	hdfs_target_dir = f"{HDFS_BASE}/{topic_suffix}"
	local_file = LOCAL_DATA_DIR / f"{topic_suffix}_{snapshot_name}"
	with open(local_file, "w", encoding="utf-8") as handle:
		json.dump(payload, handle, ensure_ascii=False, indent=2)

	if _HDFS_REMOTE_DISABLED:
		return

	client = hdfs_client()
	if client is not None:
		try:
			client.makedirs(hdfs_target_dir)
			with open(local_file, "rb") as handle:
				client.write(f"{hdfs_target_dir}/{snapshot_name}", handle, overwrite=True)
			return
		except Exception as exc:
			_HDFS_REMOTE_DISABLED = True
			# HDFS unreachable (datanode hostname, network, etc.); rely on local files.
			print(f"Warning: HDFS client failed ({exc}); using local fallback only.", flush=True)
			return

	if _HDFS_REMOTE_DISABLED:
		return

	# Try subprocess hdfs commands; support both 'hdfs dfs -put' and 'hdfs put'
	# use explicit namenode host:port to avoid unresolved 'namenode' hostname
	full_target = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}{hdfs_target_dir}/{snapshot_name}"
	try:
		proc = subprocess.run(["hdfs", "dfs", "-put", "-f", str(local_file), full_target], capture_output=True, text=True)
		if proc.returncode != 0 and "Unknown command: dfs" in proc.stderr:
			# fallback to busybox-like `hdfs put` (no -f flag)
			subprocess.run(["hdfs", "put", str(local_file), full_target], check=False)
	except FileNotFoundError:
		# hdfs CLI missing; rely on local snapshot written to `dashboard/data/` instead.
		return


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
