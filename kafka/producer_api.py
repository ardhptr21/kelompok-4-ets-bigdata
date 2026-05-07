from __future__ import annotations

import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from pathlib import Path
from datetime import datetime, timezone
import os
import json

import yfinance as yf
from kafka import KafkaProducer

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
	sys.path.insert(0, str(ROOT_DIR))

TICKERS = {
	"BBCA": "BBCA.JK",
	"BBRI": "BBRI.JK",
	"TLKM": "TLKM.JK",
	"ASII": "ASII.JK",
	"BMRI": "BMRI.JK",
}

COMPANY_NAMES = {
	"BBCA": "Bank Central Asia",
	"BBRI": "Bank Rakyat Indonesia",
	"TLKM": "Telkom Indonesia",
	"ASII": "Astra International",
	"BMRI": "Bank Mandiri",
}

DEFAULT_BOOTSTRAP     = "localhost:9092"
DEFAULT_TOPIC         = "saham-api"
POLL_INTERVAL_SECONDS = 60


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


def log_topic_status(topic: str, sent_count: int) -> None:
	print(f"Sent {sent_count} items to {topic}", flush=True)

def safe_float(value: Any) -> float | None:
	try:
		if value is None:
			return None
		return float(value)
	except (TypeError, ValueError):
		return None


def simulator_snapshot(symbol: str) -> dict[str, Any]:
	base_price = 8000 + (sum(ord(char) for char in symbol) % 3500)
	current_price = base_price + random.uniform(-120, 120)
	open_price = current_price - random.uniform(-80, 80)
	previous_close = current_price - random.uniform(-100, 100)
	change_pct = ((current_price - previous_close) / previous_close) * 100 if previous_close else None
	return {
		"symbol": symbol,
		"ticker": TICKERS[symbol],
		"company_name": COMPANY_NAMES[symbol],
		"price_current": round(current_price, 2),
		"price_open": round(open_price, 2),
		"price_high": round(current_price + random.uniform(5, 120), 2),
		"price_low": round(current_price - random.uniform(5, 120), 2),
		"volume": int(random.randint(500_000, 7_500_000)),
		"previous_close": round(previous_close, 2),
		"change_24h_pct": round(change_pct, 4) if change_pct is not None else None,
		"source": "simulator",
		"timestamp": utc_now_iso(),
	}


def fetch_ticker_snapshot(symbol: str, yahoo_symbol: str) -> dict[str, Any]:
	ticker = yf.Ticker(yahoo_symbol)
	try:
		fast_info = getattr(ticker, "fast_info", {}) or {}
		current_price = safe_float(
			fast_info.get("lastPrice")
			or fast_info.get("regularMarketPrice")
			or fast_info.get("previousClose")
		)
		open_price = safe_float(fast_info.get("open") or fast_info.get("previousClose"))
		high_price = safe_float(fast_info.get("dayHigh") or fast_info.get("regularMarketDayHigh"))
		low_price = safe_float(fast_info.get("dayLow") or fast_info.get("regularMarketDayLow"))
		previous_close = safe_float(fast_info.get("previousClose"))
		volume = safe_float(fast_info.get("volume") or fast_info.get("regularMarketVolume"))

		history = ticker.history(period="5d", interval="1d", auto_adjust=False)
		if not history.empty:
			latest_row = history.iloc[-1]
			if current_price is None:
				current_price = safe_float(latest_row.get("Close"))
			if open_price is None:
				open_price = safe_float(latest_row.get("Open"))
			if high_price is None:
				high_price = safe_float(latest_row.get("High"))
			if low_price is None:
				low_price = safe_float(latest_row.get("Low"))
			if previous_close is None and len(history) > 1:
				previous_close = safe_float(history.iloc[-2].get("Close"))
			if volume is None:
				volume = safe_float(latest_row.get("Volume"))

		if current_price is None:
			return simulator_snapshot(symbol)

		change_pct = None
		if previous_close not in (None, 0):
			change_pct = ((current_price - previous_close) / previous_close) * 100

		return {
			"symbol": symbol,
			"ticker": yahoo_symbol,
			"company_name": COMPANY_NAMES[symbol],
			"price_current": round(float(current_price), 2) if current_price is not None else None,
			"price_open": round(float(open_price), 2) if open_price is not None else None,
			"price_high": round(float(high_price), 2) if high_price is not None else None,
			"price_low": round(float(low_price), 2) if low_price is not None else None,
			"volume": int(volume) if volume is not None else None,
			"previous_close": round(float(previous_close), 2) if previous_close is not None else None,
			"change_24h_pct": round(float(change_pct), 4) if change_pct is not None else None,
			"source": "yfinance",
			"timestamp": utc_now_iso(),
		}
	except Exception:
		return simulator_snapshot(symbol)


def send_snapshot(producer: KafkaProducer) -> int:
	with ThreadPoolExecutor(max_workers=len(TICKERS)) as executor:
		futures = [executor.submit(fetch_ticker_snapshot, symbol, yahoo_symbol) for symbol, yahoo_symbol in TICKERS.items()]
		snapshots = [future.result() for future in futures]

	sent_count = 0
	for payload in snapshots:
		symbol = payload["symbol"]
		producer.send(DEFAULT_TOPIC, key=symbol, value=payload)
		sent_count += 1
	producer.flush()
	return sent_count


def log_interval_status(sent_count: int) -> None:
	log_topic_status(DEFAULT_TOPIC, sent_count)


def main() -> None:
	producer = build_producer()
	print(f"Sending API data to {DEFAULT_TOPIC} via {DEFAULT_BOOTSTRAP}")
	while True:
		sent_count = send_snapshot(producer)
		log_interval_status(sent_count)
		time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
	main()
