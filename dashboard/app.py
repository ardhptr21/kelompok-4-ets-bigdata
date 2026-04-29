from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, jsonify, render_template


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SPARK_RESULTS_PATH = DATA_DIR / "spark_results.json"
SPARK_RESULTS_FALLBACK_PATH = DATA_DIR / "spark_results_hdfs_fallback.json"
LIVE_API_PATH = DATA_DIR / "live_api.json"
LIVE_RSS_PATH = DATA_DIR / "live_rss.json"

app = Flask(__name__)


def load_json(path: Path, default):
	if not path.exists():
		return default
	try:
		with open(path, "r", encoding="utf-8") as handle:
			return json.load(handle)
	except Exception:
		return default


def read_dashboard_payload() -> dict:
	spark_results = load_json(SPARK_RESULTS_PATH, {})
	if not spark_results:
		spark_results = load_json(SPARK_RESULTS_FALLBACK_PATH, {})
	return {
		"spark": spark_results,
		"live_api": load_json(LIVE_API_PATH, []),
		"live_rss": load_json(LIVE_RSS_PATH, []),
	}


@app.route("/")
def index():
	return render_template("index.html")


@app.route("/api/data")
def api_data():
	return jsonify(read_dashboard_payload())


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5000, debug=True)
