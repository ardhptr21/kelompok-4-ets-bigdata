from __future__ import annotations

import json
import os
from pathlib import Path

from flask import Flask, jsonify, render_template, send_from_directory
import yfinance as yf



BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ASSETS_DIR = BASE_DIR / "assets"
SPARK_RESULTS_PATH = DATA_DIR / "spark_results.json"
SPARK_RESULTS_FALLBACK_PATH = DATA_DIR / "spark_results_hdfs_fallback.json"
LIVE_API_PATH = DATA_DIR / "live_api.json"
LIVE_RSS_PATH = DATA_DIR / "live_rss.json"

ENABLE_HDFS_REMOTE = os.getenv("ENABLE_HDFS_REMOTE", "").lower() in {"1", "true", "yes"}

app = Flask(__name__)

SYMBOL_TO_TICKER = {
	"BBCA": "BBCA.JK",
	"BBRI": "BBRI.JK",
	"TLKM": "TLKM.JK",
	"ASII": "ASII.JK",
	"BMRI": "BMRI.JK",
}

SYMBOL_TO_COMPANY = {
	"BBCA": "Bank Central Asia",
	"BBRI": "Bank Rakyat Indonesia",
	"TLKM": "Telkom Indonesia",
	"ASII": "Astra International",
	"BMRI": "Bank Mandiri",
}


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
	payload = read_dashboard_payload()
	payload["mode"] = "HDFS" if ENABLE_HDFS_REMOTE else "Local-first"
	return jsonify(payload)


@app.route("/api/status")
def api_status():
	return jsonify({"mode": "HDFS" if ENABLE_HDFS_REMOTE else "Local-first"})


@app.route("/assets/<path:filename>")
def assets(filename: str):
	return send_from_directory(ASSETS_DIR, filename)


@app.route("/stock/<symbol>")
def stock_detail(symbol: str):
	symbol = symbol.upper()
	return render_template("detail.html", symbol=symbol)


@app.route("/api/stock/<symbol>")
def api_stock_detail(symbol: str):
	symbol = symbol.upper()
	ticker = SYMBOL_TO_TICKER.get(symbol, symbol)
	company = SYMBOL_TO_COMPANY.get(symbol, symbol)
	try:
		df = yf.download(ticker, period="1d", interval="5m", progress=False)
		df = df.dropna()
		if df.empty:
			return jsonify({"symbol": symbol, "ticker": ticker, "company": company, "prices": [], "volumes": []})
		if hasattr(df.columns, "levels"):
			close_series = df["Close"][ticker]
			volume_series = df["Volume"][ticker]
			open_series = df["Open"][ticker]
			high_series = df["High"][ticker]
			low_series = df["Low"][ticker]
		else:
			close_series = df["Close"]
			volume_series = df["Volume"]
			open_series = df["Open"]
			high_series = df["High"]
			low_series = df["Low"]

		timestamps = [ts.to_pydatetime().isoformat() for ts in df.index]
		prices = [float(value) for value in close_series.tolist()]
		volumes = [int(value) for value in volume_series.fillna(0).tolist()]
		open_price = float(open_series.iloc[0])
		close_price = float(close_series.iloc[-1])
		high_price = float(high_series.max())
		low_price = float(low_series.min())
		change_pct = ((close_price - open_price) / open_price * 100) if open_price else 0.0
		return jsonify(
			{
				"symbol": symbol,
				"ticker": ticker,
				"company": company,
				"timestamps": timestamps,
				"prices": prices,
				"volumes": volumes,
				"open": open_price,
				"close": close_price,
				"high": high_price,
				"low": low_price,
				"change_pct": change_pct,
			}
		)
	except Exception as exc:
		return jsonify({"symbol": symbol, "ticker": ticker, "company": company, "error": str(exc)})


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=5000, debug=True)
