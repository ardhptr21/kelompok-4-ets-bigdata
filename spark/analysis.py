from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
import logging


# logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("spark.analysis")


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
DASHBOARD_DATA_DIR = ROOT_DIR / "dashboard" / "data"
DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_RESULTS_PATH = DASHBOARD_DATA_DIR / "spark_results.json"
LOCAL_CONSUMER_DATA_DIR = DASHBOARD_DATA_DIR

HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/saham")
HDFS_API_PATH = f"{HDFS_BASE_PATH}/api"
HDFS_RSS_PATH = f"{HDFS_BASE_PATH}/rss"
HDFS_RESULT_PATH = f"{HDFS_BASE_PATH}/hasil"
DEFAULT_FS = os.getenv("SPARK_DEFAULT_FS", "hdfs://localhost:8020")

STOPWORDS = {"dan", "yang", "di", "ke", "dari", "untuk", "dengan", "pada", "atau", "the", "a", "an"}
COMPANY_TERMS = {
    "Bank Central Asia": ["bca", "bank central asia"],
    "Bank Rakyat Indonesia": ["bri", "bank rakyat indonesia"],
    "Telkom Indonesia": ["telkom", "telkom indonesia"],
    "Astra International": ["astra", "astra international"],
    "Bank Mandiri": ["mandiri", "bank mandiri"],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_spark() -> SparkSession:
    logger.info("Starting SparkSession with default FS=%s", DEFAULT_FS)
    return (
        SparkSession.builder.appName("SahamMeterAnalysis")
        .config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
        .getOrCreate()
    )


def read_json_folder(spark: SparkSession, hdfs_path: str, local_patterns: str | list[str]) -> DataFrame:
    logger.info("Attempting to read JSON from %s", hdfs_path)
    try:
        df = spark.read.option("multiLine", True).json(hdfs_path)
        logger.info("Loaded data from %s", hdfs_path)
        return df
    except Exception as exc:
        patterns = [local_patterns] if isinstance(local_patterns, str) else list(local_patterns)
        logger.warning("Failed to read %s (%s). Falling back to local patterns %s", hdfs_path, exc, patterns)
        local_files = []
        for pattern in patterns:
            local_files.extend(sorted(LOCAL_CONSUMER_DATA_DIR.glob(pattern)))
        if not local_files:
            logger.info("No local files found for patterns %s", patterns)
            return spark.createDataFrame([], "symbol STRING")
        # Spark may use HDFS as default FS; provide file:// URIs for local fallback
        local_uris = [file_path.resolve().as_uri() for file_path in local_files]
        logger.info("Reading local files: %s", local_uris)
        return spark.read.option("multiLine", True).json(local_uris)

def normalize_api(df: DataFrame) -> DataFrame:
    if df.rdd.isEmpty():
        return df
    return (
        df.withColumn("timestamp_ts", F.to_timestamp("timestamp"))
        .withColumn("price_current", F.col("price_current").cast("double"))
        .withColumn("price_open", F.col("price_open").cast("double"))
        .withColumn("price_high", F.col("price_high").cast("double"))
        .withColumn("price_low", F.col("price_low").cast("double"))
        .withColumn("volume", F.col("volume").cast("double"))
        .withColumn("change_24h_pct", F.col("change_24h_pct").cast("double"))
        .filter(F.col("symbol").isNotNull())
    )


def normalize_rss(df: DataFrame) -> DataFrame:
    if df.rdd.isEmpty():
        return df
    return df.withColumn("timestamp_ts", F.to_timestamp("timestamp")).filter(F.col("title").isNotNull())


def compute_stock_return(df: DataFrame) -> list[dict]:
    if df.rdd.isEmpty():
        return []
    window_asc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").asc())
    window_desc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").desc())
    start_df = (
        df.withColumn("rn", F.row_number().over(window_asc))
        .filter(F.col("rn") == 1)
        .select("symbol", F.col("price_current").alias("price_start"))
    )
    latest_df = (
        df.withColumn("rn", F.row_number().over(window_desc))
        .filter(F.col("rn") == 1)
        .select("symbol", F.col("price_current").alias("price_latest"))
    )
    summary = (
        start_df.join(latest_df, on="symbol", how="inner")
        .withColumn(
            "return_pct",
            F.when(
                F.col("price_start").isNull() | (F.col("price_start") == 0),
                F.lit(None),
            ).otherwise(((F.col("price_latest") - F.col("price_start")) / F.col("price_start")) * 100),
        )
        .orderBy(F.col("return_pct").desc_nulls_last())
    )
    return [row.asDict() for row in summary.collect()]


def compute_intraday_volatility(df: DataFrame) -> list[dict]:
    if df.rdd.isEmpty():
        return []
    summary = (
        df.groupBy("symbol")
        .agg(
            F.stddev_pop("price_current").alias("volatility_price_std"),
            F.avg("price_current").alias("avg_price"),
            F.count("*").alias("event_count"),
        )
        .orderBy(F.col("volatility_price_std").desc_nulls_last())
    )
    return [row.asDict() for row in summary.collect()]


def compute_hourly_summary_sql(spark: SparkSession) -> list[dict]:
    result = spark.sql(
        """
        SELECT
          HOUR(timestamp_ts) AS hour,
          ROUND(AVG(price_current), 2) AS avg_price,
          COUNT(*) AS event_count
        FROM api_events
        WHERE timestamp_ts IS NOT NULL AND price_current IS NOT NULL
        GROUP BY HOUR(timestamp_ts)
        ORDER BY hour
        """
    )
    return [row.asDict() for row in result.collect()]


def compute_news_mentions_sql(spark: SparkSession) -> list[dict]:
    word_rows = spark.sql(
        f"""
        SELECT LOWER(TRIM(word)) AS word
        FROM (
          SELECT explode(split(regexp_replace(coalesce(title, ''), '[^A-Za-z0-9\u00C0-\u024F ]', ' '), ' ')) AS word
          FROM rss_events
        )
        WHERE word IS NOT NULL AND word <> ''
          AND LENGTH(word) >= 4
          AND word NOT IN ({', '.join(repr(word) for word in sorted(STOPWORDS))})
        """
    )
    counts = word_rows.groupBy("word").count().orderBy(F.col("count").desc(), F.col("word").asc())
    return [row.asDict() for row in counts.limit(15).collect()]


def compute_company_mentions(df: DataFrame) -> list[dict]:
    if df.rdd.isEmpty():
        return []
    lower_title = F.lower(F.coalesce(F.col("title"), F.lit("")))
    lower_summary = F.lower(F.coalesce(F.col("summary"), F.lit("")))
    results: list[dict] = []
    for company, terms in COMPANY_TERMS.items():
        condition = F.lit(False)
        for term in terms:
            condition = condition | lower_title.contains(term) | lower_summary.contains(term)
        results.append({"company": company, "count": df.filter(condition).count()})
    return sorted(results, key=lambda item: item["count"], reverse=True)


def write_local_results(result: dict) -> None:
    with open(LOCAL_RESULTS_PATH, "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
    logger.info("Wrote local results to %s", LOCAL_RESULTS_PATH)


def write_hdfs_results(spark: SparkSession, result: dict) -> None:
    result_frame = spark.createDataFrame(
        [
            ("stock_return", json.dumps(result["stock_return"], ensure_ascii=False)),
            ("intraday_volatility", json.dumps(result["intraday_volatility"], ensure_ascii=False)),
            ("hourly_summary", json.dumps(result["hourly_summary"], ensure_ascii=False)),
            ("word_trends", json.dumps(result["word_trends"], ensure_ascii=False)),
            ("company_mentions", json.dumps(result["company_mentions"], ensure_ascii=False)),
        ],
        ["section", "payload"],
    )
    try:
        logger.info("Writing results to HDFS path %s", f"{HDFS_RESULT_PATH}/spark_results")
        result_frame.write.mode("overwrite").json(f"{HDFS_RESULT_PATH}/spark_results")
    except Exception as exc:
        logger.warning("Failed to write results to HDFS (%s). Writing fallback to local file.", exc)
        fallback_path = DASHBOARD_DATA_DIR / "spark_results_hdfs_fallback.json"
        with open(fallback_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)
        logger.info("Wrote fallback results to %s", fallback_path)


def main() -> None:
    logger.info("Running analysis main()")
    spark = build_spark()
    api_df = normalize_api(read_json_folder(spark, HDFS_API_PATH, ["api_*.json", "live_api.json"]))
    rss_df = normalize_rss(read_json_folder(spark, HDFS_RSS_PATH, ["rss_*.json", "live_rss.json"]))

    try:
        api_count = api_df.count() if not api_df.rdd.isEmpty() else 0
    except Exception:
        api_count = -1
    try:
        rss_count = rss_df.count() if not rss_df.rdd.isEmpty() else 0
    except Exception:
        rss_count = -1
    logger.info("API events: %s rows; RSS events: %s rows", api_count, rss_count)

    api_df.createOrReplaceTempView("api_events")
    rss_df.createOrReplaceTempView("rss_events")

    stock_return = compute_stock_return(api_df)
    intraday_volatility = compute_intraday_volatility(api_df)
    hourly_summary = compute_hourly_summary_sql(spark) if not api_df.rdd.isEmpty() else []
    word_trends = compute_news_mentions_sql(spark) if not rss_df.rdd.isEmpty() else []
    company_mentions = compute_company_mentions(rss_df)

    result = {
        "generated_at": utc_now_iso(),
        "stock_return": stock_return,
        "intraday_volatility": intraday_volatility,
        "hourly_summary": hourly_summary,
        "word_trends": word_trends,
        "company_mentions": company_mentions,
        "interpretation": {
            "stock_return": "Saham dengan return tertinggi menunjukkan momentum terkuat pada periode data yang tersedia.",
            "intraday_volatility": "Standar deviasi harga yang tinggi mengindikasikan saham lebih fluktuatif untuk dipantau.",
            "hourly_summary": "Rata-rata harga per jam membantu melihat pola intraday selama data terkumpul.",
            "word_trends": "Kata yang sering muncul di judul berita membantu mengidentifikasi tema pasar yang dominan.",
            "company_mentions": "Frekuensi sebutan emiten di berita dapat dipakai sebagai sinyal konteks sentimen pasar.",
        },
    }

    write_local_results(result)
    write_hdfs_results(spark, result)
    logger.info("Analysis completed; results written")
    spark.stop()


if __name__ == "__main__":
    main()
