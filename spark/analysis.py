from __future__ import annotations

import json
import os
import sys
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from delta import configure_spark_with_delta_pip
import logging


# logging
LOG_LEVEL = "INFO"
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("spark.analysis")


BASE_DIR                = Path(__file__).resolve().parent
ROOT_DIR                = BASE_DIR.parent
DASHBOARD_DATA_DIR      = ROOT_DIR / "dashboard" / "data"
DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_RESULTS_PATH      = DASHBOARD_DATA_DIR / "spark_results.json"
LOCAL_CONSUMER_DATA_DIR = DASHBOARD_DATA_DIR

HDFS_BASE_PATH   = "/data/saham"
HDFS_API_PATH    = f"{HDFS_BASE_PATH}/api"
HDFS_RSS_PATH    = f"{HDFS_BASE_PATH}/rss"
HDFS_RESULT_PATH = f"{HDFS_BASE_PATH}/hasil"
DEFAULT_FS       = "hdfs://localhost:8020"
HDFS_USER        = "hadoop"
os.environ.setdefault("HADOOP_USER_NAME", HDFS_USER)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

STOPWORDS = {"dan", "yang", "di", "ke", "dari", "untuk", "dengan", "pada", "atau", "the", "a", "an"}
COMPANY_TERMS = {
    "Bank Central Asia": ["bca", "bank central asia"],
    "Bank Rakyat Indonesia": ["bri", "bank rakyat indonesia"],
    "Telkom Indonesia": ["telkom", "telkom indonesia"],
    "Astra International": ["astra", "astra international"],
    "Bank Mandiri": ["mandiri", "bank mandiri"],
}


def build_term_pattern(terms: list[str]) -> str:
    escaped_terms = [re.escape(term.lower()) for term in terms if term]
    if not escaped_terms:
        return r"$^"
    return rf"(?i)(?:\b{'|\b'.join(escaped_terms)}\b)"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_delta_with_fallback(spark: SparkSession, hdfs_path: str, local_path: Path) -> DataFrame:
    try:
        return spark.read.format("delta").load(hdfs_path)
    except Exception as exc:
        logger.warning("Failed to read Delta from %s: %s. Falling back to local: %s", hdfs_path, exc, local_path)
        return spark.read.format("delta").load(str(local_path))


def build_spark() -> SparkSession:
    logger.info("Starting SparkSession with default FS=%s", DEFAULT_FS)
    builder = (
        SparkSession.builder.appName("SahamMeterAnalysis")
        .config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


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
        df.withColumn("timesjtamp_ts", F.to_timestamp("timestamp"))
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
    window_asc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").asc(), F.col("price_current").asc())
    window_desc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").desc(), F.col("price_current").desc())
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
        term_pattern = build_term_pattern(terms)
        condition = lower_title.rlike(term_pattern) | lower_summary.rlike(term_pattern)
        results.append({"company": company, "count": df.filter(condition).count()})
    return sorted(results, key=lambda item: item["count"], reverse=True)


def compute_company_hourly_mentions(df: DataFrame, company: str) -> list[dict]:
    terms = COMPANY_TERMS.get(company, [])
    if df.rdd.isEmpty() or not terms:
        return []

    lower_title = F.lower(F.coalesce(F.col("title"), F.lit("")))
    lower_summary = F.lower(F.coalesce(F.col("summary"), F.lit("")))
    term_pattern = build_term_pattern(terms)
    condition = lower_title.rlike(term_pattern) | lower_summary.rlike(term_pattern)

    summary = (
        df.filter(condition)
        .withColumn("hour", F.hour("timestamp_ts"))
        .groupBy("hour")
        .agg(F.count("*").alias("count"))
        .orderBy("hour")
    )
    return [row.asDict() for row in summary.collect()]


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

    try:
        while True:
            logger.info("Starting analysis iteration...")

            # Read Silver and Gold Delta tables using fallback mechanism
            silver_api = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/silver/api", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "silver" / "api"
            )
            silver_rss = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/silver/rss", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "silver" / "rss"
            )
            gold_return = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_return", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "gold" / "saham_return"
            )
            gold_volatility = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_volatility", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "gold" / "saham_volatility"
            )
            gold_news = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_news_mention", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "gold" / "saham_news_mention"
            )
            gold_sharpe = read_delta_with_fallback(
                spark, 
                f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_sharpe_proxy", 
                ROOT_DIR / "lakehouse" / "lakehouse_data" / "gold" / "saham_sharpe_proxy"
            )

            try:
                api_count = silver_api.count() if not silver_api.rdd.isEmpty() else 0
            except Exception:
                api_count = -1
            try:
                rss_count = silver_rss.count() if not silver_rss.rdd.isEmpty() else 0
            except Exception:
                rss_count = -1
            logger.info("API events: %s rows; RSS events: %s rows", api_count, rss_count)

            # Register temporary views for SQL queries
            silver_api.createOrReplaceTempView("api_events")
            silver_rss.createOrReplaceTempView("rss_events")

            # Map Gold Return to expected format by joining with start & latest prices from Silver
            stock_return = []
            if not silver_api.rdd.isEmpty() and not gold_return.rdd.isEmpty():
                window_asc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").asc(), F.col("price_current").asc())
                window_desc = Window.partitionBy("symbol").orderBy(F.col("timestamp_ts").desc(), F.col("price_current").desc())
                start_df = silver_api.withColumn("rn", F.row_number().over(window_asc)).filter(F.col("rn") == 1).select("symbol", F.col("price_current").alias("price_start"))
                latest_df = silver_api.withColumn("rn", F.row_number().over(window_desc)).filter(F.col("rn") == 1).select("symbol", F.col("price_current").alias("price_latest"))
                prices_df = start_df.join(latest_df, "symbol")
                
                stock_return_df = gold_return.join(prices_df, "symbol").select(
                    "symbol",
                    "price_start",
                    "price_latest",
                    F.col("avg_return").alias("return_pct")
                ).orderBy(F.col("return_pct").desc_nulls_last())
                stock_return = [row.asDict() for row in stock_return_df.collect()]

            # Map Gold Volatility to expected format by joining with average price and event count
            intraday_volatility = []
            if not silver_api.rdd.isEmpty() and not gold_volatility.rdd.isEmpty():
                vol_stats = silver_api.groupBy("symbol").agg(
                    F.avg("price_current").alias("avg_price"),
                    F.count("*").alias("event_count")
                )
                vol_df = gold_volatility.join(vol_stats, "symbol").select(
                    "symbol",
                    F.col("price_stddev").alias("volatility_price_std"),
                    "avg_price",
                    "event_count"
                ).orderBy(F.col("volatility_price_std").desc_nulls_last())
                intraday_volatility = [row.asDict() for row in vol_df.collect()]

            hourly_summary = compute_hourly_summary_sql(spark) if not silver_api.rdd.isEmpty() else []
            word_trends = compute_news_mentions_sql(spark) if not silver_rss.rdd.isEmpty() else []
            company_mentions = compute_company_mentions(silver_rss)
            top_company = company_mentions[0]["company"] if company_mentions else None

            # Get hourly news mentions from Gold news table
            top_company_hourly_mentions = []
            if not gold_news.rdd.isEmpty():
                COMPANY_TO_TICKER = {
                    "Bank Central Asia": "BBCA.JK",
                    "Bank Rakyat Indonesia": "BBRI.JK",
                    "Telkom Indonesia": "TLKM.JK",
                    "Astra International": "ASII.JK",
                    "Bank Mandiri": "BMRI.JK",
                }
                top_ticker = COMPANY_TO_TICKER.get(top_company)
                if top_ticker:
                    top_company_hourly = gold_news.filter(F.col("ticker") == top_ticker).select(
                        F.col("jam").alias("hour"),
                        F.col("mention_count").alias("count")
                    ).orderBy("hour")
                    top_company_hourly_mentions = [row.asDict() for row in top_company_hourly.collect()]

                if not top_company_hourly_mentions and top_company:
                    # Fallback to the ticker with highest overall mentions
                    top_ticker_row = gold_news.groupBy("ticker").agg(F.sum("mention_count").alias("total_mentions")).orderBy(F.col("total_mentions").desc()).first()
                    if top_ticker_row:
                        top_ticker = top_ticker_row["ticker"]
                        top_company_hourly = gold_news.filter(F.col("ticker") == top_ticker).select(
                            F.col("jam").alias("hour"),
                            F.col("mention_count").alias("count")
                        ).orderBy("hour")
                        top_company_hourly_mentions = [row.asDict() for row in top_company_hourly.collect()]

            spark_kpis = {
                "api_events": api_count,
                "rss_events": rss_count,
                "top_return_symbol": stock_return[0]["symbol"] if stock_return else None,
                "top_return_pct": stock_return[0]["return_pct"] if stock_return else None,
                "top_company": top_company,
                "top_company_mentions": company_mentions[0]["count"] if company_mentions else 0,
            }

            saham_sharpe_proxy = [row.asDict() for row in gold_sharpe.collect()] if not gold_sharpe.rdd.isEmpty() else []
            saham_news_mention_full = [row.asDict() for row in gold_news.collect()] if not gold_news.rdd.isEmpty() else []

            result = {
                "generated_at": utc_now_iso(),
                "api_event_count": api_count,
                "rss_event_count": rss_count,
                "stock_return": stock_return,
                "intraday_volatility": intraday_volatility,
                "hourly_summary": hourly_summary,
                "word_trends": word_trends,
                "company_mentions": company_mentions,
                "top_company_hourly_mentions": top_company_hourly_mentions,
                "spark_kpis": spark_kpis,
                "saham_sharpe_proxy": saham_sharpe_proxy,
                "saham_news_mention": saham_news_mention_full
            }

            write_local_results(result)
            write_hdfs_results(spark, result)
            logger.info("Iteration completed. Sleeping for 5 seconds...")
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Loop interrupted by user. Stopping Spark...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
