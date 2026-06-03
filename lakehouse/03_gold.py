from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FS = "hdfs://localhost:8020"
SILVER_API_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/silver/api"
SILVER_RSS_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/silver/rss"

GOLD_RETURN_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_return"
GOLD_VOLATILITY_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_volatility"
GOLD_SHARPE_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_sharpe_proxy"
GOLD_NEWS_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/gold/saham_news_mention"

LOCAL_LAKEHOUSE_DIR = BASE_DIR / "lakehouse_data"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("Gold-SahamMeter")
        .config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.crossJoin.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta_with_fallback(spark: SparkSession, hdfs_path: str, local_path: Path) -> DataFrame:
    try:
        return spark.read.format("delta").load(hdfs_path)
    except Exception:
        if local_path.exists():
            return spark.read.format("delta").load(str(local_path))
        raise


def write_delta_with_fallback(df: DataFrame, hdfs_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(hdfs_path)
        print(f"Saved Delta table to {hdfs_path}")
    except Exception as exc:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(local_path))
        print(f"HDFS write failed for {hdfs_path}: {exc}")
        print(f"Saved Delta table to local fallback {local_path}")


def main() -> None:
    spark = build_spark()
    try:
        print("Reading Silver tables...")
        api_silver = read_delta_with_fallback(
            spark,
            SILVER_API_HDFS_PATH,
            LOCAL_LAKEHOUSE_DIR / "silver" / "api",
        )
        rss_silver = read_delta_with_fallback(
            spark,
            SILVER_RSS_HDFS_PATH,
            LOCAL_LAKEHOUSE_DIR / "silver" / "rss",
        )
        
        # Calculate base statistics per ticker for Returns, Volatility, and Sharpe
        stats = api_silver.groupBy("ticker", "symbol", "company_name").agg(
            F.avg("change_24h_pct").alias("avg_return"),
            F.stddev("price_current").alias("price_stddev"),
            F.stddev("change_24h_pct").alias("risk")
        )
        
        print("Generating Gold saham_return...")
        saham_return = stats.select("ticker", "symbol", "company_name", "avg_return").orderBy("avg_return", ascending=False)
        write_delta_with_fallback(saham_return, GOLD_RETURN_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "gold" / "saham_return")
        
        print("Generating Gold saham_volatility...")
        saham_volatility = stats.select("ticker", "symbol", "company_name", "price_stddev").orderBy("price_stddev", ascending=False)
        write_delta_with_fallback(saham_volatility, GOLD_VOLATILITY_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "gold" / "saham_volatility")
        
        print("Generating Gold saham_sharpe_proxy...")
        saham_sharpe_proxy = stats.select("ticker", "avg_return", "risk").withColumn(
            "sharpe_proxy", 
            F.when((F.col("risk").isNull()) | (F.col("risk") == 0), F.lit(None))
            .otherwise(F.col("avg_return") / F.col("risk"))
        ).orderBy("sharpe_proxy", ascending=False)
        write_delta_with_fallback(saham_sharpe_proxy, GOLD_SHARPE_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "gold" / "saham_sharpe_proxy")
        
        print("Generating Gold saham_news_mention...")
        # Get distinct stocks to cross-join with news
        stocks = api_silver.select("ticker", "symbol").distinct()
        
        # Add hour column to RSS and API
        rss_hourly = rss_silver.withColumn("jam", F.hour("timestamp_ts"))
        api_hourly = api_silver.withColumn("jam", F.hour("timestamp_ts")).groupBy("ticker", "jam").agg(
            F.avg("change_24h_pct").alias("hourly_avg_return")
        )
        
        # Find mentions of stock symbol in news title or summary
        mentions = rss_hourly.crossJoin(stocks).filter(
            F.lower(F.col("title")).contains(F.lower(F.col("symbol"))) |
            F.lower(F.col("summary")).contains(F.lower(F.col("symbol")))
        )
        
        # Count mentions per ticker per hour
        mention_counts = mentions.groupBy("ticker", "jam").count().withColumnRenamed("count", "mention_count")
        
        # Join counts with hourly returns
        saham_news_mention = api_hourly.join(mention_counts, ["ticker", "jam"], "left").fillna({"mention_count": 0})
        write_delta_with_fallback(saham_news_mention, GOLD_NEWS_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "gold" / "saham_news_mention")
        
        print("Gold layer processing completed successfully.")
        
    except Exception as e:
        print(f"Error processing Gold layer: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
