from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F
import json


os.environ.setdefault("HADOOP_USER_NAME", "hadoop")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FS = "hdfs://localhost:8020"
BRONZE_API_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/bronze/api"
BRONZE_RSS_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/bronze/rss"
SILVER_API_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/silver/api"
SILVER_RSS_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/silver/rss"
LOCAL_LAKEHOUSE_DIR = BASE_DIR / "lakehouse_data"


def build_spark() -> SparkSession:
	builder = (
		SparkSession.builder.appName("Silver-SahamMeter")
		.config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
		.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
		.config("spark.sql.session.timeZone", "UTC")
	)
	return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta_with_fallback(spark: SparkSession, hdfs_path: str, local_path: Path) -> DataFrame:
	try:
		return spark.read.format("delta").load(hdfs_path)
	except Exception:
		if local_path.exists():
			return spark.read.format("delta").load(str(local_path))
		raise


def clean_api(df: DataFrame) -> DataFrame:
	cleaned = (
		df.withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
		.withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
		.withColumn("company_name", F.trim(F.col("company_name")))
		.withColumn("source", F.trim(F.col("source")))
		.withColumn("topic", F.trim(F.col("topic")))
		.withColumn("timestamp_ts", F.to_timestamp("timestamp"))
		.withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
		.withColumn("_cleaned_at", F.current_timestamp())
		.withColumn("price_current", F.col("price_current").cast("double"))
		.withColumn("price_open", F.col("price_open").cast("double"))
		.withColumn("price_high", F.col("price_high").cast("double"))
		.withColumn("price_low", F.col("price_low").cast("double"))
		.withColumn("volume", F.col("volume").cast("double"))
		.withColumn("previous_close", F.col("previous_close").cast("double"))
		.withColumn("change_24h_pct", F.col("change_24h_pct").cast("double"))
		.fillna({"company_name": "", "source": "", "topic": ""})
		.filter(
			F.col("symbol").isNotNull()
			& F.col("timestamp_ts").isNotNull()
			& F.col("price_current").isNotNull()
			& F.col("volume").isNotNull()
		)
		.filter((F.col("price_current") > 0) & (F.col("volume") >= 0))
		.filter(
			F.col("price_high").isNull()
			| F.col("price_low").isNull()
			| (F.col("price_high") >= F.col("price_low"))
		)
		.dropDuplicates(["symbol", "ticker", "timestamp_ts"])
	)
	return cleaned


def clean_rss(df: DataFrame) -> DataFrame:
	cleaned = (
		df.withColumn("item_id", F.trim(F.col("item_id")))
		.withColumn("title", F.trim(F.col("title")))
		.withColumn("link", F.trim(F.col("link")))
		.withColumn("summary", F.trim(F.col("summary")))
		.withColumn("source", F.trim(F.col("source")))
		.withColumn("topic", F.trim(F.col("topic")))
		.withColumn("published", F.trim(F.col("published")))
		.withColumn("timestamp_ts", F.to_timestamp("timestamp"))
		.withColumn("published_ts", F.to_timestamp("published", "EEE, dd MMM yyyy HH:mm:ss Z"))
		.withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
		.withColumn("_cleaned_at", F.current_timestamp())
		.fillna({"summary": "", "source": "", "topic": ""})
		.filter(
			F.col("item_id").isNotNull()
			& F.col("title").isNotNull()
			& (F.length(F.col("title")) > 0)
			& F.col("link").isNotNull()
			& F.col("timestamp_ts").isNotNull()
		)
		.dropDuplicates(["item_id", "link", "timestamp_ts"])
	)
	return cleaned


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
		api_bronze = read_delta_with_fallback(
			spark,
			BRONZE_API_HDFS_PATH,
			LOCAL_LAKEHOUSE_DIR / "bronze" / "api",
		)
		rss_bronze = read_delta_with_fallback(
			spark,
			BRONZE_RSS_HDFS_PATH,
			LOCAL_LAKEHOUSE_DIR / "bronze" / "rss",
		)
		# API cleaning with stepwise counts to attribute losses
		api_step0 = api_bronze
		api_before = api_step0.count()

		# Step 1: require valid timestamp
		api_step1 = api_step0.filter(F.to_timestamp(F.col("timestamp")).isNotNull())
		api_after_ts = api_step1.count()

		# Step 2: require numeric price & volume
		api_step2 = api_step1.filter(
			F.col("price_current").isNotNull()
			& (F.col("price_current") > 0)
			& F.col("volume").isNotNull()
			& (F.col("volume") >= 0)
		)
		api_after_values = api_step2.count()

		# Step 3: drop duplicates
		api_step3 = api_step2.dropDuplicates(["symbol", "ticker", "timestamp_ts"])
		api_after = api_step3.count()

		api_silver = clean_api(api_bronze)

		write_delta_with_fallback(api_step3, SILVER_API_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "silver" / "api")

		# RSS cleaning with stepwise counts
		rss_step0 = rss_bronze
		rss_before = rss_step0.count()

		# Step 1: valid timestamp
		rss_step1 = rss_step0.filter(F.to_timestamp(F.col("timestamp")).isNotNull())
		rss_after_ts = rss_step1.count()

		# Step 2: require title/link/item_id
		rss_step2 = rss_step1.filter(
			F.col("item_id").isNotNull()
			& F.col("title").isNotNull()
			& (F.length(F.col("title")) > 0)
			& F.col("link").isNotNull()
		)
		rss_after_fields = rss_step2.count()

		# Step 3: drop duplicates
		rss_step3 = rss_step2.dropDuplicates(["item_id", "link", "timestamp_ts"])
		rss_after = rss_step3.count()

		rss_silver = clean_rss(rss_bronze)

		write_delta_with_fallback(rss_step3, SILVER_RSS_HDFS_PATH, LOCAL_LAKEHOUSE_DIR / "silver" / "rss")

		# Prepare report
		reports_dir = LOCAL_LAKEHOUSE_DIR / "reports"
		reports_dir.mkdir(parents=True, exist_ok=True)

		api_report = {
			"stage": "api",
			"bronze_count": api_before,
			"after_timestamp": api_after_ts,
			"after_values": api_after_values,
			"after_dedup": api_after,
			"lost_timestamp": api_before - api_after_ts,
			"lost_invalid_values": api_after_ts - api_after_values,
			"lost_duplicates": api_after_values - api_after,
		}

		rss_report = {
			"stage": "rss",
			"bronze_count": rss_before,
			"after_timestamp": rss_after_ts,
			"after_required_fields": rss_after_fields,
			"after_dedup": rss_after,
			"lost_timestamp": rss_before - rss_after_ts,
			"lost_missing_fields": rss_after_ts - rss_after_fields,
			"lost_duplicates": rss_after_fields - rss_after,
		}

		# percent
		def add_percent(report):
			bc = report.get("bronze_count") or 0
			if bc > 0:
				for k in list(report.keys()):
					if k.startswith("lost_"):
						report[f"{k}_pct"] = round(report[k] / bc * 100, 2)
			return report

		api_report = add_percent(api_report)
		rss_report = add_percent(rss_report)

		# save reports locally
		(reports_dir / "api_cleaning_report.json").write_text(json.dumps(api_report, indent=2))
		(reports_dir / "rss_cleaning_report.json").write_text(json.dumps(rss_report, indent=2))

		print("API cleaned: {} -> {} rows; RSS cleaned: {} -> {} rows".format(api_before, api_after, rss_before, rss_after))
		print(f"Reports written to {reports_dir}")
	finally:
		spark.stop()


if __name__ == "__main__":
	main()
