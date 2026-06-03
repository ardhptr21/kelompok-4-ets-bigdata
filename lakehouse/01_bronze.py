import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FS = "hdfs://localhost:8020"
BRONZE_API_HDFS  = f"{DEFAULT_FS}/data/saham/lakehouse/bronze/api"
BRONZE_RSS_HDFS  = f"{DEFAULT_FS}/data/saham/lakehouse/bronze/rss"
LOCAL_BRONZE_API = BASE_DIR / "lakehouse_data" / "bronze" / "api"
LOCAL_BRONZE_RSS = BASE_DIR / "lakehouse_data" / "bronze" / "rss"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("Bronze-SahamMeter")
        .config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta_with_fallback(df, hdfs_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.write.format("delta").mode("append").save(hdfs_path)
        print(f"[Bronze] Saved to HDFS: {hdfs_path} ({df.count()} rows)")
    except Exception as exc:
        df.write.format("delta").mode("append").save(str(local_path))
        print(f"[Bronze] HDFS failed ({exc}), saved to local fallback: {local_path}")


def main():
    spark = build_spark()
    try:
        print("Reading raw JSON from HDFS...")
        api_df = (
            spark.read.option("multiLine", True)
            .json(f"{DEFAULT_FS}/data/saham/api")
            .withColumn("_source", F.lit("api"))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        rss_df = (
            spark.read.option("multiLine", True)
            .json(f"{DEFAULT_FS}/data/saham/rss")
            .withColumn("_source", F.lit("rss"))
            .withColumn("_ingested_at", F.current_timestamp())
        )

        print("Writing Bronze layers...")
        write_delta_with_fallback(api_df, BRONZE_API_HDFS, LOCAL_BRONZE_API)
        write_delta_with_fallback(rss_df, BRONZE_RSS_HDFS, LOCAL_BRONZE_RSS)
        print("Bronze layer completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
