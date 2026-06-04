from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FS = "hdfs://localhost:8020"
SILVER_API_HDFS_PATH = f"{DEFAULT_FS}/data/saham/lakehouse/silver/api"
LOCAL_LAKEHOUSE_DIR = BASE_DIR / "lakehouse_data"

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("TimeTravelDemo-SahamMeter")
        .config("spark.hadoop.fs.defaultFS", DEFAULT_FS)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main() -> None:
    spark = build_spark()
    try:
        # Check which path exists (HDFS or local fallback)
        silver_path = SILVER_API_HDFS_PATH
        try:
            spark.read.format("delta").load(silver_path)
        except Exception:
            silver_path = str(LOCAL_LAKEHOUSE_DIR / "silver" / "api")
            print(f"HDFS path not found, using local fallback path: {silver_path}")
            
        print(f"Targeting Delta table at: {silver_path}")
        
        deltaTable = DeltaTable.forPath(spark, silver_path)
        
        print("\n=== History Tabel Silver ===")
        deltaTable.history().select("version", "timestamp", "operation").show(truncate=False)
        
        print("\n=== Lakukan Update (Time Travel Demo) ===")
        print("Mengubah company_name yang berisi 'Telkom Indonesia' menjadi 'PT Telkom Indonesia (Persero) Tbk'...")
        
        deltaTable.update(
            condition=F.col("company_name") == "Telkom Indonesia",
            set={"company_name": F.lit("PT Telkom Indonesia (Persero) Tbk")}
        )
        
        print("\n=== Data SEKARANG ===")
        spark.read.format("delta").load(silver_path).filter(F.col("ticker") == "TLKM.JK").groupBy("company_name").count().show(truncate=False)
        
        print("\n=== Data VERSI 0 (sebelum update) ===")
        spark.read.format("delta").option("versionAsOf", 0).load(silver_path).filter(F.col("ticker") == "TLKM.JK").groupBy("company_name").count().show(truncate=False)
            
    except Exception as e:
        print(f"Error during time travel demonstration: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
