import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

os.environ.setdefault("HADOOP_USER_NAME", "hadoop")

builder = (SparkSession.builder.appName("Bronze-SahamMeter")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

api_df = spark.read.option("multiLine", True).json("/data/saham/api").withColumns(
    {
        "_source": sf.lit("api"),
        "_ingested_at": sf.current_timestamp()
    }
)

rss_df = spark.read.option("multiLine", True).json("/data/saham/rss").withColumns(
    {
        "_source": sf.lit("rss"),
        "_ingested_at": sf.current_timestamp()
    }
)

api_df.write.format("delta").mode("append").save("hdfs://localhost:8020/data/saham/lakehouse/bronze/api")
rss_df.write.format("delta").mode("append").save("hdfs://localhost:8020/data/saham/lakehouse/bronze/rss")

spark.stop()
