from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = (
    SparkSession
    .builder
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/opt/workspace"

results = (
    spark.read.csv(
        path=os.path.join(DATA_DIR, "ufo.csv"),
        header=True,
        inferSchema=True
    )
    .groupby(F.col("country"))
    .count()
)

(
    results
    .orderBy("count", ascending=False)
    .show(10)
)
spark.stop()
