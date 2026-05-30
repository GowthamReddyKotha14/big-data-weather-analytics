import sys
import time
from pyspark.sql import SparkSession, functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit Task04.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("A2_Task04_Temperature_Variability") \
        .master("local[*]") \
        .getOrCreate()

    start = time.time()

    df = spark.read.parquet(input_path)

    result_df = (
        df.groupBy("year")
        .agg(
            F.min("temperature").alias("min_temp"),
            F.max("temperature").alias("max_temp"),
            F.round(F.avg("temperature"),2).alias("mean_temp"),
            F.round(F.stddev("temperature"),2).alias("stddev_temp")
        )
        .withColumn("temp_range", F.col("max_temp") - F.col("min_temp"))
        .orderBy(F.desc("temp_range"), F.desc("stddev_temp"), F.asc("year"))
    )

    result_df.write.mode("overwrite").json(output_path)

    print("=== Task04 Summary ===")
    print("Years analyzed:", result_df.count())

    elapsed = time.time() - start
    print("Runtime:", round(elapsed,2), "seconds")

    spark.stop()


if __name__ == "__main__":
    main()
