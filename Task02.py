import sys
import time
from pyspark.sql import SparkSession, functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit Task02.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("A2_Task02_Temperature_Statistics") \
        .master("local[*]") \
        .getOrCreate()

    start = time.time()

    df = spark.read.parquet(input_path)

    stats_df = df.groupBy("year").agg(
        F.min("temperature").alias("min_temp"),
        F.max("temperature").alias("max_temp"),
        F.round(F.avg("temperature"), 2).alias("mean_temp"),
        F.expr("percentile_approx(temperature, 0.5)").alias("median_temp")
    ).orderBy("year")

    stats_df.write.mode("overwrite").json(output_path)

    print("=== Task02 Summary ===")
    print(f"Years processed: {stats_df.count()}")

    elapsed = time.time() - start
    print(f"Runtime: {elapsed:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()
