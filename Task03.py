import sys
import time
from pyspark.sql import SparkSession, functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit Task03.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("A2_Task03_Hottest_Stations") \
        .master("local[*]") \
        .getOrCreate()

    start = time.time()

    df = spark.read.parquet(input_path)

    result_df = (
        df.groupBy("station_id")
          .agg(
              F.round(F.avg("temperature"), 2).alias("avg_temp"),
              F.min("temperature").alias("min_temp"),
              F.max("temperature").alias("max_temp"),
              F.count("*").alias("record_count")
          )
          .orderBy(F.desc("avg_temp"), F.desc("record_count"), F.asc("station_id"))
          .limit(50)
    )

    result_df.write.mode("overwrite").json(output_path)

    print("=== Task03 Summary ===")
    print(f"Top stations saved: {result_df.count()}")

    elapsed = time.time() - start
    print(f"Runtime: {elapsed:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()
