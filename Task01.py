import sys
import time
from pyspark.sql import SparkSession, functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit Task01.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("A2_Task01_Station_Operability") \
        .master("local[*]") \
        .getOrCreate()

    start = time.time()

    df = spark.read.parquet(input_path)

    period_df = df.filter((F.col("year") >= 1920) & (F.col("year") <= 1940))

    station_years = period_df.select("station_id", "year").distinct()

    station_summary = station_years.groupBy("station_id").agg(
        F.countDistinct("year").alias("years_operable"),
        F.sort_array(F.collect_set("year")).alias("operable_years")
    )

    threshold_80 = 17
    all_years = 21

    stations_80_or_more = station_summary \
        .filter(F.col("years_operable") >= threshold_80) \
        .orderBy(F.desc("years_operable"), F.asc("station_id"))

    all_years_stations = station_summary \
        .filter(F.col("years_operable") == all_years) \
        .orderBy(F.asc("station_id"))

    top_50 = station_summary \
        .orderBy(F.desc("years_operable"), F.asc("station_id")) \
        .limit(50)

    stations_80_or_more.write.mode("overwrite").json(f"{output_path}/stations_80_or_more")
    all_years_stations.write.mode("overwrite").json(f"{output_path}/all_years_stations")
    top_50.write.mode("overwrite").json(f"{output_path}/top_50")

    print("=== Task01 Summary ===")
    print(f"Stations operable >=80% of 1920-1940: {stations_80_or_more.count()}")
    print(f"Stations operable all 21 years     : {all_years_stations.count()}")
    print("Top 50 stations saved.")

    elapsed = time.time() - start
    print(f"Runtime: {elapsed:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()
