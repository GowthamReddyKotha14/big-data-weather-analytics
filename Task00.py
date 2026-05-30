import sys
import time
from pyspark.sql import SparkSession, functions as F, types as T

VALID_QUALITY_FLAGS = {"0", "1", "4", "5", "9"}


def parse_line(line):
    try:
        station_id = line[4:10].strip()
        year = int(line[15:19])
        month = int(line[19:21])
        temp = int(line[87:92])
        quality = line[92]

        if not station_id:
            return None
        if temp == 9999:
            return None
        if quality not in VALID_QUALITY_FLAGS:
            return None

        return (station_id, year, month, temp / 10.0)
    except Exception:
        return None


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit Task00.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("A2_Task00_Filtering") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext
    start = time.time()

    raw = sc.textFile(f"{input_path}/*.txt")
    parsed_rdd = raw.map(parse_line).filter(lambda x: x is not None)

    schema = T.StructType([
        T.StructField("station_id", T.StringType(), False),
        T.StructField("year", T.IntegerType(), False),
        T.StructField("month", T.IntegerType(), False),
        T.StructField("temperature", T.DoubleType(), False),
    ])

    df = spark.createDataFrame(parsed_rdd, schema=schema)

    before_count = df.count()

    monthly_counts = (
        df.groupBy("station_id", "year", "month")
          .agg(F.count("*").alias("obs_count"))
    )

    operable_station_years = (
        monthly_counts
        .filter(F.col("obs_count") >= 2)
        .groupBy("station_id", "year")
        .agg(
            F.countDistinct("month").alias("valid_months"),
            F.min("obs_count").alias("min_monthly_obs")
        )
        .filter((F.col("valid_months") == 12) & (F.col("min_monthly_obs") >= 2))
        .select("station_id", "year")
    )

    filtered_df = df.join(operable_station_years, on=["station_id", "year"], how="inner")

    after_count = filtered_df.count()

    filtered_df.write.mode("overwrite").parquet(output_path)

    elapsed = time.time() - start

    print("=== Task00 Summary ===")
    print(f"Records before filtering: {before_count}")
    print(f"Records after filtering : {after_count}")
    print(f"Runtime: {elapsed:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()