from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, hour, sum

# Initialize Spark session
spark = SparkSession.builder.appName("Read Raw Energy Data").getOrCreate()

raw_energy_data = raw_energy_path = "/workspace/data-lake/raw/energy/*/*.json"

# read the data
df = spark.read.json(raw_energy_data)


timeStamp = df.withColumn("event_time", to_timestamp(col("timestamp"))) # extracts timestamp from timestamp column in the data that spark reads
df = df.withColumn("event_date", to_date(col("timestamp"))) # use the timestamp column to extract date
df = df.withColumn("event_hour", hour(col("timestamp"))) # use the timestamp column to extract hour

daily_usage = df.groupBy("event_date").agg(sum("energy_kwh").alias("total_energy_kwh"))

hourly_usage = df.groupBy("event_date", "event_hour").agg(sum("energy_kwh").alias("total_energy_kwh"))

daily_output_path = "/workspace/data-lake/processed/energy/daily_usage"
hourly_output_path = "/workspace/data-lake/processed/energy/hourly_usage"

print("DAILY USAGE")
daily_usage.show(truncate=False)

print("HOURLY USAGE")
hourly_usage.show(truncate=False)

daily_usage.write.mode("overwrite").parquet(
    "/workspace/data-lake/processed/energy/daily_usage"
)


hourly_usage.write.mode("overwrite").partitionBy("event_date", "event_hour").parquet(hourly_output_path)