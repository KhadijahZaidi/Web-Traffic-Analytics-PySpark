# Large-Scale Web Traffic Analytics with PySpark
#
# Curated from the original coursework notebook (submitted as a PDF export).
# Covers: log parsing with regex into a structured DataFrame, DataFrame/Spark SQL
# analysis, RDD-based analysis, and performance experiments comparing caching,
# partitioning and bucketing. Teammates' names and student IDs have been removed.
# This is a cleaned, representative excerpt rather than a line-for-line copy of
# every cell in the original notebook.

import re
import time
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import (
    regexp_extract, col, count, lit, countDistinct, to_timestamp,
    lead, lag, stddev, desc, expr, sum as spark_sum, rank, hour, dayofweek, mean,
)

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# ---------------------------------------------------------------------------
# Load and parse the raw web-server log into a structured DataFrame
# ---------------------------------------------------------------------------
file_path = "/web.log"
regex_pattern = r'(\S+) - - \[(.*?)\] "(.*?) (.*?) (HTTPS?/[\d.]+)" (\d{3}) (\d+)'

schema = StructType([
    StructField("Host", StringType(), False),
    StructField("Timestamp", TimestampType(), False),
    StructField("HTTP_Method", StringType(), False),
    StructField("URL", StringType(), False),
    StructField("HTTP_Version", StringType(), False),
    StructField("HTTP_Status_Code", IntegerType(), False),
    StructField("Bytes", IntegerType(), False),
    StructField("Message", StringType(), False),
])

df = spark.read.text(file_path)

parsed_df = df.select(
    regexp_extract(col("value"), regex_pattern, 1).alias("Host"),
    to_timestamp(regexp_extract(col("value"), regex_pattern, 2), "dd/MMM/yyyy:HH:mm:ss").alias("Timestamp"),
    regexp_extract(col("value"), regex_pattern, 3).alias("HTTP_Method"),
    regexp_extract(col("value"), regex_pattern, 4).alias("URL"),
    regexp_extract(col("value"), regex_pattern, 5).alias("HTTP_Version"),
    regexp_extract(col("value"), regex_pattern, 6).cast("int").alias("HTTP_Status_Code"),
    regexp_extract(col("value"), regex_pattern, 7).cast("int").alias("Bytes"),
    regexp_extract(col("value"), regex_pattern, 8).alias("Message"),
)
parsed_df.printSchema()
parsed_df.createOrReplaceTempView("web_logs")

# ---------------------------------------------------------------------------
# DataFrame + Spark SQL analysis: request counts, response-size metrics
# ---------------------------------------------------------------------------
RequestCountByMethod = """
    SELECT HTTP_Method, HTTP_Version, COUNT(*) AS TotalRequests
    FROM web_logs
    GROUP BY HTTP_Method, HTTP_Version
    ORDER BY TotalRequests DESC
"""
ResponseSizeMetrics = """
    SELECT HTTP_Method, HTTP_Version,
           AVG(Bytes) AS AvgResponseSize, MAX(Bytes) AS MaxResponseSize,
           MIN(Bytes) AS MinResponseSize, COUNT(*) AS TotalRequests
    FROM web_logs
    GROUP BY HTTP_Method, HTTP_Version
    ORDER BY AvgResponseSize DESC
"""
spark.sql(RequestCountByMethod).show(5, truncate=False)
spark.sql(ResponseSizeMetrics).show(5, truncate=False)

# Rolling hourly traffic per URL using a Spark SQL window function
rolling_hourly_traffic_query = """
    SELECT URL,
           DATE_FORMAT(Timestamp, 'yyyy-MM-dd HH:mm:ss') AS Hourly_Window,
           Message,
           COUNT(*) OVER (PARTITION BY URL, DATE_FORMAT(Timestamp, 'yyyy-MM-dd HH:mm:ss'))
               AS RollingHourlyTraffic
    FROM web_logs
"""
spark.sql(rolling_hourly_traffic_query).show(truncate=False)

# Window functions: request/byte stats partitioned by host and status code
query = """
    WITH RequestStats AS (
        SELECT Host, HTTP_Status_Code, URL, HTTP_Method, Bytes,
               COUNT(*) OVER (PARTITION BY Host) AS TotalRequestsByHost,
               AVG(Bytes) OVER (PARTITION BY HTTP_Status_Code) AS AvgBytesByStatus,
               RANK() OVER (PARTITION BY URL ORDER BY Bytes DESC) AS RankByBytes
        FROM web_logs
    )
    SELECT Host, HTTP_Status_Code, URL, HTTP_Method, Bytes,
           TotalRequestsByHost, AvgBytesByStatus, RankByBytes
    FROM RequestStats
    ORDER BY TotalRequestsByHost DESC, AvgBytesByStatus DESC
"""
result_df = spark.sql(query)
result_pd_df = result_df.toPandas()

plt.figure(figsize=(12, 6))
sns.barplot(data=result_pd_df.head(10), x="Host", y="TotalRequestsByHost")
plt.title("Top 10 Hosts by Total Requests", fontsize=16)
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()

# ---------------------------------------------------------------------------
# RDD-based analysis
# ---------------------------------------------------------------------------
log_rdd = spark.sparkContext.textFile(file_path)


def parse_log(line):
    match = re.match(regex_pattern, line)
    if match:
        return {
            "IP": match.group(1),
            "Timestamp": match.group(2),
            "HTTP_Method": match.group(3),
            "URL": match.group(4),
            "Status_Code": int(match.group(6)),
            "Bytes": int(match.group(7)),
            "Message": match.group(8),
        }
    return None


parsed_rdd = log_rdd.map(parse_log).filter(lambda x: x is not None)

http_request_rdd = parsed_df.rdd.map(lambda row: (row["HTTP_Method"], row["URL"], row["HTTP_Version"]))
http_method_rdd = http_request_rdd.map(lambda x: (x[0], 1))
http_method_counts = http_method_rdd.reduceByKey(lambda a, b: a + b)

most_frequent_method = http_method_counts.reduce(lambda a, b: a if a[1] > b[1] else b)
least_frequent_method = http_method_counts.reduce(lambda a, b: a if a[1] < b[1] else b)
print("Most Frequently Used HTTP Method:", most_frequent_method)
print("Least Frequently Used HTTP Method:", least_frequent_method)

# Most frequent HTTP method per URL path segment
segment_method_rdd = (
    http_request_rdd.filter(lambda x: x[1].strip() != "")
    .flatMap(lambda x: [(segment, x[0]) for segment in x[1].strip("/").split("/")])
)
segment_method_counts = segment_method_rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b: a + b)

# ---------------------------------------------------------------------------
# Performance experiments: caching, partitioning, bucketing
# ---------------------------------------------------------------------------
start_time = time.time()
http_request_rdd.groupByKey().count()
print(f"Time without caching: {time.time() - start_time:.2f} seconds")

cached_rdd = http_request_rdd.cache()
start_time = time.time()
cached_rdd.groupByKey().count()
print(f"Time with caching: {time.time() - start_time:.2f} seconds")
# Observed: without caching ~129.01s, with caching ~141.94s (caching did not
# help this particular workload).

parsed_df.write \
    .bucketBy(4, "URL") \
    .sortBy("URL") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("bucketed_logs")

start_time = time.time()
non_bucketed_result = parsed_df.filter(col("URL") == "/home").count()
print(f"Non-bucketed query took: {time.time() - start_time:.2f} seconds")

start_time = time.time()
bucketed_result = spark.sql("SELECT * FROM bucketed_logs WHERE URL = '/home'").count()
print(f"Bucketed query took: {time.time() - start_time:.2f} seconds")
# Observed: non-bucketed ~63.95s vs bucketed ~0.95s for the same URL lookup.
