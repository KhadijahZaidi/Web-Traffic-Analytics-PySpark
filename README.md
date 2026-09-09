# Large-Scale Web Traffic Analytics with PySpark

*Academic group project — Big Data Analytics*

## Purpose
Process large web-server log data with distributed computing to identify traffic patterns, HTTP-status trends, popular URLs, host activity, and performance-optimisation opportunities.

## Overview
Collaborated on a distributed web-log analytics solution using Python, Apache Spark, PySpark DataFrames, RDDs, and Spark SQL. The project processed more than one million log records to identify traffic patterns, HTTP status-code trends, frequently accessed URLs, and host activity. My contribution included regular-expression-based log parsing, rolling hourly traffic analysis using window functions, visualisation, and performance experiments with caching, repartitioning, and bucketing. A bucketed URL query completed in approximately 0.95 seconds compared with 63.95 seconds for its non-bucketed equivalent in the recorded experiment. The work also considered privacy, fairness, and responsible data governance.

## Technical Highlights
- Built regex-based parsing and validated timestamps, URLs, methods, status codes, and transferred bytes.
- Created rolling hourly URL analysis with Spark SQL window functions.
- Compared caching, partitioning, and bucketing while reporting results without assuming every optimisation improves every workload.

## Tech Stack
Python, Apache Spark, PySpark, Spark SQL, RDDs, Pandas, Matplotlib

## Summary
Developed distributed PySpark web-log analytics using DataFrames, RDDs, Spark SQL, and window functions, with performance testing across caching, partitioning, and bucketing.

---
Note: this was a group project — my contribution is described above.
