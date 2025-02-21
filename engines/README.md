# Seeknal Engines

This folder is for storing the transformation engines to be used with Seeknal. This enables developers to enhance Seeknal's tasks functionality using various data processing libraries (e.g., Apache Spark, Pandas, etc.). The engine specifications must accept YAML input to execute the pipeline and return the results as a data frame. The following is a list of the engines that are currently available:

- `spark-engine`: The engine is based on Apache Spark.