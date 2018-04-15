# Spark template using Scala
Spark application template for running on Cloud

# Quick Tips
- Spark only work with Scala version **below** 2.12 and Java 9 according to its [documentation](https://spark.apache.org/docs/latest/)
- Compile and create scala program using sbt 
```sbt compile```
```sbt package```
- Upload datasets on Cloud Storage - example

# Example Use Case
1. Running Spark application on [Google Cloud Dataproc](https://cloud.google.com/dataproc/). Tutorial can be found [here](https://shinesolutions.com/2015/10/14/google-cloud-dataproc-and-the-17-minute-train-challenge/)
2. Save the output to a Parquet to [Google Cloud Storage](https://cloud.google.com/storage/)
3. Import to [Google BigQuery](https://cloud.google.com/bigquery/) and further process it

