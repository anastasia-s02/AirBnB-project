// SECTION: Ingestion

/**
 * Ingest data and preview schema from hdfs.
 */

val df = spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true").option("multiLine", true)
    .option("delimiter", ",").option("quote", "\"").option("escape", "\"")
    .load("project/listings.csv")

df.printSchema()

print(df.show(1))