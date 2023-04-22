/*
To run please follow the following instructions
* 1. Begins the spark shell: `spark-shell --deploy-mode client`
# 2. load the scala file: `:load crime_profiling.scala`
* 3. Run the defined object: NYPDComplaintsCount.main(Array())
*/
import org.apache.spark.sql.SparkSession

object NYPDComplaintsCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("NYPDComplaintsCount").getOrCreate()

    // Load CSV file into a DataFrame
    
    val df = spark.read.format("csv").option("header", "true")
        .option("inferSchema", "true").option("delimiter", ",")
        .option("quote", "\"").option("escape", "\"")
        .load("hillary_data/output/nyc_crime_clean.csv")

    // Count the number of records
    val count = df.count()
    println(s"Number of records: $count")
    // no numerical data that can be profiled, so all of the following is profiling by text
    // Map the records to a key-value pair and count the number of records in each column
    val columnCounts = df.columns.map(c => (c, df.select(c).distinct().count()))
    columnCounts.foreach { case (column, count) => println(s"$column: $count distinct values") }

    // Find distinct values in each column and the count of text data
    df.columns.foreach(column => {
        val distinctValues = df.select(column).distinct()
        val countDistinctValues = distinctValues.count()
        println(s"Distinct values in column $column: $countDistinctValues")
        distinctValues.show(false)
    })
    spark.stop()
  }
}