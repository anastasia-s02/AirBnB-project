/*
To run please follow the following instructions
* 1. Begins the spark shell: `spark-shell --deploy-mode client`
# 2. load the scala file: `:load nyc_crime_ingest.scala`
* 3. Run the defined object: NYPDComplaintsIngest.main(Array())
*/
import org.apache.spark.sql.SparkSession

object NYPDComplaintsIngest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("NYPDComplaintsCount").getOrCreate()

    // Load CSV file into a DataFrame
    val df = spark.read.format("csv")
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv("hillary_data/NYPD.csv")
    df.printSchema()
    spark.stop()
  }
}