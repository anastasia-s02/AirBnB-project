// SECTION: Analysis - joined data for NYC listings
// The resulting csv files can be found on as15026 hdfs in the folder "project/analytics"
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegressionModel
import scala.Array.ofDim
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
 * This file creates summaries that are to be used to create graphs and visualizations. 
 * Additionaly, it runs linear regression on the dataframe and computer correlation coefficients. 
 */

// load dataframe
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").option("quote", "\"").option("escape", "\"").load("project/join_output/joined_data.csv")

/**
 * This section rund linear regression with "rating_range" as a predictor.
 */
val assembler = new VectorAssembler().setInputCols(Array("host_total_listings_count", "number_of_reviews", "price_range", "superhost", "host_verified", "neighbourhood_match", "non_felony_crimes", "felony_crimes")).setOutputCol("features")

val lr = new LinearRegression().setLabelCol("rating_range")

val pipeline = new Pipeline().setStages(Array(assembler, lr))

val model = pipeline.fit(df)

val linRegModel = model.stages(1).asInstanceOf[LinearRegressionModel]

println("Linear regression model for rating_range as output:")
println(s"RMSE:  ${linRegModel.summary.rootMeanSquaredError}")
println(s"r2:    ${linRegModel.summary.r2}")
println(s"Model: coefficients = ${linRegModel.coefficients.toArray.mkString(", ")}, intercept =  ${linRegModel.intercept}")


/**
 * This section rund linear regression with "price_range" as a predictor.
 */
val assembler2 = new VectorAssembler().setInputCols(Array("host_total_listings_count", "number_of_reviews", "rating_range", "superhost", "host_verified", "neighbourhood_match", "non_felony_crimes", "felony_crimes")).setOutputCol("features")

val lr2 = new LinearRegression().setLabelCol("price_range")

val pipeline2 = new Pipeline().setStages(Array(assembler2, lr2))

val model2 = pipeline2.fit(df)

val linRegModel2 = model2.stages(1).asInstanceOf[LinearRegressionModel]

println("Linear regression model for price_range as output:")
println(s"RMSE:  ${linRegModel2.summary.rootMeanSquaredError}")
println(s"r2:    ${linRegModel2.summary.r2}")
println(s"Model: coefficients = ${linRegModel2.coefficients.toArray.mkString(", ")}, intercept =  ${linRegModel2.intercept}")


/**
 * This section computer correlation matrix between each column. 
 */
def correlation(col1: String, col2: String) : Double = {
	return df.stat.corr(col1, col2)
}

val col_names = Array("host_total_listings_count", "number_of_reviews", "price_range", "rating_range", "superhost", "host_verified", "neighbourhood_match", "non_felony_crimes", "felony_crimes")

var myMatrix = ofDim[Double](9,9) 

// build a matrix
for (i <- 0 to 8) {
	for ( j <- 0 to 8) {
    	myMatrix(i)(j) = correlation(col_names(i), col_names(j));
 	}
}

// print out the matrix 
println("Correlation matrix:")
println(col_names.mkString("|"))
for (i <- 0 to 8) {
	for ( j <- 0 to 8) {
		print("|" + myMatrix(i)(j) + "|");
	}
	println();
}

var outputFolder = "project/analytics"

// a helper function to save summary dataframes
def saveDF(df: DataFrame, filename: String) = {
  df.coalesce(1).write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("compression", "none")
    .csv(s"$outputFolder/$filename")
}

/**
 * This section creates summary dataframes to be abke to see the trends and influences of each of the factors on price and rating. 
 */
val priceCrimes = df.groupBy("price_range").agg((sum("non_felony_crimes") + sum("felony_crimes")) / count("*") as "mean_total_crimes").orderBy("price_range")
val ratingCrimes = df.groupBy("rating_range").agg((sum("non_felony_crimes") + sum("felony_crimes")) / count("*") as "mean_total_crimes").orderBy("rating_range")
val ratingMatch = df.groupBy("neighbourhood_match").agg(avg("rating_range").alias("mean_rating_range")).orderBy("neighbourhood_match")
val priceMatch = df.groupBy("neighbourhood_match").agg(avg("price_range").alias("mean_price_range")).orderBy("neighbourhood_match")
val priceSuperhost = df.groupBy("superhost").agg(avg("price_range").alias("mean_price_range")).orderBy("superhost")
val ratingSuperhost = df.groupBy("superhost").agg(avg("rating_range").alias("mean_rating_range")).orderBy("superhost")
val ratingVerified = df.groupBy("host_verified").agg(avg("rating_range").alias("mean_rating_range")).orderBy("host_verified")
val priceVerified = df.groupBy("host_verified").agg(avg("price_range").alias("mean_price_range")).orderBy("host_verified")
val priceListings = df.groupBy("price_range").agg(avg("host_total_listings_count").alias("mean_total_listings_count")).orderBy("price_range")
val ratingListings = df.groupBy("rating_range").agg(avg("host_total_listings_count").alias("mean_total_listings_count")).orderBy("rating_range")
val priceReviews = df.groupBy("price_range").agg(avg("number_of_reviews").alias("mean_number_of_reviews")).orderBy("price_range")
val ratingReviews = df.groupBy("rating_range").agg(avg("number_of_reviews").alias("mean_number_of_reviews")).orderBy("rating_range")
val priceRating = df.groupBy("price_range").agg(avg("rating_range").alias("mean_rating_range")).orderBy("price_range")
val ratingPrice = df.groupBy("rating_range").agg(avg("price_range").alias("mean_price_range")).orderBy("rating_range")

// save all dataframes to hdfs 
saveDF(priceCrimes, "priceCrimes")
saveDF(ratingCrimes, "ratingCrimes")
saveDF(ratingMatch, "ratingMatch")
saveDF(priceMatch, "priceMatch")
saveDF(priceSuperhost, "priceSuperhost")
saveDF(ratingSuperhost, "ratingSuperhost")
saveDF(ratingVerified, "ratingVerified")
saveDF(priceVerified, "priceVerified")
saveDF(priceListings, "priceListings")
saveDF(ratingListings, "ratingListings")
saveDF(priceReviews, "priceReviews")
saveDF(ratingReviews, "ratingReviews")
saveDF(priceRating, "priceRating")
saveDF(ratingPrice, "ratingPrice")

println("14 dataframes for analysis were created and saved on hdfs")




